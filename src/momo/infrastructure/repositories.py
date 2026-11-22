import logging
from typing import cast
from uuid import UUID, uuid7

from opentelemetry import trace
from opentelemetry.propagate import inject
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from momo.domain.modelx import ModelX
from momo.domain.repo import ModelRepo
from momo.infrastructure.models import ModelXORM, OutboxEventORM, OutboxStatus

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def _extract_trace_context() -> dict[str, str] | None:
    """Захватывает текущий W3C traceparent/tracestate из активного спана
    OpenTelemetry."""
    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier or None


class ModelXRepository(ModelRepo):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save_model(self, model: ModelX) -> ModelX:
        with tracer.start_as_current_span("db.save_model_with_outbox") as span:
            span.set_attribute("modelx.id", str(model.id))

            # 1. Upsert основной модели
            stmt = insert(ModelXORM).values(
                id=model.id,
                name=model.name,
                empty_field=model.empty_field,
                status=model.status,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[ModelXORM.id],
                set_={
                    "name": stmt.excluded.name,
                    "empty_field": stmt.excluded.empty_field,
                    "status": stmt.excluded.status,
                },
            )
            await self.session.execute(stmt)

            # 2. Запись доменных событий в Outbox в той же транзакции
            events = model.pull_events()
            if events:
                trace_ctx = _extract_trace_context()
                outbox_rows = []
                for event in events:
                    event_id = uuid7()
                    outbox_rows.append(
                        OutboxEventORM(
                            id=event_id,
                            idempotency_key=f"{event.event_type}:{event.model_id}:{event_id}",
                            topic="modelx-events",
                            partition_key=str(event.model_id),
                            payload={
                                "event_id": str(event_id),
                                "event_type": event.event_type,
                                "model_id": str(event.model_id),
                                "name": event.name,
                                "status": event.status.value,
                            },
                            trace_context=trace_ctx,
                            status=OutboxStatus.PENDING,
                        )
                    )
                self.session.add_all(outbox_rows)
                span.set_attribute("outbox.events_count", len(outbox_rows))

            # 3. Атомарный коммит (и модель, и события сохранятся
            # вместе или оба откатятся)
            await self.session.commit()

            if events:
                logger.info(
                    "Saved ModelX and queued outbox events",
                    extra={
                        "model_id": str(model.id),
                        "events_count": len(events),
                    },
                )

            return model

    async def get_model(self, model_id: UUID) -> ModelX | None:
        orm_model = cast(ModelXORM | None, await self.session.get(ModelXORM, model_id))
        if orm_model is None:
            return None
        return self._to_domain(orm_model)

    async def get_models(self) -> list[ModelX]:
        stmt = select(ModelXORM)
        result = await self.session.scalars(stmt)
        return [self._to_domain(row) for row in result.all()]

    @staticmethod
    def _to_orm(domain_model: ModelX) -> ModelXORM:
        return ModelXORM(
            id=domain_model.id,
            name=domain_model.name,
            empty_field=domain_model.empty_field,
            status=domain_model.status,
        )

    @staticmethod
    def _to_domain(orm_model: ModelXORM) -> ModelX:
        return ModelX(
            id=orm_model.id,
            name=orm_model.name,
            empty_field=orm_model.empty_field,
            status=orm_model.status,
        )
