import asyncio
import logging
import time
from typing import Any

from faststream.kafka import KafkaBroker
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from momo.infrastructure.models import OutboxEventORM, OutboxStatus

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

# --- Метрики Prometheus ---
OUTBOX_PUBLISHED_TOTAL = Counter(
    "outbox_events_published_total",
    "Общее количество событий Outbox, обработанных воркером",
    labelnames=["topic", "status"],  # status: "sent" | "failed"
)
OUTBOX_PUBLISH_DURATION = Histogram(
    "outbox_publish_duration_seconds",
    "Время отправки одного события из Outbox в Kafka",
    labelnames=["topic"],
)
OUTBOX_PENDING_GAUGE = Gauge(
    "outbox_events_pending",
    "Текущее количество неотправленных событий в очереди Outbox",
)


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: KafkaBroker,
        poll_interval: float = 0.5,
        batch_size: int = 50,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._poll_interval = poll_interval
        self._batch_size = batch_size

    async def run(self) -> None:
        logger.info(
            "Outbox relay started",
            extra={
                "poll_interval": self._poll_interval,
                "batch_size": self._batch_size,
            },
        )
        while True:
            try:
                processed_count = await self._process_batch()
                if processed_count == 0:
                    await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                logger.info("Outbox relay stopping gracefully")
                raise
            except Exception:
                logger.exception("Unexpected error in OutboxRelay loop")
                await asyncio.sleep(self._poll_interval)

    async def _process_batch(self) -> int:
        async with self._session_factory() as session:
            # Обновляем Gauge очереди (удобно для алертов в Prometheus,
            # если воркер встал)
            pending_count = await session.scalar(
                select(func.count(OutboxEventORM.id)).where(
                    OutboxEventORM.status == OutboxStatus.PENDING
                )
            )
            OUTBOX_PENDING_GAUGE.set(pending_count or 0)

            stmt = (
                select(OutboxEventORM)
                .where(OutboxEventORM.status == OutboxStatus.PENDING)
                .order_by(OutboxEventORM.created_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            result = await session.execute(stmt)
            events = result.scalars().all()

            if not events:
                return 0

            for event in events:
                await self._publish_single_event(event)

            await session.commit()
            return len(events)

    async def _publish_single_event(self, event: OutboxEventORM) -> None:
        # Восстанавливаем родительский контекст трейса, сохранённый при HTTP-запросе
        parent_ctx = extract(event.trace_context) if event.trace_context else None

        with tracer.start_as_current_span(
            f"outbox.publish {event.topic}",
            context=parent_ctx,
            kind=SpanKind.PRODUCER,
        ) as span:
            span.set_attribute("messaging.system", "kafka")
            span.set_attribute("messaging.destination.name", event.topic)
            span.set_attribute("outbox.event_id", str(event.id))
            span.set_attribute("outbox.idempotency_key", event.idempotency_key)

            headers: dict[str, Any] = {
                "idempotency_key": event.idempotency_key,
            }
            # Инжектим текущий спан (PRODUCER) в заголовки Kafka для сервиса-консьюмера
            inject(headers)

            start_time = time.perf_counter()
            try:
                await self._broker.publish(
                    message=event.payload,
                    topic=event.topic,
                    key=event.partition_key.encode("utf-8"),
                    headers=headers,
                )
                duration = time.perf_counter() - start_time

                event.status = OutboxStatus.SENT
                OUTBOX_PUBLISHED_TOTAL.labels(topic=event.topic, status="sent").inc()
                OUTBOX_PUBLISH_DURATION.labels(topic=event.topic).observe(duration)

            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                event.status = OutboxStatus.FAILED
                OUTBOX_PUBLISHED_TOTAL.labels(topic=event.topic, status="failed").inc()

                logger.exception(
                    "Failed to publish outbox event to Kafka",
                    extra={
                        "event_id": str(event.id),
                        "topic": event.topic,
                        "idempotency_key": event.idempotency_key,
                    },
                )
