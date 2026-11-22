import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer
from fastapi import status
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from momo.domain.modelx import ModelXStatus
from momo.infrastructure.models import ModelXORM, OutboxEventORM, OutboxStatus

PAYLOAD = {
    "name": "test-model",
    "empty_field": "",
    "status": ModelXStatus.ACTIVE.value,
}
TOPIC = "modelx-events"


async def _wait_until[T](
    predicate: Callable[[], Awaitable[T]],
    interval: float = 0.2,
) -> T:
    """Опрашивает async-предикат, пока он не вернёт truthy-значение."""
    while True:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(interval)


async def test_create_model_persists_model(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    response = await client.post("/models", json=PAYLOAD)

    assert response.status_code == status.HTTP_201_CREATED
    model_id = response.json()["id"]

    row = await db_session.scalar(select(ModelXORM).where(ModelXORM.id == model_id))
    assert row is not None
    assert row.name == PAYLOAD["name"]


async def test_create_model_writes_outbox_event(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    response = await client.post("/models", json=PAYLOAD)
    assert response.status_code == status.HTTP_201_CREATED
    model_id = response.json()["id"]

    events = (
        await db_session.scalars(
            select(OutboxEventORM).where(OutboxEventORM.partition_key == model_id)
        )
    ).all()

    assert len(events) == 1
    event = events[0]
    assert event.topic == TOPIC
    assert event.payload["model_id"] == model_id
    assert event.payload["name"] == PAYLOAD["name"]
    assert event.idempotency_key.endswith(str(event.id))
    # релей работает в фоне и мог уже отправить событие
    assert event.status in (OutboxStatus.PENDING, OutboxStatus.SENT)


async def test_relay_publishes_event_to_kafka(
    client: AsyncClient,
    db_session: AsyncSession,
    kafka_consumer: AIOKafkaConsumer,
) -> None:
    response = await client.post("/models", json=PAYLOAD)
    assert response.status_code == status.HTTP_201_CREATED
    model_id = response.json()["id"]

    # 1. Сообщение пришло в Kafka (в топике могут быть чужие, фильтруем по ключу)
    async def _find_message() -> Any:
        batches = await kafka_consumer.getmany(timeout_ms=500)
        for messages in batches.values():
            for msg in messages:
                if msg.key == model_id.encode("utf-8"):
                    return msg
        return None

    async with asyncio.timeout(15):
        message = await _wait_until(_find_message, interval=0)

    body = json.loads(message.value)
    assert body["model_id"] == model_id
    assert body["name"] == PAYLOAD["name"]
    headers = {k: v.decode() for k, v in message.headers}
    assert headers["idempotency_key"].endswith(body["event_id"])

    # 2. Релей пометил событие отправленным
    async def _is_sent() -> bool:
        status_ = await db_session.scalar(
            select(OutboxEventORM.status).where(
                OutboxEventORM.partition_key == model_id
            )
        )
        return bool(status_ == OutboxStatus.SENT)

    async with asyncio.timeout(10):
        await _wait_until(_is_sent)
