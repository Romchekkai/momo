from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid7

from sqlalchemy import DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from momo.domain.modelx import ModelXStatus


class Base(AsyncAttrs, DeclarativeBase):
    pass


class ModelXORM(Base):
    __tablename__ = "model_x"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    empty_field: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    status: Mapped[ModelXStatus] = mapped_column(String(32), nullable=False)


class OutboxStatus(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class OutboxEventORM(Base):
    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    idempotency_key: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False
    )
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    trace_context: Mapped[dict[str, str] | None] = mapped_column(JSONB, nullable=True)
    status: Mapped[OutboxStatus] = mapped_column(
        String(32), nullable=False, default=OutboxStatus.PENDING, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
