from collections.abc import AsyncGenerator

from faststream.kafka import KafkaBroker
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from momo.infrastructure.settings import KafkaSettings


def create_engine(
    url: str,
    *,
    echo: bool = False,
    pool_size: int = 10,
    max_overflow: int = 20,
) -> AsyncEngine:
    return create_async_engine(
        url,
        echo=echo,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
    )


def create_session_factory(
    bind: AsyncEngine | AsyncConnection,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=bind,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


async def get_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession]:
    async with session_factory() as session:
        yield session


def create_kafka_broker(settings: KafkaSettings) -> KafkaBroker:
    return KafkaBroker(
        bootstrap_servers=settings.bootstrap_servers,
        client_id=settings.client_id,
    )
