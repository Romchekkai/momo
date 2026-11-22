import asyncio
import inspect
import os
import selectors
import sys
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import cast

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer
from alembic import command
from alembic.config import Config
from asgi_lifespan import LifespanManager
from faststream.kafka import KafkaBroker
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from momo.infrastructure.models import Base
from momo.main import app

ROOT_DIR = Path(__file__).resolve().parent.parent
ALEMBIC_INI_PATH = ROOT_DIR / "alembic.ini"


def _selector_loop_factory() -> asyncio.AbstractEventLoop:
    return asyncio.SelectorEventLoop(selectors.SelectSelector())


def pytest_asyncio_loop_factories(
    config: pytest.Config,
    item: pytest.Item,
) -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
    if sys.platform == "win32":
        return {"selector": _selector_loop_factory}
    return {"default": asyncio.new_event_loop}


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if inspect.iscoroutinefunction(getattr(item, "obj", None)):
            item.add_marker(pytest.mark.asyncio(loop_scope="session"))


def _get_alembic_config() -> Config:
    return Config(str(ALEMBIC_INI_PATH))


def _run_migrations(alembic_cfg: Config, revision: str = "head") -> None:
    command.upgrade(alembic_cfg, revision)


def _downgrade_migrations(alembic_cfg: Config, revision: str = "base") -> None:
    command.downgrade(alembic_cfg, revision)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def started_app() -> AsyncGenerator[None]:
    alembic_cfg = _get_alembic_config()
    await asyncio.to_thread(_run_migrations, alembic_cfg, "head")  # должна быть

    async with LifespanManager(app):
        yield

    await asyncio.to_thread(_downgrade_migrations, alembic_cfg, "base")


@pytest.fixture(autouse=True)
async def clean_db(started_app: None) -> AsyncGenerator[None]:
    yield
    async with app.state.session_factory() as session:
        table_names = ", ".join(
            f'"{table.name}"' for table in reversed(Base.metadata.sorted_tables)
        )
        if table_names:
            await session.execute(
                text(f"TRUNCATE TABLE {table_names} RESTART IDENTITY CASCADE")
            )
            await session.commit()


@pytest.fixture
async def client(started_app: None) -> AsyncGenerator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
async def db_session(started_app: None) -> AsyncGenerator[AsyncSession]:
    async with app.state.session_factory() as session:
        yield session


@pytest.fixture
def kafka_broker(started_app: None) -> KafkaBroker:
    return cast(KafkaBroker, app.state.broker)


TOPIC = "modelx-events"


@pytest.fixture
async def kafka_consumer() -> AsyncGenerator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()
