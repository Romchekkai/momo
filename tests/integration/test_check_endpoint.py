from unittest.mock import AsyncMock

from fastapi import status
from httpx import AsyncClient
from sqlalchemy.exc import OperationalError

from momo.main import app, get_broker, get_db_session


async def test_check_helthz_endpoint(client: AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_check_readyz_endpoint(
    client: AsyncClient,
) -> None:
    response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "ok", "kafka": "ok"}


async def test_readyz_kafka_unavailable(client: AsyncClient) -> None:
    broken_broker = AsyncMock()
    broken_broker.ping.return_value = False

    app.dependency_overrides[get_broker] = lambda: broken_broker
    try:
        response = await client.get("/readyz")
    finally:
        app.dependency_overrides.pop(get_broker, None)

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert response.json() == {
        "status": "not_ready",
        "database": "ok",
        "kafka": "unavailable",
    }


async def test_readyz_database_unavailable(client: AsyncClient) -> None:
    broken_session = AsyncMock()
    broken_session.execute.side_effect = OperationalError(
        "SELECT 1", {}, Exception("DB down")
    )

    app.dependency_overrides[get_db_session] = lambda: broken_session
    try:
        response = await client.get("/readyz")
    finally:
        app.dependency_overrides.pop(get_db_session, None)

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert response.json() == {
        "status": "not_ready",
        "database": "unavailable",
        "kafka": "ok",
    }
