import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import Annotated, cast
from uuid import UUID

import structlog
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from faststream.kafka import KafkaBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from momo.application.modeldto import InputModelXDTO, OutputModelXDTO
from momo.application.usecase import (
    CreateModelXUseCase,
    DeleteModelXUseCase,
    GetModelXUseCase,
    ModelXNotFoundError,
)
from momo.infrastructure.rely import OutboxRelay
from momo.infrastructure.repositories import ModelXRepository
from momo.infrastructure.session import (
    create_engine,
    create_kafka_broker,
    create_session_factory,
)
from momo.infrastructure.settings import DatabaseSettings, KafkaSettings

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    settings = DatabaseSettings()
    kafka_settings = KafkaSettings()

    engine = create_engine(url=settings.url)
    session_factory = create_session_factory(engine)
    broker = create_kafka_broker(kafka_settings)

    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.broker = broker

    await broker.start()
    relay = OutboxRelay(session_factory=session_factory, broker=broker)
    relay_task = asyncio.create_task(relay.run())

    yield

    relay_task.cancel()
    with suppress(asyncio.CancelledError):
        await relay_task
    await broker.stop()
    await engine.dispose()


app = FastAPI(lifespan=lifespan)


async def get_db_session(request: Request) -> AsyncGenerator[AsyncSession]:
    session_factory: async_sessionmaker[AsyncSession] = (
        request.app.state.session_factory
    )
    async with session_factory() as session:
        yield session


def get_broker(request: Request) -> KafkaBroker:
    return cast(KafkaBroker, request.app.state.broker)


BrokerDep = Annotated[KafkaBroker, Depends(get_broker)]

DbSession = Annotated[AsyncSession, Depends(get_db_session)]


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "Hello World"}


# git tag v0.2.0 && git push --tags


# 1. Liveness probe: проверяет только, что процесс приложения не завис
@app.get("/healthz", status_code=status.HTTP_200_OK, tags=["Health"])
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


# 2. Readiness probe: проверяет подключение к PostgreSQL
# (позже добавить Kafka)
@app.get("/readyz", tags=["Health"], include_in_schema=False)
async def readyz(session: DbSession, broker: BrokerDep) -> JSONResponse:
    checks: dict[str, str] = {"database": "ok", "kafka": "ok"}
    is_ready = True

    try:
        await session.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("Readiness check failed: database unavailable", exc_info=exc)
        checks["database"] = "unavailable"
        is_ready = False

    try:
        if not await broker.ping(timeout=2.0):
            checks["kafka"] = "unavailable"
            is_ready = False
    except Exception as exc:
        logger.warning("Readiness check failed: kafka unavailable", exc_info=exc)
        checks["kafka"] = "unavailable"
        is_ready = False

    if not is_ready:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not_ready", **checks},
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"status": "ready", **checks},
    )


# --- Зависимости (DI) ---
def get_model_repo(session: DbSession) -> ModelXRepository:
    return ModelXRepository(session)


ModelRepoDep = Annotated[ModelXRepository, Depends(get_model_repo)]


def get_create_uc(repo: ModelRepoDep) -> CreateModelXUseCase:
    return CreateModelXUseCase(repo)


def get_get_uc(repo: ModelRepoDep) -> GetModelXUseCase:
    return GetModelXUseCase(repo)


def get_delete_uc(repo: ModelRepoDep) -> DeleteModelXUseCase:
    return DeleteModelXUseCase(repo)


# --- Эндпоинты ---
@app.post(
    "/models",
    response_model=OutputModelXDTO,
    status_code=status.HTTP_201_CREATED,
    tags=["Models"],
)
async def create_model(
    data: InputModelXDTO,
    use_case: Annotated[CreateModelXUseCase, Depends(get_create_uc)],
) -> OutputModelXDTO:
    return await use_case.execute(data)


@app.get("/models/{model_id}", response_model=OutputModelXDTO, tags=["Models"])
async def get_model(
    model_id: UUID,
    use_case: Annotated[GetModelXUseCase, Depends(get_get_uc)],
) -> OutputModelXDTO:
    try:
        return await use_case.execute(model_id)
    except ModelXNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc


@app.delete("/models/{model_id}", response_model=OutputModelXDTO, tags=["Models"])
async def delete_model(
    model_id: UUID,
    use_case: Annotated[DeleteModelXUseCase, Depends(get_delete_uc)],
) -> OutputModelXDTO:
    try:
        return await use_case.execute(model_id)
    except ModelXNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from exc
