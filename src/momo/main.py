from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import fastapi
import structlog

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: fastapi.FastAPI) -> AsyncIterator[None]:
    logger.info("Starting lifespan")
    yield
    logger.info("Ending lifespan")


app = fastapi.FastAPI()


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "Hello World"}


# git tag v0.2.0 && git push --tags
