import abc
from uuid import UUID

from momo.domain.modelx import ModelX


class ModelRepo(abc.ABC):
    @abc.abstractmethod
    async def save_model(self, model: ModelX) -> ModelX:
        pass

    @abc.abstractmethod
    async def get_model(self, id: UUID) -> ModelX | None:
        pass

    @abc.abstractmethod
    async def get_models(self) -> list[ModelX]:
        pass
