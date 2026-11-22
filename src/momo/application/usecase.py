from uuid import UUID

from momo.application.modeldto import InputModelXDTO, OutputModelXDTO
from momo.domain.modelx import ModelX
from momo.domain.repo import ModelRepo


class ModelXNotFoundError(Exception):
    def __init__(self, model_id: UUID) -> None:
        super().__init__(f"ModelX with id={model_id} not found")
        self.model_id = model_id


class CreateModelXUseCase:
    def __init__(self, repo: ModelRepo) -> None:
        self.repo = repo

    async def execute(self, data: InputModelXDTO) -> OutputModelXDTO:
        domain_model = ModelX.register(
            name=data.name,
            empty_field=data.empty_field,
        )
        await self.repo.save_model(domain_model)
        return OutputModelXDTO.model_validate(domain_model)


class GetModelXUseCase:
    def __init__(self, repo: ModelRepo) -> None:
        self.repo = repo

    async def execute(self, model_id: UUID) -> OutputModelXDTO:
        model = await self.repo.get_model(model_id)
        if model is None:
            raise ModelXNotFoundError(model_id)
        return OutputModelXDTO.model_validate(model)


class DeleteModelXUseCase:
    def __init__(self, repo: ModelRepo) -> None:
        self.repo = repo

    async def execute(self, model_id: UUID) -> OutputModelXDTO:
        model = await self.repo.get_model(model_id)
        if model is None:
            raise ModelXNotFoundError(model_id)

        model.delete()
        await self.repo.save_model(model)
        return OutputModelXDTO.model_validate(model)


class RestoreModelXUseCase:
    def __init__(self, repo: ModelRepo) -> None:
        self.repo = repo

    async def execute(self, model_id: UUID) -> OutputModelXDTO:
        model = await self.repo.get_model(model_id)
        if model is None:
            raise ModelXNotFoundError(model_id)

        model.restore()
        await self.repo.save_model(model)
        return OutputModelXDTO.model_validate(model)


class GetModelsUseCase:
    def __init__(self, repo: ModelRepo) -> None:
        self.repo = repo

    async def execute(self) -> list[OutputModelXDTO]:
        models = await self.repo.get_models()
        return [OutputModelXDTO.model_validate(model) for model in models]
