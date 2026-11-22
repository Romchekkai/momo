from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from momo.domain.modelx import ModelXStatus


class BaseDTO(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        frozen=True,
        str_strip_whitespace=True,
    )


class InputModelXDTO(BaseDTO):
    name: str = Field(min_length=1, max_length=255)
    empty_field: str = ""
    status: ModelXStatus


class OutputModelXDTO(BaseDTO):
    id: UUID
    name: str
    empty_field: str
    status: ModelXStatus
