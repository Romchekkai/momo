from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from momo.domain.modelx import ModelXStatus


@dataclass(frozen=True, slots=True)
class ModelXEvent:
    event_type: str  # Например: "modelx.created", "modelx.deleted", "modelx.restored"
    model_id: UUID
    name: str
    status: ModelXStatus
