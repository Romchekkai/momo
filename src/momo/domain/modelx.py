from dataclasses import dataclass, field
from enum import StrEnum
from uuid import UUID, uuid7

from momo.domain.eventent import ModelXEvent


class ModelXStatus(StrEnum):
    ACTIVE = "active"
    DELETED = "deleted"


@dataclass
class ModelX:
    id: UUID
    name: str
    empty_field: str
    status: ModelXStatus
    _events: list[ModelXEvent] = field(default_factory=list, init=False, repr=False)

    @classmethod
    def register(cls, name: str, empty_field: str) -> ModelX:
        model = cls(
            id=uuid7(),
            name=name,
            empty_field=empty_field,
            status=ModelXStatus.ACTIVE,
        )
        model._record_event("modelx.created")
        return model

    def delete(self) -> None:
        if self.status == ModelXStatus.DELETED:
            raise ValueError("Объект уже удален")
        self.status = ModelXStatus.DELETED
        self._record_event("modelx.deleted")

    def restore(self) -> None:
        if self.status == ModelXStatus.ACTIVE:
            raise ValueError("Объект уже активен")
        self.status = ModelXStatus.ACTIVE
        self._record_event("modelx.restored")

    def _record_event(self, event_type: str) -> None:
        self._events.append(
            ModelXEvent(
                event_type=event_type,
                model_id=self.id,
                name=self.name,
                status=self.status,
            )
        )

    def pull_events(self) -> list[ModelXEvent]:
        events = list(self._events)
        self._events.clear()
        return events
