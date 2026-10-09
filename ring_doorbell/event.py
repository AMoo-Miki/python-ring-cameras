"""Module for ring events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, NamedTuple


@dataclass
class RingEvent:
    """Class for ring events."""

    id: str
    doorbot_id: int
    device_name: str
    device_kind: str
    now: float
    expires_in: float
    kind: str
    state: str
    is_update: bool = False
    # Ring sends one push per detection: a single ding (``id``) can carry
    # several detections, e.g. a person and then a package, each with its own
    # notification id (``riid``). A repeat push of the same detection (same
    # ``riid``, ``is_update`` True) typically only delivers a better
    # ``description``.
    riid: str | None = None
    # Notification text: Ring's generic message ("There is a Person at your
    # Front Door") or, with ``description_provider`` "llm" on plans that
    # include it, an AI description of the clip.
    description: str | None = None
    description_provider: str | None = None

    def __getitem__(self, key: str) -> Any:
        """Get a value by string."""
        return getattr(self, key)

    def get(self, key: str) -> Any | None:
        """Get a value by string and return None if not present."""
        return getattr(self, key) if hasattr(self, key) else None

    def get_key(self) -> RingEventKey:
        """Return the identificationkey for the event."""
        return RingEventKey(self.id, self.doorbot_id, self.kind, self.now)


class RingEventKey(NamedTuple):
    """Class to identify an event.

    Used for determining if messages are updates to events.
    """

    id: str
    doorbot_id: int
    kind: str
    now: float
