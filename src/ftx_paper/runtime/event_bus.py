"""Synchronous in-process event delivery for one runtime owner."""

from __future__ import annotations

from collections.abc import Callable, Iterable

from ftx_paper.contracts import RuntimeEvent


EventHandler = Callable[[RuntimeEvent], None]


class EventBus:
    """Deliver critical handlers in order and isolate optional observers."""

    def __init__(self, *, critical: Iterable[EventHandler] = (), optional: Iterable[EventHandler] = ()) -> None:
        self._critical = list(critical)
        self._optional = list(optional)

    def subscribe_critical(self, handler: EventHandler) -> None:
        self._critical.append(handler)

    def subscribe_optional(self, handler: EventHandler) -> None:
        self._optional.append(handler)

    def publish(self, event: RuntimeEvent) -> None:
        """Publish through critical delivery, then isolated observers."""
        self.publish_critical(event)
        self.publish_optional(event)

    def publish_critical(self, event: RuntimeEvent) -> None:
        """Run critical handlers in order; exceptions deliberately propagate."""
        for handler in self._critical:
            handler(event)

    def publish_optional(self, event: RuntimeEvent) -> None:
        """Run optional observers while isolating observer failures."""
        for handler in self._optional:
            try:
                handler(event)
            except Exception:
                continue


__all__ = ["EventBus", "EventHandler"]
