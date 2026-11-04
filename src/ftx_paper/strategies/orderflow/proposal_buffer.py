"""Transient host-proposal ownership for the orderflow transition boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Iterable

from .execution_adapter import PaperOrderProposal


@dataclass(slots=True)
class ProposalBuffer:
    """Hold proposals until the runtime host drains the current transition."""

    _pending: tuple[PaperOrderProposal, ...] = field(default=(), init=False, repr=False)
    _published_intents: set[str] = field(default_factory=set, init=False, repr=False)

    def begin_transition(self) -> None:
        """Clear proposals from the previous completed transition."""
        if self._pending:
            raise RuntimeError(
                "orderflow host proposals must be drained before the next transition"
            )
        self._pending = ()

    def contains_intent(self, intent_id: str) -> bool:
        return intent_id in self._published_intents

    def publish(self, intent_id: str, proposal: PaperOrderProposal) -> None:
        """Publish a proposal once for an intent until the owner is restored."""
        if self.contains_intent(intent_id):
            return
        self._pending = (*self._pending, proposal)
        self._published_intents.add(intent_id)

    def drain(self) -> tuple[PaperOrderProposal, ...]:
        proposals = self._pending
        self._pending = ()
        return proposals

    def has_pending(self) -> bool:
        return bool(self._pending)

    def published_intents(self) -> tuple[str, ...]:
        return tuple(sorted(self._published_intents))

    def restore(self, published_intents: Iterable[str]) -> None:
        self._pending = ()
        self._published_intents = {
            str(intent_id) for intent_id in published_intents
        }


__all__ = ["ProposalBuffer"]
