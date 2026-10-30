"""Read-only protocols for optional value-profile consumers."""

from typing import Protocol, Sequence

from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import ProfileBinding, ProfileEvent, ValueProfileSnapshot


class ProfileContextView(Protocol):
    """Injected view of per-bar profile context without importing builder internals."""

    def bindings(self) -> Sequence[ProfileBinding | None]:
        """Return profile bindings aligned to the replay bar stream."""
        ...

    def events(self) -> Sequence[ProfileEvent]:
        """Return edge-triggered profile events for the stream."""
        ...

    def snapshots(self) -> Sequence[ValueProfileSnapshot]:
        """Return frozen and developing snapshots emitted by the track."""
        ...
