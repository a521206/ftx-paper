"""Temporal contracts for value-profile input streams."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.domain.time import require_order


@dataclass(frozen=True, slots=True)
class TemporalGap:
    """A missing interval between two otherwise adjacent completed bars."""

    previous_timestamp: datetime
    next_timestamp: datetime
    previous_available_at: datetime
    next_available_at: datetime
    expected_minutes: int

    def __post_init__(self) -> None:
        require_order(
            observed_at=self.previous_timestamp,
            available_at=self.previous_available_at,
            subject="temporal gap previous bar",
        )
        require_order(
            observed_at=self.next_timestamp,
            available_at=self.next_available_at,
            subject="temporal gap next bar",
        )
        if self.next_timestamp <= self.previous_timestamp:
            raise ValueError("temporal gaps require increasing timestamps")
        if self.expected_minutes < 1:
            raise ValueError("temporal gap expected interval must be positive")


@dataclass(frozen=True, slots=True)
class TemporalValidation:
    """Validated stream identity and gaps for one value-profile run."""

    instrument: str
    timeframe_minutes: int
    session_date: str
    session_dates: tuple[str, ...]
    gaps: tuple[TemporalGap, ...]


def validate_profile_stream(bars: Sequence[CompletedBar]) -> TemporalValidation:
    """Validate one causally ordered instrument/timeframe stream (multi-session allowed)."""
    if not bars:
        raise ValueError("value profile requires at least one completed bar")

    first = bars[0]
    if first.timeframe_minutes < 1:
        raise ValueError("completed bar timeframe must be positive")
    identities = {(bar.instrument, bar.timeframe_minutes, bar.timestamp) for bar in bars}
    if len(identities) != len(bars):
        raise ValueError("profile stream bars must have unique identities")

    session_dates: list[str] = []
    previous_session: str | None = None
    for index, bar in enumerate(bars):
        if bar.instrument != first.instrument or bar.timeframe_minutes != first.timeframe_minutes:
            raise ValueError("profile stream must contain one instrument and timeframe")
        require_order(
            observed_at=bar.timestamp,
            available_at=bar.available_at,
            subject="completed bar",
        )
        if bar.session_date != previous_session:
            session_dates.append(bar.session_date)
            previous_session = bar.session_date
        if index == 0:
            continue
        previous = bars[index - 1]
        if bar.available_at < previous.available_at:
            raise ValueError("profile stream must be ordered by causal availability")
        if bar.timestamp <= previous.timestamp:
            raise ValueError("profile stream timestamps must be strictly increasing")

    expected = timedelta(minutes=first.timeframe_minutes)
    gaps: list[TemporalGap] = []
    for previous, current in zip(bars, bars[1:]):
        if current.timestamp - previous.timestamp > expected:
            gaps.append(
                TemporalGap(
                    previous_timestamp=previous.timestamp,
                    next_timestamp=current.timestamp,
                    previous_available_at=previous.available_at,
                    next_available_at=current.available_at,
                    expected_minutes=first.timeframe_minutes,
                )
            )
    return TemporalValidation(
        instrument=first.instrument,
        timeframe_minutes=first.timeframe_minutes,
        session_date=first.session_date,
        session_dates=tuple(session_dates),
        gaps=tuple(gaps),
    )
