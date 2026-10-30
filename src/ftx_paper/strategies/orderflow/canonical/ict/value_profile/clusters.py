"""Historical reference clusters built from immutable daily profiles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import hashlib
from math import pow
from typing import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import DailyProfileQuality
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import DailyProfileRecord


class ClusterReactionKind(StrEnum):
    TOUCH = "touch"
    SUPPORT_REJECTION = "support_rejection"
    RESISTANCE_REJECTION = "resistance_rejection"
    ACCEPTED_BREAK = "accepted_break"
    ROLE_REVERSAL = "role_reversal"


@dataclass(frozen=True, slots=True)
class ReferenceCluster:
    cluster_id: str
    instrument: str
    timeframe_minutes: int
    config_fingerprint: str
    member_snapshot_ids: tuple[str, ...]
    session_dates: tuple[str, ...]
    lowest_val: float
    highest_vah: float
    common_val: float | None
    common_vah: float | None
    original_pocs: tuple[float, ...]
    first_session_date: str
    latest_session_date: str
    session_count: int
    acceptance_score: float

    def __post_init__(self) -> None:
        if not self.member_snapshot_ids or not self.session_dates:
            raise ValueError("reference cluster requires provenance")
        if self.session_count != len(self.session_dates):
            raise ValueError("reference cluster session count must be distinct")
        if self.highest_vah < self.lowest_val:
            raise ValueError("reference cluster envelope is inverted")
        if (self.common_val is None) != (self.common_vah is None):
            raise ValueError("reference cluster overlap bounds must match")
        if (
            self.common_val is not None
            and self.common_vah is not None
            and self.common_vah < self.common_val
        ):
            raise ValueError("reference cluster common overlap is inverted")


@dataclass(frozen=True, slots=True)
class ClusterReactionEvent:
    cluster_id: str
    kind: ClusterReactionKind
    timestamp: datetime
    available_at: datetime
    session_date: str
    lower: float
    upper: float


@dataclass(frozen=True, slots=True)
class ClusterReactionStats:
    cluster_id: str
    completed_tests: int
    rejections: int
    accepted_breaks: int
    last_test_at: datetime | None


@dataclass(frozen=True, slots=True)
class ClusterReactionResult:
    events: tuple[ClusterReactionEvent, ...]
    stats: tuple[ClusterReactionStats, ...]


def _ratio(left: DailyProfileRecord, right: DailyProfileRecord) -> float:
    width = min(left.vah - left.val, right.vah - right.val)
    if width <= 0:
        return 1.0 if left.val == right.val else 0.0
    return max(0.0, min(left.vah, right.vah) - max(left.val, right.val)) / width


def build_reference_clusters(
    records: Sequence[DailyProfileRecord],
    *,
    overlap_ratio: float = 0.50,
    acceptance_half_life: float = 5.0,
    as_of: datetime | None = None,
    trading_dates: Sequence[str] | None = None,
) -> tuple[ReferenceCluster, ...]:
    if not 0.0 < overlap_ratio <= 1.0 or acceptance_half_life <= 0:
        raise ValueError("invalid cluster configuration")
    eligible = [
        record for record in records
        if record.quality is DailyProfileQuality.COMPLETE
        and (as_of is None or record.available_at <= as_of)
    ]
    eligible.sort(key=lambda item: (item.session_date, item.snapshot_id))
    identities = {(item.instrument, item.timeframe_minutes, item.config_fingerprint) for item in eligible}
    if len(identities) > 1:
        raise ValueError("clusters require one instrument, timeframe, and configuration")
    unique: dict[str, DailyProfileRecord] = {}
    for record in eligible:
        unique.setdefault(record.session_date, record)
    ordered = tuple(unique.values())
    age_dates = tuple(sorted(trading_dates if trading_dates is not None else unique))
    dates = {session_date: index for index, session_date in enumerate(age_dates)}
    groups: list[list[DailyProfileRecord]] = []
    for record in ordered:
        group = next(
            (candidate for candidate in groups if all(_ratio(record, member) >= overlap_ratio for member in candidate)),
            None,
        )
        if group is None:
            group = []
            groups.append(group)
        group.append(record)
    result: list[ReferenceCluster] = []
    for index, members in enumerate(groups, start=1):
        session_dates = tuple(member.session_date for member in members)
        common_val = max(member.val for member in members)
        common_vah = min(member.vah for member in members)
        if common_val > common_vah:
            common_val = common_vah = None
        latest = max(dates.values(), default=0)
        score = sum(pow(0.5, (latest - dates[item.session_date]) / acceptance_half_life) for item in members)
        first = members[0]
        result.append(ReferenceCluster(
            cluster_id=(
                f"vc:{first.instrument}:{first.timeframe_minutes}m:"
                f"{first.config_fingerprint[:12]}:"
                f"{hashlib.sha256('|'.join(item.snapshot_id for item in members).encode()).hexdigest()[:12]}"
            ),
            instrument=first.instrument,
            timeframe_minutes=first.timeframe_minutes,
            config_fingerprint=first.config_fingerprint,
            member_snapshot_ids=tuple(item.snapshot_id for item in members),
            session_dates=session_dates,
            lowest_val=min(item.val for item in members),
            highest_vah=max(item.vah for item in members),
            common_val=common_val,
            common_vah=common_vah,
            original_pocs=tuple(item.poc for item in members),
            first_session_date=session_dates[0],
            latest_session_date=session_dates[-1],
            session_count=len(session_dates),
            acceptance_score=score,
        ))
    return tuple(result)


def select_nearest_reference_clusters(close: float, clusters: Sequence[ReferenceCluster]) -> tuple[ReferenceCluster | None, ReferenceCluster | None, ReferenceCluster | None]:
    containing = [item for item in clusters if item.lowest_val <= close <= item.highest_vah]
    above = [item for item in clusters if item.lowest_val > close]
    below = [item for item in clusters if item.highest_vah < close]
    return (
        min(containing, key=lambda item: (item.highest_vah - item.lowest_val, item.cluster_id), default=None),
        min(above, key=lambda item: (item.lowest_val - close, item.cluster_id), default=None),
        min(below, key=lambda item: (close - item.highest_vah, item.cluster_id), default=None),
    )


def evaluate_cluster_reactions(
    bars: Sequence[CompletedBar],
    clusters: Sequence[ReferenceCluster],
    *,
    confirmation_bars: int = 2,
) -> ClusterReactionResult:
    if confirmation_bars < 1:
        raise ValueError("confirmation bars must be positive")
    events: list[ClusterReactionEvent] = []
    stats: list[ClusterReactionStats] = []
    for cluster in clusters:
        active_side: str | None = None
        reversal_side: str | None = None
        count = tests = rejections = breaks = 0
        last_test: datetime | None = None
        previous_close: float | None = None
        for bar in sorted(bars, key=lambda item: item.timestamp):
            touched = bar.high >= cluster.lowest_val and bar.low <= cluster.highest_vah
            if active_side is None and previous_close is not None and touched:
                if previous_close > cluster.highest_vah:
                    active_side = "above"
                elif previous_close < cluster.lowest_val:
                    active_side = "below"
                if active_side is not None:
                    tests += 1
                    last_test = bar.timestamp
                    events.append(_event(cluster, bar, ClusterReactionKind.TOUCH))
            if active_side == "above":
                if bar.close > cluster.highest_vah:
                    rejections += 1
                    events.append(_event(cluster, bar, ClusterReactionKind.SUPPORT_REJECTION))
                    if reversal_side == "above":
                        events.append(_event(cluster, bar, ClusterReactionKind.ROLE_REVERSAL))
                        reversal_side = None
                    active_side = None
                elif bar.close < cluster.lowest_val:
                    count += 1
                    if count >= confirmation_bars:
                        breaks += 1
                        events.append(_event(cluster, bar, ClusterReactionKind.ACCEPTED_BREAK))
                        reversal_side = "below"
                        active_side = None
                        count = 0
                else:
                    count = 0
            elif active_side == "below":
                if bar.close < cluster.lowest_val:
                    rejections += 1
                    events.append(_event(cluster, bar, ClusterReactionKind.RESISTANCE_REJECTION))
                    if reversal_side == "below":
                        events.append(_event(cluster, bar, ClusterReactionKind.ROLE_REVERSAL))
                        reversal_side = None
                    active_side = None
                elif bar.close > cluster.highest_vah:
                    count += 1
                    if count >= confirmation_bars:
                        breaks += 1
                        events.append(_event(cluster, bar, ClusterReactionKind.ACCEPTED_BREAK))
                        reversal_side = "above"
                        active_side = None
                        count = 0
                else:
                    count = 0
            previous_close = bar.close
        stats.append(ClusterReactionStats(cluster.cluster_id, tests, rejections, breaks, last_test))
    return ClusterReactionResult(tuple(events), tuple(stats))


def _event(cluster: ReferenceCluster, bar: CompletedBar, kind: ClusterReactionKind) -> ClusterReactionEvent:
    return ClusterReactionEvent(cluster.cluster_id, kind, bar.timestamp, bar.available_at, bar.session_date, cluster.lowest_val, cluster.highest_vah)
