"""Immutable records produced by the causal value-profile track."""

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from ftx_paper.strategies.orderflow.canonical.ict.domain.time import require_aware, require_order
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import (
    DailyProfileQuality,
    NearbyReferenceRole,
    ProfileEventKind,
    ProfileLevelRole,
    ProfileScope,
    SnapshotStatus,
    ValuePosition,
)

if TYPE_CHECKING:
    from ftx_paper.strategies.orderflow.canonical.ict.value_profile.clusters import ClusterReactionEvent, ClusterReactionStats, ReferenceCluster

def _require_id(value: str, name: str) -> None:
    if not value:
        raise ValueError(f"{name} requires identity")


def _require_stream_identity(
    *,
    instrument: str,
    timeframe_minutes: int,
    session_date: str,
    subject: str,
) -> None:
    if not instrument:
        raise ValueError(f"{subject} requires instrument")
    if timeframe_minutes < 1:
        raise ValueError(f"{subject} timeframe must be positive")
    if not session_date:
        raise ValueError(f"{subject} requires session_date")


@dataclass(frozen=True, slots=True)
class ValueProfileSnapshot:
    snapshot_id: str
    scope: ProfileScope
    status: SnapshotStatus
    instrument: str
    timeframe_minutes: int
    session_date: str
    window_first_bar_ts: datetime
    window_last_bar_ts: datetime
    poc: float
    vah: float
    val: float
    total_volume: float
    observed_at: datetime
    available_at: datetime
    config_fingerprint: str
    profile_version: str
    vwap: float | None = None
    source_bar_fingerprints: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_id(self.snapshot_id, "snapshot")
        _require_stream_identity(
            instrument=self.instrument,
            timeframe_minutes=self.timeframe_minutes,
            session_date=self.session_date,
            subject="snapshot",
        )
        if self.window_last_bar_ts < self.window_first_bar_ts:
            raise ValueError("snapshot window is inverted")
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="snapshot")
        if not self.config_fingerprint:
            raise ValueError("snapshot requires config_fingerprint")
        if not self.profile_version:
            raise ValueError("snapshot requires profile_version")
        if self.total_volume < 0:
            raise ValueError("snapshot total volume cannot be negative")
        if self.vah < self.val:
            raise ValueError("snapshot value area is inverted")


@dataclass(frozen=True, slots=True)
class ProfileLevel:
    level_id: str
    snapshot_id: str
    role: ProfileLevelRole
    price: float
    instrument: str
    timeframe_minutes: int
    session_date: str
    available_at: datetime
    config_fingerprint: str
    profile_version: str

    def __post_init__(self) -> None:
        _require_id(self.level_id, "profile level")
        _require_id(self.snapshot_id, "profile level snapshot")
        _require_stream_identity(
            instrument=self.instrument,
            timeframe_minutes=self.timeframe_minutes,
            session_date=self.session_date,
            subject="profile level",
        )
        require_aware(self.available_at, name="profile level.available_at")
        if not self.config_fingerprint:
            raise ValueError("profile level requires config_fingerprint")
        if not self.profile_version:
            raise ValueError("profile level requires profile_version")


@dataclass(frozen=True, slots=True)
class ProfileObservation:
    bar_fingerprint: str
    snapshot_id: str
    scope: ProfileScope
    instrument: str
    timeframe_minutes: int
    session_date: str
    observed_at: datetime
    available_at: datetime
    poc: float
    vah: float
    val: float
    config_fingerprint: str
    profile_version: str

    def __post_init__(self) -> None:
        if not self.bar_fingerprint:
            raise ValueError("profile observation requires bar fingerprint")
        _require_id(self.snapshot_id, "profile observation snapshot")
        _require_stream_identity(
            instrument=self.instrument,
            timeframe_minutes=self.timeframe_minutes,
            session_date=self.session_date,
            subject="profile observation",
        )
        require_order(
            observed_at=self.observed_at,
            available_at=self.available_at,
            subject="profile observation",
        )
        if not self.config_fingerprint:
            raise ValueError("profile observation requires config_fingerprint")
        if not self.profile_version:
            raise ValueError("profile observation requires profile_version")
        if self.vah < self.val:
            raise ValueError("profile observation value area is inverted")


@dataclass(frozen=True, slots=True)
class ProfileBinding:
    snapshot_id: str
    position: ValuePosition
    poc: float
    vah: float
    val: float
    instrument: str
    timeframe_minutes: int
    session_date: str
    observed_at: datetime
    available_at: datetime
    selected_at: datetime
    config_fingerprint: str
    profile_version: str
    distance_to_poc: float | None = None
    distance_to_vah: float | None = None
    distance_to_val: float | None = None
    distance_atr: float | None = None
    near_poc: bool = False

    def __post_init__(self) -> None:
        _require_id(self.snapshot_id, "profile binding")
        _require_stream_identity(
            instrument=self.instrument,
            timeframe_minutes=self.timeframe_minutes,
            session_date=self.session_date,
            subject="profile binding",
        )
        require_order(
            observed_at=self.observed_at,
            available_at=self.available_at,
            subject="profile binding",
        )
        require_order(
            observed_at=self.selected_at,
            available_at=self.available_at,
            subject="profile binding selection",
        )
        if not self.config_fingerprint:
            raise ValueError("profile binding requires config_fingerprint")
        if not self.profile_version:
            raise ValueError("profile binding requires profile_version")


@dataclass(frozen=True, slots=True)
class ProfileEvent:
    kind: ProfileEventKind
    instrument: str
    timeframe_minutes: int
    session_date: str
    observed_at: datetime
    available_at: datetime
    snapshot_id: str | None = None
    config_fingerprint: str = ""
    profile_version: str = ""

    def __post_init__(self) -> None:
        _require_stream_identity(
            instrument=self.instrument,
            timeframe_minutes=self.timeframe_minutes,
            session_date=self.session_date,
            subject="profile event",
        )
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="profile event")
        if self.kind is ProfileEventKind.PUBLISHED:
            if self.snapshot_id is None:
                raise ValueError("published profile events require snapshot_id")
            _require_id(self.snapshot_id, "profile event snapshot")
        if not self.config_fingerprint:
            raise ValueError("profile event requires config_fingerprint")
        if not self.profile_version:
            raise ValueError("profile event requires profile_version")


@dataclass(frozen=True, slots=True)
class ValueProfileResult:
    """Complete value-profile output aligned to one validated bar stream."""

    snapshots: tuple[ValueProfileSnapshot, ...]
    levels: tuple[ProfileLevel, ...]
    observations: tuple[ProfileObservation, ...] = ()
    bindings: tuple[ProfileBinding | None, ...] = ()
    bindings_by_scope: tuple[tuple[ProfileScope, tuple[ProfileBinding | None, ...]], ...] = ()
    events: tuple[ProfileEvent, ...] = ()
    nearby_references: tuple["NearbyValueAreaSelection | None", ...] = ()
    reference_clusters: tuple["ReferenceCluster", ...] = ()
    cluster_reaction_events: tuple["ClusterReactionEvent", ...] = ()
    cluster_reaction_stats: tuple["ClusterReactionStats", ...] = ()
    prior_session_resolution: "PriorSessionResolution | None" = None


@dataclass(frozen=True, slots=True)
class DailyProfileRecord:
    """One completed (or incomplete) session profile persisted for reuse."""

    snapshot_id: str
    instrument: str
    timeframe_minutes: int
    session_date: str
    poc: float
    vah: float
    val: float
    vwap: float | None
    window_first_bar_ts: datetime
    window_last_bar_ts: datetime
    bar_count: int
    total_volume: float
    gap_count: int
    quality: DailyProfileQuality
    observed_at: datetime
    available_at: datetime
    config_fingerprint: str
    profile_version: str
    aggregate_source_fingerprint: str

    def __post_init__(self) -> None:
        _require_id(self.snapshot_id, "daily profile")
        _require_stream_identity(
            instrument=self.instrument,
            timeframe_minutes=self.timeframe_minutes,
            session_date=self.session_date,
            subject="daily profile",
        )
        if self.window_last_bar_ts < self.window_first_bar_ts:
            raise ValueError("daily profile window is inverted")
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="daily profile")
        if not self.config_fingerprint:
            raise ValueError("daily profile requires config_fingerprint")
        if not self.profile_version:
            raise ValueError("daily profile requires profile_version")
        if not self.aggregate_source_fingerprint:
            raise ValueError("daily profile requires aggregate_source_fingerprint")
        if self.bar_count < 1:
            raise ValueError("daily profile requires at least one bar")
        if self.total_volume < 0:
            raise ValueError("daily profile total volume cannot be negative")
        if self.gap_count < 0:
            raise ValueError("daily profile gap count cannot be negative")
        if self.vah < self.val:
            raise ValueError("daily profile value area is inverted")


@dataclass(frozen=True, slots=True)
class PriorSessionResolution:
    """Outcome of resolving the previous session reference for the active day."""

    session_date: str | None
    snapshot: ValueProfileSnapshot | None
    daily_record: DailyProfileRecord | None
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.snapshot is None and not self.reason:
            raise ValueError("prior session resolution requires a snapshot or explicit reason")


@dataclass(frozen=True, slots=True)
class NearbyValueAreaReference:
    session_date: str
    snapshot_id: str
    poc: float
    val: float
    vah: float
    distance: float
    overlaps_range: bool
    role: NearbyReferenceRole
    available_at: datetime
    instrument: str
    timeframe_minutes: int
    quality: DailyProfileQuality
    config_fingerprint: str
    profile_version: str
    aggregate_source_fingerprint: str
    position: ValuePosition = ValuePosition.INSIDE
    near_poc: bool = False

    def __post_init__(self) -> None:
        if not self.session_date:
            raise ValueError("nearby value reference requires session_date")
        if not self.instrument or self.timeframe_minutes < 1:
            raise ValueError("nearby value reference requires stream identity")
        if not self.config_fingerprint or not self.profile_version:
            raise ValueError("nearby value reference requires profile provenance")
        if not self.aggregate_source_fingerprint:
            raise ValueError("nearby value reference requires source provenance")
        _require_id(self.snapshot_id, "nearby value reference")
        if self.vah < self.val:
            raise ValueError("nearby value reference value area is inverted")
        if self.distance < 0:
            raise ValueError("nearby value reference distance cannot be negative")
        require_aware(self.available_at, name="nearby value reference.available_at")


@dataclass(frozen=True, slots=True)
class NearbyValueAreaSelection:
    containing: NearbyValueAreaReference | None
    nearest_above: NearbyValueAreaReference | None
    nearest_below: NearbyValueAreaReference | None
    prior_session: NearbyValueAreaReference | None
    none_within_lookback: bool = False
    history_available: bool = False
