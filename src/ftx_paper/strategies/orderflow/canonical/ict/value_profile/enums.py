"""Vocabulary for the causal value-profile track."""

from enum import StrEnum


class ProfileScope(StrEnum):
    SESSION_DEVELOPING = "session_developing"
    PRIOR_SESSION = "prior_session"
    SESSION_TO_DATE = "session_to_date"  # reserved; not enabled in ValueProfileConfig v1
    ROLLING_BARS = "rolling_bars"
    ZONE = "zone"


class SnapshotStatus(StrEnum):
    DEVELOPING = "developing"
    FROZEN = "frozen"


class DailyProfileQuality(StrEnum):
    """Coverage quality for a persisted session daily profile."""

    COMPLETE = "complete"
    INCOMPLETE = "incomplete"


class NearbyReferenceRole(StrEnum):
    """How a historical value area relates to the active candle."""

    CONTAINING = "containing"
    NEAREST_ABOVE = "nearest_above"
    NEAREST_BELOW = "nearest_below"
    PRIOR_SESSION = "prior_session"


class ProfileLevelRole(StrEnum):
    POC = "poc"
    VAH = "vah"
    VAL = "val"
    VWAP = "vwap"


class ValuePosition(StrEnum):
    INSIDE = "inside"
    ABOVE_VAH = "above_vah"
    BELOW_VAL = "below_val"
    AT_POC = "at_poc"


class ProfileEventKind(StrEnum):
    PUBLISHED = "published"
    ENTERED_VALUE = "entered_value"
    EXITED_VALUE = "exited_value"
    POC_TOUCH = "poc_touch"
