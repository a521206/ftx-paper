"""Session zone windows for value-profile ZONE scope (project Z1/Z2/Z3 clocks)."""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from ftx_paper.strategies.orderflow.canonical.ict.bars.aggregate import SessionSpec
from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar

# Align with project zone closes: Z1 through 11:00, Z2 through 13:00, Z3 through 15:00.
_Z1_END = time(11, 0)
_Z2_END = time(13, 0)


def session_zone_label(bar: CompletedBar, spec: SessionSpec = SessionSpec()) -> str:
    """Return Z1, Z2, or Z3 for the bar timestamp within the session clock."""
    zone = ZoneInfo(spec.timezone)
    local = bar.timestamp.astimezone(zone)
    start = datetime.combine(local.date(), spec.start, tzinfo=zone)
    if local < start:
        return "Z1"
    clock = local.timetz().replace(tzinfo=None)
    if clock < _Z1_END:
        return "Z1"
    if clock < _Z2_END:
        return "Z2"
    return "Z3"
