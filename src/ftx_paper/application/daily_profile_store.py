"""Composition adapter for the daily profile persistence port."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from ftx_paper.infrastructure.sqlite.daily_profile_repository import SqliteDailyProfileRepository
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import DailyProfileQuality
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import DailyProfileRecord
from ftx_paper.strategies.orderflow.timestamps import parse_utc, serialize_utc


def _encode_datetime(value: datetime) -> str:
    return serialize_utc(value, field="daily profile timestamp")


def _decode_datetime(value: str) -> datetime:
    return parse_utc(value, field="daily profile timestamp")


def daily_profile_to_dict(record: DailyProfileRecord) -> dict[str, object]:
    payload = asdict(record)
    payload["quality"] = record.quality.value
    payload["window_first_bar_ts"] = _encode_datetime(record.window_first_bar_ts)
    payload["window_last_bar_ts"] = _encode_datetime(record.window_last_bar_ts)
    payload["observed_at"] = _encode_datetime(record.observed_at)
    payload["available_at"] = _encode_datetime(record.available_at)
    return payload


def daily_profile_from_dict(payload: dict[str, object]) -> DailyProfileRecord:
    vwap_raw = payload.get("vwap")
    return DailyProfileRecord(
        snapshot_id=str(payload["snapshot_id"]),
        instrument=str(payload["instrument"]),
        timeframe_minutes=int(str(payload["timeframe_minutes"])),
        session_date=str(payload["session_date"]),
        poc=float(str(payload["poc"])),
        vah=float(str(payload["vah"])),
        val=float(str(payload["val"])),
        vwap=float(str(vwap_raw)) if vwap_raw is not None else None,
        window_first_bar_ts=_decode_datetime(str(payload["window_first_bar_ts"])),
        window_last_bar_ts=_decode_datetime(str(payload["window_last_bar_ts"])),
        bar_count=int(str(payload["bar_count"])),
        total_volume=float(str(payload["total_volume"])),
        gap_count=int(str(payload["gap_count"])),
        quality=DailyProfileQuality(str(payload["quality"])),
        observed_at=_decode_datetime(str(payload["observed_at"])),
        available_at=_decode_datetime(str(payload["available_at"])),
        config_fingerprint=str(payload["config_fingerprint"]),
        profile_version=str(payload["profile_version"]),
        aggregate_source_fingerprint=str(payload["aggregate_source_fingerprint"]),
    )


class DatabaseDailyProfileStore:
    """Adapt SQLite daily profile rows to the strategy store port."""

    def __init__(self, path: str | Path) -> None:
        self._repo = SqliteDailyProfileRepository(path)
        self._repo.ensure_schema()

    def load(
        self,
        *,
        instrument: str,
        timeframe_minutes: int,
        config_fingerprint: str,
        session_date: str,
    ) -> DailyProfileRecord | None:
        payload = self._repo.load(
            instrument=instrument,
            timeframe_minutes=timeframe_minutes,
            config_fingerprint=config_fingerprint,
            session_date=session_date,
        )
        if payload is None:
            return None
        return daily_profile_from_dict(payload)

    def load_all(
        self,
        *,
        instrument: str,
        timeframe_minutes: int,
        config_fingerprint: str,
    ) -> tuple[DailyProfileRecord, ...]:
        return tuple(
            daily_profile_from_dict(payload)
            for payload in self._repo.load_all(
                instrument=instrument,
                timeframe_minutes=timeframe_minutes,
                config_fingerprint=config_fingerprint,
            )
        )

    def save(self, record: DailyProfileRecord) -> None:
        self._repo.save(daily_profile_to_dict(record))


__all__ = ["DatabaseDailyProfileStore"]
