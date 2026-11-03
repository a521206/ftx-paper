"""Replay input loading, source selection, and neutral input manifests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole
from ftx_paper.market import AggregatorConfig, CompletedBarAggregator, InstrumentKey
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver, virtual_continuous_bar
from ftx_paper.runtime.input_identity import NORMALIZED_BAR_FINGERPRINT_RECIPE, normalized_bar_fingerprint
from ftx_paper.config import (
    IST, SESSION_DATA_END, SESSION_DATA_END_MINUTES, SESSION_OPEN_MINUTES,
)
from ftx_paper.strategies.research.config import AFTERNOON_ENTRY_MINUTES, MORNING_ENTRY_MINUTES
from ftx_paper.strategies.orderflow.timestamps import parse_utc


class ReplayInput:
    """Own deterministic replay input resolution and normalization."""

    def __init__(self, store: Any, source_resolver: Any | None = None) -> None:
        self.store = store
        self._sources = source_resolver or FuturesSessionSourceResolver(store)
        self._source_cache: dict[str, Any] = {}

    def reset(self) -> None:
        self._source_cache.clear()

    def resolve_source(
        self, session_date: str, *, rows: list[dict[str, Any]] | None = None,
    ) -> Any | None:
        if session_date not in self._source_cache:
            self._source_cache[session_date] = self._sources.resolve(session_date, rows=rows)
        return self._source_cache[session_date]

    def prior_source_provenance(self, session_date: str) -> dict[str, Any] | None:
        dates = sorted(value for value in self.store.read_dates() if value < session_date)
        if not dates:
            return None
        source = self.resolve_source(dates[-1])
        return source.provenance() if source is not None else None

    def bars(
        self, session_date: str, *, rows: list[dict[str, Any]] | None = None,
        futures_source: Any | None = None,
    ) -> tuple[MarketBar, ...]:
        loaded_rows = rows if rows is not None else self.store.read_market_bars(session_date)
        source = futures_source if futures_source is not None else self.resolve_source(
            session_date, rows=loaded_rows,
        )
        if any(str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"} for row in loaded_rows):
            loaded_rows = [
                row for row in loaded_rows
                if str(row.get("instrument_type", "")).upper() not in {"FUT", "FUTURES"}
                or (source is not None and str(row.get("symbol")) == source.symbol
                    and str(row.get("expiry", ""))[:10] == source.expiry)
            ]
        selected_times = [
            parse_utc(str(row["minute"]), field="replay minute") for row in loaded_rows
            if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
        ]
        if any(left >= right for left, right in zip(selected_times, selected_times[1:])):
            raise ValueError("futures source rows must be strictly chronological")
        bars = []
        for row in loaded_rows:
            kind = str(row.get("instrument_type", "")).upper()
            if kind not in {"FUT", "FUTURES", "INDEX", "EQ", "CE", "PE"}:
                continue
            timestamp = parse_utc(str(row["minute"]), field="replay minute")
            local = timestamp.astimezone(IST)
            minute = local.hour * 60 + local.minute
            if not SESSION_OPEN_MINUTES <= minute <= SESSION_DATA_END_MINUTES:
                continue
            selected = source is not None and kind in {"FUT", "FUTURES"} and (
                str(row.get("symbol")) == source.symbol
                and str(row.get("expiry", ""))[:10] == source.expiry
            )
            bar = MarketBar(
                instrument=Instrument(
                    symbol=str(row["symbol"]), exchange=str(row["exchange"]),
                    instrument_type=kind, expiry=row.get("expiry"), strike=row.get("strike"),
                ),
                timestamp=timestamp, open=float(row["open"]), high=float(row["high"]),
                low=float(row["low"]), close=float(row["close"]),
                volume=row.get("volume"), open_interest=row.get("open_interest"),
            )
            bars.append(virtual_continuous_bar(bar) if selected else bar)
        return tuple(sorted(
            bars,
            key=lambda bar: (bar.timestamp,
                             str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES"}),
        ))

    @staticmethod
    def aggregator(bars: tuple[MarketBar, ...]) -> CompletedBarAggregator:
        roles = {}
        for bar in bars:
            kind = str(bar.instrument.instrument_type).upper()
            role = (MarketRole.FUTURES if kind in {"FUT", "FUTURES"}
                    else MarketRole.VIX if bar.instrument.symbol.upper() == "INDIA VIX"
                    else OptionRole(bar.instrument.symbol) if kind in {"CE", "PE"}
                    else MarketRole.SPOT)
            roles[InstrumentKey(bar.instrument.exchange, bar.instrument.symbol)] = role
        return CompletedBarAggregator(
            roles, AggregatorConfig(required_roles=(MarketRole.FUTURES,), deadline_seconds=0),
        )

    def manifest(
        self, session_date: str, *, rows: list[dict[str, Any]] | None = None,
        bars: tuple[MarketBar, ...] | None = None, futures_source: Any | None = None,
    ) -> Mapping[str, Any]:
        loaded = rows if rows is not None else self.store.read_market_bars(session_date)
        source = futures_source if futures_source is not None else self.resolve_source(session_date)
        normalized = bars if bars is not None else self.bars(
            session_date, rows=loaded, futures_source=source,
        )
        replay_rows = [{
            "symbol": bar.instrument.symbol, "exchange": bar.instrument.exchange,
            "minute": bar.timestamp.isoformat(), "open": bar.open, "high": bar.high,
            "low": bar.low, "close": bar.close, "volume": bar.volume,
            "open_interest": bar.open_interest,
            "instrument_type": bar.instrument.instrument_type, "expiry": bar.instrument.expiry,
            "strike": bar.instrument.strike, "option_type": bar.instrument.option_type,
        } for bar in normalized]
        strategy_rows = [row for row in replay_rows
                         if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES", "INDEX"}
                         and (str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
                              or str(row.get("symbol", "")).upper() in {"NIFTY", "NIFTY 50", "INDIA VIX"})]
        selected_futures_rows = [row for row in strategy_rows
                                 if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}]
        per_instrument: dict[str, int] = {}
        for row in strategy_rows:
            key = ":".join(str(row.get(field) or "").upper()
                           for field in ("exchange", "symbol", "instrument_type"))
            if key:
                per_instrument[key] = per_instrument.get(key, 0) + 1
        vix = [row for row in replay_rows if str(row.get("symbol", "")).upper() == "INDIA VIX"]
        options = [row for row in replay_rows if str(row.get("instrument_type", "")).upper() in {"CE", "PE"}]
        option_minutes = {str(row.get("minute")) for row in options}
        future_minutes = {str(row.get("minute")) for row in loaded
                          if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}}
        provenance = {
            "current": source.provenance() if source is not None else None,
            "prior": self.prior_source_provenance(session_date),
        }
        completed = len({str(row.get("minute")) for row in replay_rows
                         if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}})
        current = provenance["current"]
        selected_count = int(current["source_bar_count"] if current is not None
                             and "source_bar_count" in current else completed)
        expected = SESSION_DATA_END_MINUTES - SESSION_OPEN_MINUTES + 1
        payload = {
            "available_date": session_date, "date": session_date, "session_date": session_date,
            "bar_count": len(strategy_rows), "raw_bar_count": len(loaded),
            "supporting_bar_count": len(normalized) - len(strategy_rows),
            "selected_source_rows": selected_count,
            "filtered_source_rows": max(0, selected_count - completed),
            "completed_source_rows": completed, "gap_rows": max(0, expected - completed),
            "futures_source": source.provenance() if source is not None else None,
            "provenance": provenance, "per_instrument_counts": dict(sorted(per_instrument.items())),
            "availability": {"vix": bool(vix), "pcr": bool(options and option_minutes.intersection(future_minutes)) or None},
            "expiry_calendar": sorted({str(item)[:10] for item in self.store.read_expiry_dates()
                                        if str(item)[:10] >= session_date} | {
                                            str(row["expiry"])[:10] for row in options
                                            if row.get("expiry") and str(row["expiry"])[:10] >= session_date}),
            "session_configuration": {
                "timezone": "Asia/Kolkata", "open": "09:15", "data_end": SESSION_DATA_END,
                "morning_entry_minutes": list(MORNING_ENTRY_MINUTES),
                "afternoon_entry_minutes": list(AFTERNOON_ENTRY_MINUTES),
                "aggregator": {"required_roles": ["futures"], "deadline_seconds": 0},
            },
            "fingerprint_recipe": NORMALIZED_BAR_FINGERPRINT_RECIPE,
            "normalized_fingerprint": normalized_bar_fingerprint(selected_futures_rows),
        }
        if isinstance(current, Mapping):
            provenance["current"] = {**current, "normalized_fingerprint": payload["normalized_fingerprint"]}
        return payload


__all__ = ["ReplayInput"]
