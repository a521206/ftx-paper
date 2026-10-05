"""Broker-neutral normalization of raw quote payloads into market contracts."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

from ftx_paper.contracts import Instrument, MarketBar, OptionType, normalize_exchange_timestamp


class LiveMarketNormalizer:
    """Normalize broker payloads while keeping broker adapters out of market logic."""

    def __init__(
        self,
        instruments: Sequence[Mapping[str, Any]],
        *,
        expiry_normalizer: Callable[[object], str | None],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._by_token = {int(item["instrument_token"]): item for item in instruments}
        self._expiry_normalizer = expiry_normalizer
        self._clock = clock or (lambda: datetime.now().astimezone())

    def __call__(self, payload: Mapping[str, object]) -> MarketBar:
        raw_token = payload["instrument_token"]
        if not isinstance(raw_token, (int, str)):
            raise ValueError("market payload instrument_token is invalid")
        item = self._by_token[int(raw_token)]
        timestamp = self._timestamp(payload.get("timestamp"))
        instrument_type = str(item.get("instrument_type", "INDEX"))
        if str(item["symbol"]).upper() in {"NIFTY", "NIFTY 50", "INDIA VIX"}:
            instrument_type = "INDEX"
        expiry = (
            self._expiry_normalizer(item.get("expiry"))
            if instrument_type in {"FUT", "CE", "PE"} else None
        )
        raw_strike = item.get("strike")
        strike = float(raw_strike) if raw_strike is not None and instrument_type in {"CE", "PE"} else None
        instrument = Instrument(
            str(item["symbol"]), str(item["exchange"]), instrument_type,
            expiry=expiry,
            strike=strike,
            option_type=OptionType(instrument_type) if instrument_type in {"CE", "PE"} else None,
        )
        open_interest = payload.get("oi")
        if instrument_type == "FUT" and open_interest is None:
            open_interest = 0.0
        raw_price = payload["last_price"]
        if not isinstance(raw_price, (int, float, str)):
            raise ValueError("market payload last_price is invalid")
        price = float(raw_price)
        raw_volume = payload.get("volume_traded")
        volume = float(raw_volume) if isinstance(raw_volume, (int, float)) else None
        if isinstance(open_interest, (int, float)):
            open_interest = float(open_interest)
        else:
            open_interest = None
        return MarketBar(
            instrument, timestamp, price, price, price, price,
            volume, open_interest,
        )

    def _timestamp(self, raw_timestamp: object) -> datetime:
        if isinstance(raw_timestamp, (int, float, str)):
            try:
                value = float(raw_timestamp)
                if value > 100_000_000_000:
                    value /= 1000
                return normalize_exchange_timestamp(value)
            except (OverflowError, TypeError, ValueError):
                pass
        return normalize_exchange_timestamp(self._clock())


__all__ = ["LiveMarketNormalizer"]
