"""Canonical evidence and formation providers for the Paper host boundary.

Providers own ingress and publication timing only.  They do not decide policy,
repair gaps, or derive facts from terminal transition state.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from ftx_paper.contracts import MarketRole

from .coordinator import OrderflowClockInput
from .evidence.features_adapter import PaperFeatureAdapter
from .evidence.producer import produce_evidence
from .orderflow_common import Aggregate
from .evidence_publication import EvidencePublication
from .context_location_replay import _causal_children, _location_observation
from .timestamps import coerce_timestamp, nifty_label
from .vwap_location import causal_vwap_prefix
from .opportunity_coordinator import LifecycleFormation
from .feature_store import FeatureStore


def _aggregate(value: Mapping[str, Any], timeframe: str) -> Aggregate:
    return Aggregate(
        coerce_timestamp(str(value["time"])),
        coerce_timestamp(str(value["end_time"])),
        coerce_timestamp(str(value["available_at"])),
        timeframe,
        int(value["source_bar_count"]),
        float(value["open"]), float(value["high"]), float(value["low"]),
        float(value["close"]), int(value["volume"]), str(value["symbol"]),
        str(value.get("expiry", "")), str(value.get("contract_type", "CONTINUOUS")),
        str(value["session_date"]), str(value["source_fingerprint"]),
    )


@dataclass(frozen=True, slots=True)
class CanonicalInputEnvelope:
    """One provider publication and its causal lineage."""

    input: OrderflowClockInput
    source_ids: tuple[str, ...]
    unavailable: tuple[str, ...] = ()


class CanonicalInputProvider:
    """Produce the same causal clock inputs for bounded replay and live ingress.

    ``rows`` is a bounded, already selected source scope.  Live callers can use
    ``from_bundle`` with a bundle containing an explicit ``orderflow_clock_input``
    or a ``CanonicalInputEnvelope``; no missing publication is inferred.
    """

    def __init__(
        self,
        rows: Sequence[Mapping[str, Any]] = (),
        *,
        rows_by_date: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
        feature_store: FeatureStore | None = None,
    ) -> None:
        dates = dict(rows_by_date or {})
        if rows and not dates:
            dates[str(rows[0]["session_date"])] = rows
        self._feature_store = feature_store or FeatureStore(dates)
        self._rows = tuple(dict(row) for row in rows)
        if not self._rows and dates:
            target = max(str(key) for key in dates)
            self._rows = tuple(self._feature_store.rows_by_date[target])
        self._by_identity: dict[str, CanonicalInputEnvelope] = {}
        self._build()

    def _build(self) -> None:
        self._by_identity = {}
        if not self._rows:
            return
        from .feature_input import build_observation_artifact, build_prior_values

        session_date = str(self._rows[0]["session_date"])
        if any(str(row.get("session_date")) != session_date for row in self._rows):
            raise ValueError("canonical input rows must belong to one session")
        artifact, events = build_observation_artifact([dict(row) for row in self._rows])
        parsed = {
            timeframe: tuple(_aggregate(item, timeframe) for item in artifact["aggregates"][timeframe])
            for timeframe in ("1m", "5m")
        }
        rows_by_date = self._feature_store.rows_by_date
        adapter = PaperFeatureAdapter(rows_by_date, prior_values=build_prior_values(rows_by_date))
        vwap = {
            timeframe: causal_vwap_prefix(values, session_date=session_date)
            for timeframe, values in parsed.items()
        }
        available: dict[str, list[dict[str, Any]]] = {"1m": [], "5m": []}
        for event in events:
            available[event.timeframe].append(event.value)
            if event.timeframe != "1m":
                continue
            available_at = event.available_at
            available = {
                timeframe: [item for item in values
                            if coerce_timestamp(str(item["available_at"])) <= available_at]
                for timeframe, values in {
                    "1m": [item.value for item in events if item.timeframe == "1m"],
                    "5m": [item.value for item in events if item.timeframe == "5m"],
                }.items()
            }
            prefix = {
                **artifact,
                "aggregates": {key: list(value) for key, value in available.items()},
            }
            evidence = produce_evidence(
                prefix,
                session_date=session_date,
                adapter=adapter,
                parsed_aggregates={
                    key: tuple(_aggregate(item, key) for item in value)
                    for key, value in available.items()
                },
                precomputed_vwap_measurements=vwap,
            )["publication"]
            if not isinstance(evidence, EvidencePublication):
                raise TypeError("canonical evidence producer returned an invalid publication")
            if event.timeframe != "1m":
                continue
            one = _aggregate(event.value, "1m")
            latest5 = max(
                (_aggregate(item, "5m") for item in available["5m"]),
                key=lambda item: item.available_at,
                default=None,
            )
            atr = evidence.atr
            levels = adapter.session_levels(session_date)
            prior = adapter.prior_value(session_date)
            source_id = f"aggregate:1m:{nifty_label(one.time)}"
            unavailable = tuple(evidence.unavailable)
            observations_1m = []
            observations_5m = []
            aggregates_by_timeframe = {
                "1m": tuple(_aggregate(item, "1m") for item in available["1m"]),
                "5m": tuple(_aggregate(item, "5m") for item in available["5m"]),
            }
            for record in evidence.locations:
                if record.available_at > one.available_at:
                    continue
                values = aggregates_by_timeframe.get(record.timeframe, ())
                source = next(
                    (item for item in values
                     if nifty_label(item.time) == str(record.source_aggregate_id)),
                    None,
                )
                if source is None:
                    continue
                # The detector evaluates one completed 5m aggregate at a
                # time. Older observations remain causally available but must
                # not be replayed with the current aggregate.
                if source.timeframe == "5m" and (latest5 is None or source.time != latest5.time):
                    continue
                children = _causal_children(source, aggregates_by_timeframe["1m"])
                if source.timeframe == "5m" and len(children) != 5:
                    continue
                if source.timeframe == "5m":
                    child_observations = tuple(
                        _location_observation(
                            item, record, source_fingerprint=source.source_fingerprint,
                            available_at=max(
                                coerce_timestamp(item.available_at),
                                coerce_timestamp(record.available_at),
                            ),
                        )
                        for item in children
                    )
                    observations_1m.extend(child_observations)
                    observations_5m.append(_location_observation(
                        source, record, source_fingerprint=source.source_fingerprint,
                        child_observation_ids=tuple(item.observation_id for item in child_observations),
                    ))
                else:
                    observations_1m.append(_location_observation(
                        source, record, source_fingerprint=source.source_fingerprint,
                    ))
            self._by_identity[source_id] = CanonicalInputEnvelope(
                OrderflowClockInput(
                    one_minute=one,
                    five_minute=latest5,
                    atr=atr,
                    session_levels=levels,
                    prior_session=adapter.prior_session(session_date),
                    prior_value=prior,
                    vwap_measurements=tuple(evidence.vwap_measurements),
                    observations_1m=tuple(observations_1m),
                    observations_5m=tuple(observations_5m),
                ),
                (source_id, *atr.lineage, *prior.lineage),
                unavailable,
            )

    def __call__(self, bundle: object) -> OrderflowClockInput:
        return self.from_bundle(bundle).input

    @property
    def envelopes(self) -> tuple[CanonicalInputEnvelope, ...]:
        """Published bounded prefix in canonical availability order."""
        return tuple(self._by_identity.values())

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[Mapping[str, Any]],
        *,
        rows_by_date: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
    ) -> "CanonicalInputProvider":
        return cls(rows, rows_by_date=rows_by_date)

    @classmethod
    def from_market_bars(cls, bars: Sequence[Any]) -> "CanonicalInputProvider":
        """Adapt Paper completed futures bars into the canonical source shape."""
        rows = cls._rows_from_market_bars(bars)
        rows_by_date: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            rows_by_date.setdefault(str(row["session_date"]), []).append(row)
        target = max(rows_by_date, default=None)
        return cls(
            rows_by_date.get(target, ()) if target is not None else (),
            rows_by_date=rows_by_date,
        )

    def append_market_bars(self, bars: Sequence[Any]) -> None:
        rows = self._rows_from_market_bars(bars)
        if not rows:
            return
        self._feature_store.append(rows)
        session_date = str(rows[-1]["session_date"])
        self._rows = tuple(self._feature_store.rows_by_date[session_date])
        self._build()

    @staticmethod
    def _rows_from_market_bars(bars: Sequence[Any]) -> list[dict[str, Any]]:
        return [
            {
                "time": nifty_label(bar.timestamp), "open": bar.open, "high": bar.high,
                "low": bar.low, "close": bar.close, "volume": int(bar.volume or 0),
                "oi": int(bar.open_interest or 0), "symbol": bar.instrument.symbol,
                "expiry": bar.instrument.expiry or "", "contract_type": bar.instrument.instrument_type,
                "session_date": nifty_label(bar.timestamp)[:10],
            }
            for bar in bars
            if getattr(bar.instrument, "instrument_type", "") in {"FUT", "FUTURES"}
        ]

    def from_bundle(self, bundle: object) -> CanonicalInputEnvelope:
        if isinstance(bundle, Mapping):
            supporting = bundle.get("supporting_inputs")
            if isinstance(supporting, Mapping):
                supplied = supporting.get("orderflow_clock_input")
                if isinstance(supplied, OrderflowClockInput):
                    return CanonicalInputEnvelope(supplied, (), ())
                envelope = supporting.get("canonical_input")
                if isinstance(envelope, CanonicalInputEnvelope):
                    return envelope
            supplied = bundle.get("orderflow_clock_input")
            if isinstance(supplied, OrderflowClockInput):
                return CanonicalInputEnvelope(supplied, (), ())
            envelope = bundle.get("canonical_input")
            if isinstance(envelope, CanonicalInputEnvelope):
                return envelope
        if hasattr(bundle, "values"):
            return self.from_evidence(bundle)
        supporting = getattr(bundle, "supporting_inputs", None)
        if isinstance(supporting, Mapping):
            supplied = supporting.get("orderflow_clock_input")
            if isinstance(supplied, OrderflowClockInput):
                return CanonicalInputEnvelope(supplied, (), ())
            envelope = supporting.get("canonical_input")
            if isinstance(envelope, CanonicalInputEnvelope):
                return envelope
        bars = getattr(bundle, "bars", {})
        bar = bars.get(MarketRole.FUTURES) if isinstance(bars, Mapping) else None
        timestamp = getattr(bar, "timestamp", None)
        if bar is not None and timestamp is not None:
            timestamp = coerce_timestamp(timestamp)
            identity = f"aggregate:1m:{nifty_label(timestamp)}"
            if identity in self._by_identity:
                return self._by_identity[identity]
            self.append_market_bars((bar,))
            if identity in self._by_identity:
                return self._by_identity[identity]
            raise ValueError(f"completed futures bar was not published: {identity}")
        raise ValueError("bundle has no canonical orderflow clock input")

    def from_evidence(self, evidence: object) -> CanonicalInputEnvelope:
        """Consume an explicit completed-bar evidence envelope from live ingress."""
        values = getattr(evidence, "values", None)
        if not isinstance(values, Mapping) and isinstance(evidence, Mapping):
            values = evidence.get("values")
        if isinstance(values, Mapping):
            supplied = values.get("canonical_input", values.get("orderflow_clock_input"))
            if isinstance(supplied, CanonicalInputEnvelope):
                return supplied
            if isinstance(supplied, OrderflowClockInput):
                return CanonicalInputEnvelope(supplied, (), ())
        raise ValueError("evidence has no explicit canonical orderflow input")


class CanonicalFormationProvider:
    """Expose canonical owner formations once, in causal order."""

    def __init__(
        self,
        owner: object | None = None,
        *,
        formations: Iterable[LifecycleFormation] = (),
        source: Callable[[OrderflowClockInput], Iterable[LifecycleFormation]] | None = None,
    ) -> None:
        if source is None and owner is not None:
            candidate = getattr(owner, "formations_for", None)
            if callable(candidate):
                owner_formations = cast(
                    Callable[[object], Iterable[LifecycleFormation]], candidate,
                )

                def owner_source(clock: OrderflowClockInput) -> Iterable[LifecycleFormation]:
                    return owner_formations(clock.one_minute.available_at)

                source = owner_source
        self._source = source
        self._formations = tuple(formations)
        self._seen: set[str] = set()

    def __call__(self, clock: OrderflowClockInput) -> tuple[LifecycleFormation, ...]:
        supplied = self._source(clock) if self._source is not None else self._formations
        result: list[LifecycleFormation] = []
        for item in supplied:
            identity = item.identity or item.lifecycle_id
            if identity in self._seen:
                continue
            if coerce_timestamp(item.available_at) > clock.one_minute.available_at:
                continue
            self._seen.add(identity)
            result.append(item)
        return tuple(sorted(result, key=lambda item: (coerce_timestamp(item.available_at), item.identity)))


__all__ = ["CanonicalFormationProvider", "CanonicalInputEnvelope", "CanonicalInputProvider"]
