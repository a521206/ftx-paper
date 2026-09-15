from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Mapping


_OBSOLETE_ALIASES = frozenset({"time", "timestamp", "qty", "quantity", "reason_code"})


def _required(data: Mapping[str, Any], name: str, *, allow_none: bool = False) -> Any:
    if name not in data or (not allow_none and data[name] is None):
        raise ValueError(f"missing required field: {name}")
    return data[name]


def _decision_at(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("decision_at must be a timezone-aware datetime")
    return value


def _check_aliases(data: Mapping[str, Any]) -> None:
    found = _OBSOLETE_ALIASES.intersection(data)
    if found:
        raise ValueError("obsolete decision aliases are not accepted: " + ", ".join(sorted(found)))


def _to_dict(values: Mapping[str, Any], *, datetime_fields: tuple[str, ...] = ()) -> dict[str, Any]:
    result = dict(values)
    for name in datetime_fields:
        value = result[name]
        result[name] = value.isoformat() if value is not None else None
    return result


def _from_dict(
    data: Mapping[str, Any],
    fields: tuple[str, ...],
    *,
    nullable_fields: frozenset[str] = frozenset(),
    datetime_fields: tuple[str, ...] = (),
) -> dict[str, Any]:
    _check_aliases(data)
    values = {
        name: _required(data, name, allow_none=name in nullable_fields)
        for name in fields
    }
    for name in datetime_fields:
        if isinstance(values[name], str):
            values[name] = datetime.fromisoformat(values[name])
    return values


@dataclass(frozen=True, slots=True)
class TradePlan:
    decision_id: str
    sequence: int
    decision_at: datetime
    cell: str
    direction: str
    entry_price: float
    setup_type: str
    score: float
    score_factors: Mapping[str, Any]
    vix: float | None
    pcr: float | None
    stop: float
    requested_quantity: int
    final_quantity: int
    outcome: str
    rejection_reason: str | None = None
    exit_mode: str | None = None
    exit_result: Mapping[str, Any] | None = None

    _FIELDS: ClassVar[tuple[str, ...]] = (
        "decision_id", "sequence", "decision_at", "cell", "direction", "entry_price",
        "setup_type", "score", "score_factors", "vix", "pcr", "stop",
        "requested_quantity", "final_quantity", "outcome", "rejection_reason",
        "exit_mode", "exit_result",
    )

    def __post_init__(self) -> None:
        _decision_at(self.decision_at)
        if not self.decision_id or self.sequence < 0:
            raise ValueError("decision identity and sequence are required")
        if self.requested_quantity < 0 or self.final_quantity < 0:
            raise ValueError("quantities cannot be negative")
        if self.outcome in {"accepted", "filled"} and self.final_quantity < 1:
            raise ValueError("accepted decisions require a positive final quantity")
        if self.rejection_reason is None and self.outcome in {"rejected", "policy rejection", "sizing rejection"}:
            raise ValueError("rejected decisions require rejection_reason")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict({name: getattr(self, name) for name in self._FIELDS}, datetime_fields=("decision_at",))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TradePlan:
        return cls(**_from_dict(
            data,
            cls._FIELDS,
            nullable_fields=frozenset({"vix", "pcr", "rejection_reason", "exit_mode", "exit_result"}),
            datetime_fields=("decision_at",),
        ))


@dataclass(frozen=True, slots=True)
class FuturesExecutionPlan:
    trade_plan: TradePlan
    instrument: str
    entry_order_id: str
    exit_mode: str
    exit_result: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.trade_plan.outcome not in {"accepted", "filled"}:
            raise ValueError("futures execution requires an accepted trade plan")
        if not self.instrument or not self.entry_order_id or not self.exit_mode:
            raise ValueError("futures execution identity and exit mode are required")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict({"trade_plan": self.trade_plan.to_dict(), "instrument": self.instrument,
                         "entry_order_id": self.entry_order_id, "exit_mode": self.exit_mode,
                         "exit_result": self.exit_result})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FuturesExecutionPlan:
        values = _from_dict(
            data,
            ("trade_plan", "instrument", "entry_order_id", "exit_mode", "exit_result"),
            nullable_fields=frozenset({"exit_result"}),
        )
        values["trade_plan"] = TradePlan.from_dict(values["trade_plan"])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class SyntheticSettlement:
    plan_id: str
    status: str
    contract: Mapping[str, Any] | None
    entry_at: datetime
    exit_at: datetime
    entry_premiums: Mapping[str, float] | None
    exit_premiums: Mapping[str, float] | None
    costs: float | None
    pnl: float | None

    STATUSES: ClassVar[frozenset[str]] = frozenset({"settled", "missing_entry_premium", "missing_exit_premium", "invalid_contract"})

    def __post_init__(self) -> None:
        if not self.plan_id or self.status not in self.STATUSES:
            raise ValueError("invalid synthetic settlement identity or status")
        _decision_at(self.entry_at)
        _decision_at(self.exit_at)
        if self.status != "settled" and (self.pnl is not None or self.costs is not None):
            raise ValueError("unavailable synthetic settlements cannot contain P&L or costs")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict({"plan_id": self.plan_id, "status": self.status, "contract": self.contract,
                         "entry_at": self.entry_at, "exit_at": self.exit_at,
                         "entry_premiums": self.entry_premiums, "exit_premiums": self.exit_premiums,
                         "costs": self.costs, "pnl": self.pnl}, datetime_fields=("entry_at", "exit_at"))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SyntheticSettlement:
        return cls(**_from_dict(data, ("plan_id", "status", "contract", "entry_at", "exit_at",
                                       "entry_premiums", "exit_premiums", "costs", "pnl"),
                                nullable_fields=frozenset({"contract", "entry_premiums", "exit_premiums", "costs", "pnl"}),
                                datetime_fields=("entry_at", "exit_at")))


@dataclass(frozen=True, slots=True)
class DecisionTrace:
    plan: TradePlan
    gate: str
    details: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return _to_dict({"plan": self.plan.to_dict(), "gate": self.gate, "details": dict(self.details)})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DecisionTrace:
        values = _from_dict(data, ("plan", "gate", "details"))
        values["plan"] = TradePlan.from_dict(values["plan"])
        return cls(**values)


__all__ = ["TradePlan", "FuturesExecutionPlan", "SyntheticSettlement", "DecisionTrace"]
