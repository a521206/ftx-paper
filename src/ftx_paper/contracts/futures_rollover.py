"""Canonical dated-futures rollover policy shared by live and replay selection.

Live startup resolution and replay session resolution must agree on which dated
contract is live for a session. Both call :func:`select_front_futures_contract`
so the rule cannot drift between the two paths.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

SELECTION_POLICY_VERSION = "canonical-expiry-session"
ROLLOVER_RULE = "keep the nearest contract through its expiry session"


@dataclass(frozen=True, slots=True)
class FuturesContract:
    """One dated futures contract eligible for selection."""

    symbol: str
    expiry: date
    payload: Any = None


@dataclass(frozen=True, slots=True)
class RolloverSelection:
    """The contract selected for a session, with the decision that chose it."""

    contract: FuturesContract
    session_date: date
    policy_version: str = SELECTION_POLICY_VERSION

    @property
    def symbol(self) -> str:
        return self.contract.symbol

    @property
    def expiry(self) -> date:
        return self.contract.expiry

    @property
    def payload(self) -> Any:
        return self.contract.payload

    def decision(self) -> dict[str, Any]:
        return {
            "rule": ROLLOVER_RULE,
            "front_expiry": self.expiry.isoformat(),
            "days_to_front_expiry": (self.expiry - self.session_date).days,
            "selected_expiry": self.expiry.isoformat(),
        }


def parse_expiry(value: object) -> date | None:
    """Return a ``date`` for date-like broker or stored values, or None."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _contract_expiry(contract: FuturesContract) -> date | None:
    expiry = contract.expiry
    return expiry if isinstance(expiry, date) else parse_expiry(expiry)


def select_front_futures_contract(
    contracts: Iterable[FuturesContract],
    *,
    session_date: date,
) -> RolloverSelection | None:
    """Select the nearest unexpired contract, inclusive of its expiry session.

    ``session_date`` is the trading date the selection must serve. Ties on
    expiry break on symbol so the result is deterministic regardless of input
    order, which keeps replay fingerprints stable.
    """
    eligible = [
        contract for contract in contracts
        if (expiry := _contract_expiry(contract)) is not None and expiry >= session_date
    ]
    if not eligible:
        return None
    selected = min(eligible, key=lambda item: (_contract_expiry(item), item.symbol))
    return RolloverSelection(contract=selected, session_date=session_date)


def futures_contract_from_row(row: Mapping[str, Any]) -> FuturesContract | None:
    """Build a contract from a broker instrument row, or None when unusable."""
    expiry = parse_expiry(row.get("expiry"))
    symbol = str(row.get("tradingsymbol") or row.get("symbol") or "")
    if expiry is None or not symbol:
        return None
    return FuturesContract(symbol=symbol, expiry=expiry, payload=row)


__all__ = [
    "FuturesContract", "ROLLOVER_RULE", "RolloverSelection", "SELECTION_POLICY_VERSION",
    "futures_contract_from_row", "parse_expiry", "select_front_futures_contract",
]
