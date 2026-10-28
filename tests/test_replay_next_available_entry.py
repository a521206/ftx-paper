from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.domain.capital import RESEARCH_CAPITAL_PROFILE
from ftx_paper.domain.portfolio import PortfolioState
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.strategy import OrderflowStrategy
from tests.test_orderflow_strategy import _context


FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "next_available_entry.json").read_text(),
)
class _CanonicalReplayAggregator:
    def __init__(self, bundles):
        self.bundles = iter(bundles)
        self.calls = 0

    def ingest(self, bar):
        self.calls += 1
        return next(self.bundles, None)


class _CanonicalFactory:
    def create(self, **kwargs):
        return OrderflowStrategy(config=kwargs["config"])


def _worker(tmp_path):
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES", FIXTURE["session_date"])
    bars = tuple(
        MarketBar(
            instrument,
            datetime.fromisoformat(item["timestamp"]),
            item["open"], item["high"], item["low"], item["close"],
        )
        for item in FIXTURE["bars"]
    )
    selected, account = _context(response_close=101.0)
    bundles = tuple(
        replace(
            selected,
            bundle_id=f"{FIXTURE['session_date']}:{bar.timestamp.isoformat()}",
            trading_date=FIXTURE["session_date"],
            minute=bar.timestamp.isoformat(),
            bars={MarketRole.FUTURES: bar},
        )
        for bar in bars
    )
    worker = ReplayWorker(RuntimeStore(tmp_path), strategy_factory=_CanonicalFactory())
    worker._bars = lambda session_date, **kwargs: bars
    aggregator = _CanonicalReplayAggregator(bundles)
    setattr(worker, "_aggregator", lambda *args, **kwargs: aggregator)
    return worker, bundles[0], account, bars, aggregator


def test_canonical_replay_entry_uses_next_available_bar_and_is_idempotent(tmp_path) -> None:
    worker, diagnostic_bundle, diagnostic_account, bars, aggregator = _worker(tmp_path)
    request = {
        "session_date": FIXTURE["session_date"],
        "strategy_id": "ftx-paper-orderflow",
        "vehicles": ["futures"],
        "mode": "replay",
    }

    first = worker.execute_detached(request)
    assert aggregator.calls == 2
    second_worker, _, _, _, _ = _worker(tmp_path / "repeat")
    second = second_worker.execute_detached(request)

    def values(result, event_type):
        return [event["values"] for event in result["events"]
                if event["values"].get("event_type") == event_type]

    fills = values(first, "FILL")
    reservations = values(first, "RESERVATION")
    assert len(fills) == 1
    assert len(reservations) == 1
    assert fills[0]["fill_timestamp"] == bars[1].timestamp.isoformat()
    assert fills[0]["price"] == bars[1].open
    assert all(
        event.get("timestamp", event.get("fill_timestamp")) != bars[0].timestamp.isoformat()
        for event in fills + reservations
    )

    def stable(events):
        return [{key: value for key, value in event.items()
                 if key not in {"run_id", "neutral_event_key"}} for event in events]

    assert stable(values(first, "FILL") + values(first, "RESERVATION")) == stable(
        values(second, "FILL") + values(second, "RESERVATION"),
    )
    assert first["shadow_ledger"] == second["shadow_ledger"]

    diagnostic_portfolio = PortfolioState(RESEARCH_CAPITAL_PROFILE.initial_capital)
    diagnostic_context = replace(
        diagnostic_account,
        margin=replace(diagnostic_account.margin, available=diagnostic_portfolio.available_capital),
    )
    diagnostic = PaperEngine(OrderflowStrategy()).on_bundle(
        diagnostic_bundle, context=diagnostic_context,
    )
    assert diagnostic.orders == ()
    assert diagnostic_portfolio.reservations == {}
    assert diagnostic_portfolio.positions == {}
