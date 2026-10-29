from __future__ import annotations

from types import SimpleNamespace

from ftx_paper.runtime.decision_turn import DecisionTurn, evaluate_turn


def test_live_and_replay_turns_use_the_same_engine_dispatch() -> None:
    calls: list[tuple[str, object | None]] = []

    class Engine:
        def on_bundle(self, bundle, *, context=None):
            calls.append((bundle.bundle_id, context))
            return SimpleNamespace(orders=(), events=())

    engine = Engine()
    bundle = SimpleNamespace(bundle_id="session:09:15")
    context = object()

    evaluate_turn(engine, DecisionTurn(bundle, "live", context))
    evaluate_turn(engine, DecisionTurn(bundle, "replay", context))

    assert calls == [("session:09:15", context), ("session:09:15", context)]


def test_turn_dispatch_keeps_legacy_bundle_only_strategies_compatible() -> None:
    calls: list[object] = []

    class LegacyEngine:
        def on_bundle(self, bundle):
            calls.append(bundle)
            return SimpleNamespace(orders=(), events=())

    bundle = SimpleNamespace(bundle_id="session:09:15")
    evaluate_turn(LegacyEngine(), DecisionTurn(bundle, "replay", object()))

    assert calls == [bundle]
