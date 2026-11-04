from datetime import datetime, timezone
from types import SimpleNamespace

from ftx_paper.strategies.orderflow.execution_clock import ExecutionClock


def test_management_runs_only_in_post_structure_execution_pass() -> None:
    lifecycle = SimpleNamespace(
        lifecycle_id="life-1",
        active=True,
        session_date="2026-10-01",
        formation_available_at=datetime(2026, 10, 1, 3, 40, tzinfo=timezone.utc),
        clock=None,
    )
    state = SimpleNamespace(
        position=SimpleNamespace(
            status=SimpleNamespace(value="open"),
            quantity=1.0,
            fill_price=100.0,
            failure_boundary=99.0,
            protective_stop=99.0,
            post_3r_activated=False,
            partial_exit_taken=False,
        ),
        pending_exit=None,
        intent=None,
    )
    calls: list[str] = []
    context = SimpleNamespace(
        opportunities={lifecycle.lifecycle_id: lifecycle},
        pending_bar_events=[],
        execution_proposals=[],
        execution_diagnostics=[],
        aggregates={"5m": []},
        regime=None,
        position_is_open=lambda _life: True,
        parity_snapshot=lambda: {},
    )
    owner = SimpleNamespace(
        engine=SimpleNamespace(
            state_for=lambda _lifecycle_id: state,
            setup_for=lambda _lifecycle_id: None,
        ),
        opportunity_expired=lambda _life, *, timestamp: False,
        apply_session_deadline=lambda *_args, **_kwargs: None,
        handle_management=lambda *_args, **_kwargs: calls.append("management"),
        handle_execution=lambda *_args, **_kwargs: calls.append("execution"),
        select_execution_proposals=lambda *_args, **_kwargs: None,
    )
    engine = SimpleNamespace(
        execution_owner=owner,
        context=context,
        observation_owner=SimpleNamespace(five_minute_aggregates=()),
        execution_adapter=None,
    )
    clock = ExecutionClock(engine)
    row = {"time": "09:50", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5}

    clock.settle_pending(
        index=0,
        timestamp="2026-10-01T03:50:00Z",
        next_time="2026-10-01T03:51:00Z",
        row=row,
        events=[],
    )
    clock.advance_post_structure(
        (SimpleNamespace(lifecycle=lifecycle, setup=None, available_at=lifecycle.formation_available_at),),
        index=0,
        timestamp="2026-10-01T03:50:00Z",
        next_time="2026-10-01T03:51:00Z",
        row=row,
    )

    assert calls == ["execution", "management", "execution"]
