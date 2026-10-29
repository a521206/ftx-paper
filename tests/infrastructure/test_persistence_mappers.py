from datetime import date, datetime, timezone

from ftx_paper.infrastructure.sqlite.mappers import (
    auth_token_from_row, contract_from_row, event_from_row, replay_run_from_row,
)
from ftx_paper.ports.records import AuthToken, StoredEvent


def test_event_row_maps_to_typed_record():
    record = event_from_row({"id": 3, "event_type": "WARMUP", "payload": '{"x": 1}', "created_at": "2026-01-01T00:00:00+00:00"})
    assert record == StoredEvent(3, "WARMUP", {"x": 1}, datetime(2026, 1, 1, tzinfo=timezone.utc))


def test_replay_contract_and_token_rows_map_without_raw_rows():
    replay = replay_run_from_row({"run_id": "r1", "status": "queued", "request": '{}', "result": None, "error": None, "created_at": "2026-01-01T00:00:00+00:00", "updated_at": "2026-01-01T00:00:00+00:00"})
    contract = contract_from_row({"exchange": "NFO", "underlying": "NIFTY", "role": "futures", "tradingsymbol": "NIFTYFUT", "instrument_token": 1, "expiry": "2026-01-29", "selected_at": "2026-01-01T00:00:00+00:00"})
    token = auth_token_from_row({"provider": "zerodha", "token_date": "2026-01-01", "access_token": "secret"})
    assert replay.run_id == "r1"
    assert contract.expiry == date(2026, 1, 29)
    assert token == AuthToken("zerodha", date(2026, 1, 1), "secret")
