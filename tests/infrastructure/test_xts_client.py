from unittest.mock import MagicMock

import requests

from ftx_paper.broker.xts.client import XTSClient


def test_ohlc_refreshes_once_after_invalid_token():
    expired = MagicMock(status_code=400, text='{"code":"e-session-0007"}')
    valid = MagicMock(
        status_code=200,
        json=lambda: {"type": "success", "result": {"dataResponse": "1|1|2|0|1|10|0"}},
    )
    session = MagicMock()
    session.get.side_effect = [expired, valid]
    refresh = MagicMock(return_value="new-token")
    client = XTSClient(token="old-token", session=session, refresh_token=refresh)

    assert client.ohlc(1, 2, "2026-10-26", "2026-10-27")
    refresh.assert_called_once_with()
    assert client.token == "new-token"
    assert session.get.call_count == 2


def test_ohlc_raises_when_invalid_token_cannot_be_refreshed():
    response = MagicMock(status_code=400, text='{"code":"e-session-0007"}')
    session = MagicMock()
    session.get.return_value = response
    client = XTSClient(token="old-token", session=session)

    try:
        client.ohlc(1, 2, "2026-10-26", "2026-10-27")
    except RuntimeError as exc:
        assert str(exc) == "XTS session token is invalid"
    else:
        raise AssertionError("expected invalid-token failure")


def test_expired_token_code_is_recognized_in_structured_response():
    response = MagicMock(status_code=401, text='{"type":"error","code":"e-session-0007"}')
    session = MagicMock()
    session.get.return_value = response
    client = XTSClient(token="old-token", session=session)

    try:
        client.ohlc(1, 2, "2026-10-26", "2026-10-27")
    except RuntimeError as exc:
        assert str(exc) == "XTS session token is invalid"
    else:
        raise AssertionError("expected invalid-token failure")


def test_missing_authorization_is_refreshed_once():
    expired = MagicMock(
        status_code=400,
        text='{"type":"error","description":"Token/Authorization not found"}',
        json=lambda: {"type": "error", "description": "Token/Authorization not found"},
    )
    valid = MagicMock(
        status_code=200,
        text='{"type":"success"}',
        json=lambda: {"type": "success", "result": {"dataResponse": "1|1|2|0|1|10|0"}},
    )
    session = MagicMock()
    session.get.side_effect = [expired, valid]
    refresh = MagicMock(return_value="new-token")
    client = XTSClient(token="old-token", session=session, refresh_token=refresh)

    assert client.ohlc(1, 2, "2026-10-26", "2026-10-27")
    refresh.assert_called_once_with()
    assert client.token == "new-token"
    assert session.get.call_count == 2


def test_request_retries_transient_connection_reset():
    valid = MagicMock(
        status_code=200,
        json=lambda: {"type": "success", "result": {"dataResponse": "1|1|2|0|1|10|0"}},
    )
    session = MagicMock()
    session.get.side_effect = [requests.ConnectionError("connection reset"), valid]
    client = XTSClient(token="token", session=session)

    assert client.ohlc(1, 2, "2026-10-26", "2026-10-27")
    assert session.get.call_count == 2


def test_ohlc_normalizes_xts_exchange_wall_clock_epoch_to_ist():
    response = MagicMock(
        status_code=200,
        json=lambda: {
            "type": "success",
            "result": {
                "dataResponse": (
                    "1793178959|1|2|0|1|10|0,"
                    "1793201399|1|2|0|1|10|0"
                ),
            },
        },
    )
    session = MagicMock()
    session.get.return_value = response
    client = XTSClient(token="token", session=session)

    bars = client.ohlc(1, 26000, "2026-10-28 09:15:00", "2026-10-28 15:30:00")

    assert [bar["time"] for bar in bars] == [
        "2026-10-28T09:15:59+05:30",
        "2026-10-28T15:29:59+05:30",
    ]
