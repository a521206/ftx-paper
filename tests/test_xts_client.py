from unittest.mock import MagicMock

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
