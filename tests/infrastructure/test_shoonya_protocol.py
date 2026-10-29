import json

from ftx_paper.broker.shoonya.protocol import (
    connection_message,
    decode_message,
    heartbeat_message,
    subscription_messages,
    unsubscription_messages,
)


def test_subscription_messages():
    instruments = [("NSE", 26000), ("NFO", 35000)]
    msg = subscription_messages(instruments)
    parsed = json.loads(msg)
    assert parsed == {"t": "t", "k": "NSE|26000#NFO|35000"}


def test_unsubscription_messages():
    instruments = [("NSE", 26000)]
    msg = unsubscription_messages(instruments)
    parsed = json.loads(msg)
    assert parsed == {"t": "u", "k": "NSE|26000"}


def test_connection_and_heartbeat_messages():
    conn = json.loads(connection_message("U123", "TOK456"))
    assert conn == {"t": "c", "uid": "U123", "actid": "U123", "susertoken": "TOK456", "source": "API"}

    hb = json.loads(heartbeat_message())
    assert hb == {"t": "h"}


def test_decode_message_touchline_feed():
    raw = json.dumps({
        "t": "tk",
        "e": "NSE",
        "tk": "26000",
        "lp": "24500.50",
        "v": "1000",
        "oi": "50000",
        "o": "24400.0",
        "h": "24550.0",
        "l": "24390.0",
        "c": "24450.0",
        "ft": "1728900000",
    })
    payloads = decode_message(raw)
    assert len(payloads) == 1
    p = payloads[0]
    assert p["instrument_token"] == 26000
    assert p["last_price"] == 24500.50
    assert p["volume_traded"] == 1000.0
    assert p["oi"] == 50000.0
    assert p["open_price"] == 24400.0
    assert p["high_price"] == 24550.0
    assert p["low_price"] == 24390.0
    assert p["close_price"] == 24450.0
    assert p["timestamp"] == 1728900000000


def test_decode_message_partial_feed():
    raw = json.dumps({
        "t": "tf",
        "e": "NSE",
        "tk": "26000",
        "lp": "24510.00",
    })
    payloads = decode_message(raw)
    assert len(payloads) == 1
    assert payloads[0]["instrument_token"] == 26000
    assert payloads[0]["last_price"] == 24510.00
