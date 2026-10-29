
from ftx_paper.broker.xts.protocol import (
    build_subscription_payload,
    decode_touchline_data,
    exchange_code_for_segment,
    extract_socket_io_data,
    normalize_segment,
)


def test_normalize_segment():
    assert normalize_segment("NSECM") == 1
    assert normalize_segment("NSE") == 1
    assert normalize_segment("NSEFO") == 2
    assert normalize_segment("NFO") == 2
    assert normalize_segment(1) == 1
    assert normalize_segment("1") == 1
    assert exchange_code_for_segment(1) == "NSE"
    assert exchange_code_for_segment(2) == "NFO"


def test_build_subscription_payload():
    instruments = [("NSECM", 26000), ("NSEFO", 35000)]
    payload = build_subscription_payload(instruments, message_code=1502)
    assert payload == {
        "instruments": [
            {"exchangeSegment": 1, "exchangeInstrumentID": 26000},
            {"exchangeSegment": 2, "exchangeInstrumentID": 35000},
        ],
        "xtsMessageCode": 1502,
    }


def test_extract_socket_io_data():
    raw_frame = '42["1502-json-full", "{\\"ExchangeSegment\\": 1, \\"ExchangeInstrumentID\\": 26000}"]'
    result = extract_socket_io_data(raw_frame)
    assert result is not None
    event, data = result
    assert event == "1502-json-full"
    assert data["ExchangeSegment"] == 1
    assert data["ExchangeInstrumentID"] == 26000


def test_decode_touchline_data():
    raw = {
        "ExchangeSegment": 1,
        "ExchangeInstrumentID": 26000,
        "Touchline": {
            "LastTradedPrice": 25120.50,
            "TotalTradedQuantity": 1500000,
            "Open": 25000.0,
            "High": 25150.0,
            "Low": 24980.0,
            "Close": 25050.0,
            "OpenInterest": 85000,
            "LastUpdateTime": 1728900000,
        },
    }
    quotes = decode_touchline_data(raw)
    assert len(quotes) == 1
    q = quotes[0]
    assert q["instrument_token"] == 26000
    assert q["exchange"] == "NSE"
    assert q["last_price"] == 25120.50
    assert q["volume_traded"] == 1500000.0
    assert q["open_price"] == 25000.0
    assert q["high_price"] == 25150.0
    assert q["low_price"] == 24980.0
    assert q["close_price"] == 25050.0
    assert q["oi"] == 85000.0
    assert q["timestamp"] == 1728900000000
