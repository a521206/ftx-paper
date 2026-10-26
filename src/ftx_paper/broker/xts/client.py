"""HTTP client for Symphony / IIFL XTS Market Data REST API."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

from .exceptions import XTSApiError, XTSAuthenticationError, XTSTransportError
from .protocol import exchange_code_for_segment, normalize_segment
from .session import XTSMarketDataSession

IST = ZoneInfo("Asia/Kolkata")
logger = logging.getLogger(__name__)

def _format_xts_datetime(dt: datetime | date | str) -> str:
    """Format datetime as 'mmm dd yyyy HHMMSS' expected by XTS OHLC API."""
    if isinstance(dt, datetime):
        return dt.strftime("%b %d %Y %H%M%S")
    if isinstance(dt, date):
        return datetime.combine(dt, datetime.min.time(), tzinfo=IST).strftime("%b %d %Y %H%M%S")
    raw = str(dt).strip()
    # Try parsing common formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw[:19], fmt).replace(tzinfo=IST)
            return parsed.strftime("%b %d %Y %H%M%S")
        except ValueError:
            pass
    return raw


def _parse_xts_date(val: str) -> date | None:
    if not val or not val.strip():
        return None
    raw = val.strip()
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            pass
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%b %d %Y"):
        try:
            return datetime.strptime(raw[:11].strip(), fmt).date()
        except ValueError:
            pass
    return None


class XTSClient:
    """Client for XTS Market Data REST endpoints."""

    def __init__(
        self,
        token: str | None = None,
        user_id: str | None = None,
        *,
        root_url: str = "https://developers.symphonyfintech.in",
        market_data_path: str = "/apimarketdata",
        session: requests.Session | None = None,
        cache_dir: Path | None = None,
        timeout: float = 15.0,
        refresh_token: Callable[[], str] | None = None,
        token_session: XTSMarketDataSession | None = None,
    ) -> None:
        self.token = token
        self.user_id = user_id
        norm_root = root_url.rstrip("/")
        parsed_root = urlparse(norm_root)
        if parsed_root.scheme not in {"http", "https"} or not parsed_root.netloc:
            raise ValueError("XTS root_url must be an absolute HTTP(S) URL")
        path = market_data_path.strip()
        if not path.startswith("/"):
            raise ValueError("XTS market_data_path must start with '/'")
        self.root_url = (
            norm_root
            if norm_root.endswith(path) or norm_root.endswith(("/apimarketdata", "/apibinarymarketdata"))
            else f"{norm_root}/{path.lstrip('/')}"
        )
        self._session = session or requests.Session()
        self.cache_dir = cache_dir
        self.timeout = timeout
        self.session = token_session
        if self.session is None and token is not None and refresh_token is not None:
            self.session = XTSMarketDataSession(token, user_id or "", refresh=refresh_token)
        self._master_cache: dict[str, list[dict[str, Any]]] = {}
        self.cancel_event: Any = None

    def _headers(self) -> dict[str, str]:
        if self.session is not None:
            state = self.session.snapshot()
            self.token = state.token
            self.user_id = state.user_id
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = self.token
        return headers

    def login(
        self,
        app_key: str,
        secret_key: str,
        source: str = "WEBAPI",
    ) -> dict[str, Any]:
        """Authenticate with XTS Market Data API via appKey and secretKey."""
        url = f"{self.root_url}/auth/login"
        payload = {
            "appKey": app_key.strip(),
            "secretKey": secret_key.strip(),
            "source": source.strip(),
        }
        resp = self._session.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=self.timeout)
        try:
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise XTSTransportError("XTS login request failed") from exc
        data = resp.json()
        if str(data.get("type", "")).lower() != "success":
            desc = data.get("description") or data.get("message") or "XTS login failed"
            raise XTSAuthenticationError(f"XTS login error: {desc}")
        result = data.get("result", {})
        token = str(result.get("token", "")).strip()
        user_id = str(result.get("userID", "")).strip()
        if not token:
            raise XTSAuthenticationError("XTS login did not return a session token")
        self.token = token
        self.user_id = user_id
        return data

    @staticmethod
    def _is_auth_expired(response: requests.Response, data: Any) -> bool:
        text = response.text.lower()
        if "e-session-0007" in text:
            return True
        if isinstance(data, dict):
            description = " ".join(str(data.get(key) or "") for key in ("code", "description", "message", "error")).lower()
            return str(data.get("type", "")).lower() == "error" and (
                "invalid token" in description or "e-session-0007" in description
            )
        return False

    def _request(self, method: str, url: str, *, retry: bool = True, **kwargs: Any) -> requests.Response:
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise XTSTransportError("XTS request cancelled")
        state = self.session.snapshot() if self.session is not None else None
        headers = kwargs.pop("headers", self._headers())
        timeout = kwargs.pop("timeout", self.timeout)
        try:
            request = getattr(self._session, method.lower())
            response = request(url, headers=headers, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            raise XTSTransportError(f"XTS {method} request failed") from exc
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise XTSTransportError("XTS request cancelled")
        try:
            data = response.json()
        except ValueError:
            data = None
        if self._is_auth_expired(response, data):
            if not retry or self.session is None or state is None:
                raise XTSAuthenticationError("XTS session token is invalid")
            self.session.refresh_if_stale(state.generation)
            return self._request(method, url, retry=False, headers=self._headers(), **kwargs)
        if response.status_code == 400 and "already subscribed" in response.text.lower():
            return response
        if response.status_code >= 400:
            description = data.get("description") if isinstance(data, dict) else response.text
            raise XTSApiError(f"XTS API error ({response.status_code}): {description}")
        if isinstance(data, dict) and str(data.get("type", "")).lower() == "error":
            raise XTSApiError(str(data.get("description") or data.get("message") or "XTS API error"))
        return response

    def search(self, search_string: str) -> list[dict[str, Any]]:
        """Search instruments using searchString."""
        url = f"{self.root_url}/instruments/search"
        resp = self._request("GET", url, params={"searchString": search_string})
        data = resp.json()
        result = data.get("result", [])
        return list(result) if isinstance(result, list) else []

    def subscription(
        self,
        instruments: list[dict[str, Any]],
        message_code: int = 1502,
    ) -> dict[str, Any]:
        """Send market data subscription request, chunked into batches of at most 50 instruments."""
        if not instruments:
            return {"type": "success", "result": []}

        url = f"{self.root_url}/instruments/subscription"
        last_resp: dict[str, Any] = {}
        batch_size = 50

        for i in range(0, len(instruments), batch_size):
            batch = instruments[i : i + batch_size]
            payload = {
                "instruments": batch,
                "xtsMessageCode": message_code,
            }
            resp = self._request("POST", url, json=payload)
            if resp.status_code == 400 and ("Already Subscribed" in resp.text or "already subscribed" in resp.text.lower()):
                last_resp = resp.json()
                continue
            resp.raise_for_status()
            last_resp = resp.json()

        return last_resp

    def quotes(
        self,
        instruments: list[dict[str, Any]],
        message_code: int = 1502,
    ) -> dict[str, Any]:
        """Request quotes snapshot for specified instruments."""
        url = f"{self.root_url}/instruments/quotes"
        payload = {
            "instruments": instruments,
            "xtsMessageCode": message_code,
            "publishFormat": "JSON",
        }
        resp = self._request("POST", url, json=payload)
        return resp.json()

    def ohlc(
        self,
        exchange_segment: int | str,
        instrument_id: int | str,
        start_time: datetime | date | str,
        end_time: datetime | date | str,
        compression: int = 60,
    ) -> list[dict[str, Any]]:
        """Fetch 1-minute historical OHLC data from XTS."""
        url = f"{self.root_url}/instruments/ohlc"
        seg = normalize_segment(exchange_segment)
        params = {
            "exchangeSegment": seg,
            "exchangeInstrumentID": int(instrument_id),
            "startTime": _format_xts_datetime(start_time),
            "endTime": _format_xts_datetime(end_time),
            "compressionValue": compression,
        }
        resp = self._request("GET", url, params=params)
        data = resp.json()
        if str(data.get("type", "")).lower() != "success":
            raise XTSApiError(str(data.get("description") or "XTS OHLC returned an invalid response"))

        result = data.get("result", {})
        raw_bars = result.get("dataReponse") or result.get("dataResponse") or ""
        if not raw_bars or not isinstance(raw_bars, str):
            return []

        # XTS OHLC format: "timestamp|open|high|low|close|volume|oi|...,timestamp|..."
        bars: list[dict[str, Any]] = []
        for chunk in raw_bars.strip().split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            parts = chunk.split("|")
            if len(parts) < 5:
                continue
            try:
                raw_ts = parts[0].strip()
                # Check if epoch seconds
                if raw_ts.isdigit():
                    sec = int(raw_ts)
                    dt = datetime.fromtimestamp(sec, tz=IST)
                else:
                    dt = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                
                open_p = float(parts[1])
                high_p = float(parts[2])
                low_p = float(parts[3])
                close_p = float(parts[4])
                volume = float(parts[5]) if len(parts) > 5 and parts[5] else 0.0
                oi = float(parts[6]) if len(parts) > 6 and parts[6] else 0.0

                bars.append({
                    "time": dt.isoformat(),
                    "timestamp": int(dt.timestamp()),
                    "open": open_p,
                    "high": high_p,
                    "low": low_p,
                    "close": close_p,
                    "volume": volume,
                    "oi": oi,
                })
            except Exception as exc:
                logger.debug("Failed parsing XTS OHLC bar %s: %s", chunk, exc)
                continue

        return bars

    def master(self, exchange_segment_list: list[str | int]) -> list[dict[str, Any]]:
        """Download or fetch master instrument list for specified segments."""
        url = f"{self.root_url}/instruments/master"
        # XTS accepts POST with JSON exchangeSegmentList
        norm_list = []
        for s in exchange_segment_list:
            if isinstance(s, int):
                # Convert back to segment name for API call
                norm_list.append(exchange_code_for_segment(s) + ("CM" if s in (1, 11) else ("FO" if s in (2, 12, 51) else "")))
            else:
                norm_list.append(str(s).upper())

        resp = self._request("POST", url, json={"exchangeSegmentList": norm_list}, timeout=45.0)
        data = resp.json()
        result = data.get("result", "")
        if isinstance(result, list):
            # Already JSON list
            return self._normalize_json_instruments(result)
        elif isinstance(result, str):
            # Pipe-separated content
            return self._parse_pipe_instruments(result)
        return []

    def instruments(self, exchange: str) -> list[dict[str, Any]]:
        """Fetch and cache instruments for exchange (e.g. 'NSE' or 'NFO' or 'NSECM' or 'NSEFO')."""
        norm_exch = exchange.upper().strip()
        mapped_seg = normalize_segment(norm_exch)
        cache_key = f"SEG_{mapped_seg}"
        if cache_key in self._master_cache:
            return self._master_cache[cache_key]

        # Check local cache
        today_str = datetime.now(IST).strftime("%Y%m%d")
        cache_file = (
            self.cache_dir / f"xts_{cache_key}_{today_str}.json"
            if self.cache_dir is not None
            else None
        )
        if cache_file and cache_file.exists():
            try:
                cached_data = json.loads(cache_file.read_text(encoding="utf-8"))
                if isinstance(cached_data, list):
                    # Rehydrate expiry date objects
                    for item in cached_data:
                        if item.get("expiry"):
                            item["expiry"] = _parse_xts_date(item["expiry"])
                    self._master_cache[cache_key] = cached_data
                    return cached_data
            except Exception as exc:
                logger.warning("Failed reading cached XTS instruments %s: %s", cache_file, exc)

        seg_name = "NSECM" if mapped_seg == 1 else ("NSEFO" if mapped_seg == 2 else exchange_code_for_segment(mapped_seg))
        items = self.master([seg_name])
        # Filter for this segment
        filtered = [item for item in items if item.get("segment_id") == mapped_seg]
        if not filtered and items:
            filtered = items
        self._master_cache[cache_key] = filtered

        if cache_file and filtered:
            try:
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                # Serialize dates as iso strings for disk cache
                serializable = []
                for it in filtered:
                    copy_it = dict(it)
                    if isinstance(copy_it.get("expiry"), (date, datetime)):
                        copy_it["expiry"] = copy_it["expiry"].isoformat()
                    serializable.append(copy_it)
                cache_file.write_text(json.dumps(serializable), encoding="utf-8")
            except Exception as exc:
                logger.warning("Failed saving XTS instruments cache: %s", exc)

        return filtered

    def _normalize_json_instruments(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for r in rows:
            seg = r.get("ExchangeSegment")
            token = r.get("ExchangeInstrumentID")
            tsym = r.get("DisplayName") or r.get("TradingSymbol") or r.get("Description") or r.get("Name")
            if token is None or not tsym:
                continue
            seg_int = int(seg) if seg is not None and str(seg).isdigit() else 1
            exch = exchange_code_for_segment(seg_int)
            name = r.get("Name") or r.get("UnderlyingIndexName") or tsym
            opt_type = str(r.get("OptionType", "")).upper()
            inst_type = "INDEX" if seg_int in (1, 11) else ("FUT" if "FUT" in str(r.get("InstrumentType", "")).upper() else (opt_type or "EQUITY"))
            
            strike_raw = r.get("StrikePrice")
            strike = float(strike_raw) if strike_raw is not None else None
            expiry = _parse_xts_date(str(r.get("ContractExpiration", "")))

            results.append({
                "instrument_token": int(token),
                "segment_id": seg_int,
                "exchange": exch,
                "symbol": tsym,
                "tradingsymbol": tsym,
                "name": name,
                "instrument_type": inst_type,
                "expiry": expiry,
                "strike": strike,
                "lot_size": int(r.get("LotSize", 1)) if r.get("LotSize") else 1,
                "tick_size": float(r.get("TickSize", 0.05)) if r.get("TickSize") else 0.05,
            })
        return results

    def _parse_pipe_instruments(self, text: str) -> list[dict[str, Any]]:
        """Parse pipe-separated XTS master response."""
        results: list[dict[str, Any]] = []
        lines = text.strip().splitlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 4:
                continue
            # Header check: "ExchangeSegment|ExchangeInstrumentID|..."
            if parts[0].strip().lower() == "exchangesegment":
                continue
            try:
                seg_int = normalize_segment(parts[0].strip())
            except ValueError:
                continue
            try:
                token = int(parts[1].strip())
            except ValueError:
                continue

            raw_inst_type = parts[5].strip().upper() if len(parts) > 5 else (parts[2].strip().upper() if len(parts) > 2 else "")
            name = parts[3].strip() if len(parts) > 3 else ""
            desc = parts[4].strip() if len(parts) > 4 else ""
            tsym = desc or (parts[19].strip() if len(parts) > 19 and parts[19].strip() else name)

            exch = exchange_code_for_segment(seg_int)

            if seg_int in (1, 11):
                inst_type = "INDEX"
            elif "FUT" in raw_inst_type or tsym.endswith("FUT"):
                inst_type = "FUT"
            elif "OPT" in raw_inst_type or tsym.endswith("CE") or tsym.endswith("PE"):
                inst_type = "CE" if tsym.endswith("CE") else ("PE" if tsym.endswith("PE") else "OPT")
            else:
                inst_type = "INDEX" if "INDEX" in raw_inst_type else "EQUITY"

            strike = None
            if len(parts) > 17 and parts[17].strip():
                try:
                    strike = float(parts[17].strip())
                except ValueError:
                    pass

            expiry = None
            if len(parts) > 16 and parts[16].strip():
                expiry = _parse_xts_date(parts[16].strip())

            lot_size = 1
            if len(parts) > 12 and parts[12].strip().isdigit():
                lot_size = int(parts[12].strip())

            tick_size = 0.05
            if len(parts) > 11 and parts[11].strip():
                try:
                    tick_size = float(parts[11].strip())
                except ValueError:
                    pass

            results.append({
                "instrument_token": token,
                "segment_id": seg_int,
                "exchange": exch,
                "symbol": tsym,
                "tradingsymbol": tsym,
                "name": name or tsym,
                "instrument_type": inst_type,
                "expiry": expiry,
                "strike": strike,
                "lot_size": lot_size,
                "tick_size": tick_size,
            })
        return results
