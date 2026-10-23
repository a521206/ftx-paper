"""Shoonya (Finvasia) REST client for market data, warmup history, and symbol discovery."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any
import zipfile
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.shoonya.com/NorenWClientAPI"
SYMBOLS_URL_MAP = {
    "NSE": "https://api.shoonya.com/NSE_symbols.txt.zip",
    "NFO": "https://api.shoonya.com/NFO_symbols.txt.zip",
    "BSE": "https://api.shoonya.com/BSE_symbols.txt.zip",
}


def _parse_shoonya_date(value: str) -> date | None:
    if not value or value.strip() == "":
        return None
    raw = value.strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    return None


class ShoonyaClient:
    """HTTP client for Shoonya REST API and symbol files."""

    def __init__(
        self,
        token: str | None = None,
        uid: str | None = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        session: requests.Session | None = None,
        cache_dir: Path | None = None,
        timeout: float = 15.0,
    ) -> None:
        self.token = token
        self.uid = uid
        self.base_url = base_url.rstrip("/")
        self._session = session or requests.Session()
        self.cache_dir = cache_dir
        self.timeout = timeout
        self._master_cache: dict[str, list[dict[str, Any]]] = {}

    def post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Send a form-encoded post request with jData and optional jKey."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        data_str = f"jData={json.dumps(payload)}"
        if self.token:
            data_str += f"&jKey={self.token}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = self._session.post(url, data=data_str, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        res_json = response.json()
        if not isinstance(res_json, dict):
            raise ValueError(f"Expected dict response from {endpoint}, got {type(res_json)}")
        return res_json

    def generate_access_token(
        self,
        auth_code: str,
        secret_code: str,
        client_id: str,
        uid: str,
    ) -> dict[str, Any]:
        """Exchange OAuth auth code for access token via GenAcsTok."""
        data_to_hash = (client_id + secret_code + auth_code).encode("utf-8")
        checksum = hashlib.sha256(data_to_hash).hexdigest()
        payload = {
            "code": auth_code,
            "checksum": checksum,
            "uid": uid,
        }
        res = self.post("GenAcsTok", payload)
        token = str(res.get("susertoken") or res.get("access_token", "")).strip()
        if not token:
            emsg = res.get("emsg") or res.get("stat") or "Failed to generate access token"
            raise RuntimeError(f"Shoonya GenAcsTok error: {emsg}")
        self.token = token
        self.uid = uid
        return res

    def quick_auth(
        self,
        uid: str,
        pwd_sha256: str,
        factor2: str,
        vc: str,
        appkey_sha256: str,
        imei: str = "ftx-paper-api",
    ) -> dict[str, Any]:
        """Authenticate via QuickAuth endpoint."""
        payload = {
            "apkversion": "js:1.0.0",
            "uid": uid,
            "pwd": pwd_sha256,
            "factor2": factor2,
            "vc": vc,
            "appkey": appkey_sha256,
            "imei": imei,
        }
        res = self.post("QuickAuth", payload)
        if str(res.get("stat", "")).lower() != "ok":
            msg = res.get("emsg") or res.get("stat") or "Authentication failed"
            raise RuntimeError(f"Shoonya login error: {msg}")
        self.token = str(res.get("susertoken", ""))
        self.uid = uid
        return res

    def search_scrip(self, exchange: str, search_text: str) -> list[dict[str, Any]]:
        """Search scrips matching exchange and search text."""
        payload = {"uid": self.uid or "", "exch": exchange, "stext": search_text}
        res = self.post("SearchScrip", payload)
        values = res.get("values", [])
        return list(values) if isinstance(values, list) else []

    def get_security_info(self, exchange: str, token: str | int) -> dict[str, Any]:
        """Fetch instrument details by token."""
        payload = {"uid": self.uid or "", "exch": exchange, "token": str(token)}
        return self.post("GetSecurityInfo", payload)

    def historical_data(
        self,
        exchange: str,
        token: str | int,
        start_time: datetime | date | str,
        end_time: datetime | date | str,
        interval: str = "1",
    ) -> list[dict[str, Any]]:
        """Fetch 1-minute historical OHLCV data using TPSeries.

        start_time and end_time can be datetime, date, or epoch seconds string.
        """
        def _to_epoch(dt: datetime | date | str) -> str:
            if isinstance(dt, datetime):
                return str(int(dt.timestamp()))
            if isinstance(dt, date):
                return str(int(datetime.combine(dt, datetime.min.time(), tzinfo=IST).timestamp()))
            return str(dt)

        st = _to_epoch(start_time)
        et = _to_epoch(end_time)
        payload = {
            "uid": self.uid or "",
            "exch": exchange,
            "token": str(token),
            "st": st,
            "et": et,
            "intrv": interval,
        }
        res = self.post("TPSeries", payload)
        stat = str(res.get("stat", "")).lower()
        if stat == "not_ok":
            logger.warning("TPSeries returned not_ok for %s:%s: %s", exchange, token, res.get("emsg"))
            return []
        # TPSeries returns a list or a dict containing items
        if isinstance(res, list):
            return res
        items = res.get("values") or res.get("data")
        if isinstance(items, list):
            return items
        return []

    def instruments(self, exchange: str) -> list[dict[str, Any]]:
        """Load and cache the symbol master records for an exchange."""
        exchange_norm = exchange.upper().strip()
        if exchange_norm in self._master_cache:
            return self._master_cache[exchange_norm]

        symbols_url = SYMBOLS_URL_MAP.get(exchange_norm)
        if not symbols_url:
            return []

        # Check local cache first if configured
        today_str = datetime.now(IST).strftime("%Y%m%d")
        cache_file = (
            self.cache_dir / f"{exchange_norm}_{today_str}.csv"
            if self.cache_dir is not None
            else None
        )

        rows: list[dict[str, Any]] = []
        if cache_file and cache_file.exists():
            try:
                rows = self._read_symbols_csv(cache_file.read_text(encoding="utf-8"), exchange_norm)
                self._master_cache[exchange_norm] = rows
                return rows
            except Exception as exc:
                logger.warning("Failed reading cached Shoonya symbols %s: %s", cache_file, exc)

        try:
            resp = self._session.get(symbols_url, timeout=30.0)
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                name = zf.namelist()[0]
                content = zf.read(name).decode("utf-8", errors="replace")
                if cache_file:
                    cache_file.parent.mkdir(parents=True, exist_ok=True)
                    cache_file.write_text(content, encoding="utf-8")
                rows = self._read_symbols_csv(content, exchange_norm)
                self._master_cache[exchange_norm] = rows
                return rows
        except Exception as exc:
            logger.warning("Unable to download Shoonya symbol file for %s: %s", exchange_norm, exc)
            return []

    def _read_symbols_csv(self, content: str, exchange: str) -> list[dict[str, Any]]:
        """Parse raw Shoonya master CSV into standard records."""
        reader = csv.DictReader(io.StringIO(content))
        results: list[dict[str, Any]] = []
        for raw in reader:
            token = raw.get("Token", "").strip()
            tsym = raw.get("TradingSymbol", "").strip()
            name = raw.get("Symbol", "").strip()
            inst = raw.get("Instrument", "").strip().upper()
            opt_type = raw.get("OptionType", "").strip().upper()
            strike_raw = raw.get("StrikePrice", "").strip()
            expiry_raw = raw.get("Expiry", "").strip()

            if not token or not tsym:
                continue

            inst_type = "INDEX"
            if "FUT" in inst:
                inst_type = "FUT"
            elif opt_type in {"CE", "PE"}:
                inst_type = opt_type
            elif inst in {"OPTIDX", "OPTSTK"}:
                inst_type = "CE" if tsym.endswith("CE") else ("PE" if tsym.endswith("PE") else "OPT")

            strike = None
            if strike_raw:
                try:
                    strike = float(strike_raw)
                except ValueError:
                    pass

            expiry = _parse_shoonya_date(expiry_raw)

            results.append({
                "instrument_token": int(token) if token.isdigit() else token,
                "exchange": exchange,
                "symbol": tsym,
                "tradingsymbol": tsym,
                "name": name or tsym,
                "instrument_type": inst_type,
                "expiry": expiry,
                "strike": strike,
                "lot_size": int(raw.get("LotSize", 1)) if raw.get("LotSize", "").isdigit() else 1,
                "tick_size": float(raw.get("TickSize", 0.05)) if raw.get("TickSize", "") else 0.05,
            })
        return results
