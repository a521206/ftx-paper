from datetime import date
import json

from ftx_paper.broker.zerodha.auth import ZerodhaAuth
from ftx_paper.infrastructure.sqlite.auth_token_repository import SqliteTokenRepository
from ftx_paper.ports.records import AuthToken


def test_sqlite_token_repository_round_trip(tmp_path):
    repository = SqliteTokenRepository(tmp_path / "auth.sqlite3")
    repository.save(AuthToken("zerodha", date(2026, 1, 1), "token"))
    assert repository.get_valid("zerodha", date(2026, 1, 1)).access_token == "token"
    assert repository.get_valid("zerodha", date(2026, 1, 2)) is None


def test_auth_uses_explicit_token_database(tmp_path):
    config_path = tmp_path / "zerodha.json"
    config_path.write_text(json.dumps({"api_key": "key", "api_secret": "secret"}), encoding="utf-8")

    auth = ZerodhaAuth.from_config_path(config_path, token_db=tmp_path / "runtime" / "auth.sqlite3")

    assert auth.token_db == tmp_path / "runtime" / "auth.sqlite3"
