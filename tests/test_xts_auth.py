from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from ftx_paper.broker.xts.auth import XTSAuth
from ftx_paper.ports.records import AuthToken


def test_xts_auth_token_persistence(tmp_path: Path):
    db_path = tmp_path / "auth.sqlite3"
    auth = XTSAuth(
        token_db=db_path,
        app_key="test_app_key",
        secret_key="test_secret_key",
    )
    repo = auth.token_repository
    today = date.today()

    assert auth.token_repository.get_valid("xts", today) is None

    # Simulate saved token
    repo.save(AuthToken("xts", today, "jwt_token_abc_123"))

    assert auth.access_token() == "jwt_token_abc_123"


def test_xts_auth_from_config(tmp_path: Path):
    config_path = tmp_path / "xts.json"
    config_path.write_text(
        '{"app_key": "AK123", "secret_key": "SK456", "root_url": "https://ttblaze.iifl.com", "source": "WEBAPI"}',
        encoding="utf-8",
    )
    auth = XTSAuth.from_config_path(config_path)
    assert auth.app_key == "AK123"
    assert auth.secret_key == "SK456"
    assert auth.root_url == "https://ttblaze.iifl.com"
    assert auth.source == "WEBAPI"


def test_xts_auth_auto_login(tmp_path: Path):
    db_path = tmp_path / "auth.sqlite3"
    auth = XTSAuth(
        token_db=db_path,
        app_key="test_app_key",
        secret_key="test_secret_key",
    )

    with patch("ftx_paper.broker.xts.auth.XTSClient") as mock_client_cls:
        mock_instance = MagicMock()
        mock_instance.token = "generated_jwt_token"
        mock_instance.user_id = "USER123"
        mock_instance.login.return_value = {"type": "success", "result": {"token": "generated_jwt_token"}}
        mock_client_cls.return_value = mock_instance

        token = auth.access_token()
        assert token == "generated_jwt_token"
        mock_instance.login.assert_called_once_with("test_app_key", "test_secret_key", "WEBAPI")

        # Verify saved in sqlite repo
        today = date.today()
        saved = auth.token_repository.get_valid("xts", today)
        assert saved is not None
        assert saved.access_token == "generated_jwt_token"
