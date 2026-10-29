from datetime import date
from pathlib import Path

from ftx_paper.broker.shoonya.auth import ShoonyaAuth, generate_totp
from ftx_paper.ports.records import AuthToken



def test_generate_totp():
    # RFC 6238 standard test vector with base32 secret 'JBSWY3DPEHPK3PXP'
    code = generate_totp("JBSWY3DPEHPK3PXP")
    assert len(code) == 6
    assert code.isdigit()


def test_shoonya_auth_token_persistence(tmp_path: Path):
    db_path = tmp_path / "auth.sqlite3"
    auth = ShoonyaAuth(
        token_db=db_path,
        uid="FA12345",
        pwd="secret_password",
        api_key="shoonya_key",
        vc="FA12345_U",
        totp_secret="JBSWY3DPEHPK3PXP",
    )
    repo = auth.token_repository
    today = date.today()

    assert auth.token_repository.get_valid("shoonya", today) is None

    # Simulate saved token
    repo.save(AuthToken("shoonya", today, "test_session_token_123"))

    assert auth.access_token() == "test_session_token_123"


def test_shoonya_auth_from_config(tmp_path: Path):
    config_path = tmp_path / "shoonya.json"
    config_path.write_text(
        '{"uid": "U123", "pwd": "P123", "api_key": "K123", "vc": "VC123", "totp_secret": "JBSWY3DPEHPK3PXP", "auth_db": "runtime/auth.sqlite3"}',
        encoding="utf-8",
    )

    auth = ShoonyaAuth.from_config_path(config_path)
    assert auth.uid == "U123"
    assert auth.pwd == "P123"
    assert auth.api_key == "K123"
    assert auth.vc == "VC123"
    assert auth.totp_secret == "JBSWY3DPEHPK3PXP"
