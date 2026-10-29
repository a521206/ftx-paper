from pathlib import Path


ROOT = Path(__file__).parents[1] / "src" / "ftx_paper"


def _imports(path: Path) -> str:
    return "\n".join(item.read_text(encoding="utf-8") for item in path.rglob("*.py"))


def test_domain_and_application_do_not_import_sqlite():
    for package in ("application", "domain", "contracts", "strategy", "market", "execution", "runtime"):
        text = _imports(ROOT / package)
        assert "import sqlite3" not in text
        assert "from sqlite3" not in text


def test_ports_do_not_import_infrastructure():
    assert "infrastructure" not in _imports(ROOT / "ports")


def test_sqlite_imports_are_confined_to_sqlite_infrastructure():
    for path in ROOT.rglob("*.py"):
        if "infrastructure" in path.parts and "sqlite" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        assert "import sqlite3" not in text
        assert "from sqlite3" not in text


def test_sql_statements_are_confined_to_sqlite_infrastructure():
    sql_markers = ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE TABLE")
    for package in ("api", "runtime", "application", "domain", "ports", "contracts", "strategy", "market", "execution"):
        text = _imports(ROOT / package)
        assert not any(marker in text for marker in sql_markers), package


def test_runtime_orchestration_does_not_import_infrastructure():
    for path in (ROOT / "runtime").glob("*.py"):
        if path.name == "store.py":  # historical import shim only
            continue
        assert "infrastructure" not in path.read_text(encoding="utf-8")
