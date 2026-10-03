import sqlite3

from ftx_paper.infrastructure.sqlite.market_bar_repository import SqliteMarketBarRepository


def test_market_bar_repository_contract_reads_dates():
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE market_dates (date TEXT PRIMARY KEY)")
    connection.execute("INSERT INTO market_dates VALUES ('2026-01-01')")
    assert SqliteMarketBarRepository(connection).read_dates() == ("2026-01-01",)
