import sqlite3

from ftx_paper.infrastructure.sqlite.market_bar_repository import SqliteMarketBarRepository


def test_market_bar_repository_contract_reads_dates():
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE market_bars (minute TEXT NOT NULL)")
    connection.executemany(
        "INSERT INTO market_bars VALUES (?)",
        (("2026-01-02T09:15:00+05:30",), ("2026-01-01T09:15:00+05:30",),
         ("2026-01-02T09:16:00+05:30",)),
    )
    assert SqliteMarketBarRepository(connection).read_dates() == (
        "2026-01-01", "2026-01-02",
    )
