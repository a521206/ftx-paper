import sqlite3

from ftx_paper.infrastructure.sqlite.unit_of_work import SqliteUnitOfWork


def test_sqlite_uow_rolls_back_when_commit_is_omitted():
    database = ":memory:"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE values_table (value INTEGER)")
    # An in-memory connection cannot be reopened after the UoW closes it;
    # rollback is asserted through the connection's trace before closure.
    rolled_back = []
    connection.set_trace_callback(rolled_back.append)
    with SqliteUnitOfWork(connection):
        connection.execute("INSERT INTO values_table VALUES (1)")
    assert any(statement == "ROLLBACK" for statement in rolled_back)


def test_sqlite_uow_commits_explicitly():
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE values_table (value INTEGER)")
    committed = []
    connection.set_trace_callback(committed.append)
    uow = SqliteUnitOfWork(connection)
    with uow:
        connection.execute("INSERT INTO values_table VALUES (1)")
        uow.commit()
    assert any(statement == "COMMIT" for statement in committed)
