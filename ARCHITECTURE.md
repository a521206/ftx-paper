# Dependency Boundaries

The dependency direction is:

```text
domain -> nothing infrastructure-specific
application -> domain + ports
runtime -> application + ports
infrastructure -> domain + ports
api -> application
```

SQLite, SQL statements, filesystem database paths, and database row types are
confined to `ftx_paper.infrastructure.sqlite`. Application code exchanges
typed records and repository protocols from `ftx_paper.ports`.

Each API request, feed callback, and replay job obtains a fresh
`UnitOfWorkFactory()` product. A unit of work commits only when the application
explicitly calls `commit`; exceptions roll back and the connection is always
closed. Nested transactions are not supported. Network calls stay outside
write transactions.

Engine, portfolio, and broker mutations cannot be rolled back by SQLite. The
runtime persists a committed checkpoint before advancing in-memory state; a
commit failure stops processing and recovery reconstructs state from the last
checkpoint. Contract replacement and its `CONTRACT_CHANGED` event remain one
atomic operation. Status patching uses a database transaction, not only an
instance-local lock. Replay transitions are compare-and-set operations, so
terminal states cannot be overwritten by racing workers.

`core/`, `contracts/`, and `strategy/` are existing business packages and are
covered by the same infrastructure-import checks as `domain/`.
