import json
from datetime import datetime, timezone

from ftx_paper.ports.records import StoredEvent


class InMemoryEventRepository:
    def __init__(self):
        self._events = []
        self._keys = set()

    def append(self, event_type, payload, idempotency_key=None):
        if idempotency_key is not None and idempotency_key in self._keys:
            return False
        if idempotency_key is not None:
            self._keys.add(idempotency_key)
        self._events.append(StoredEvent(len(self._events) + 1, event_type, json.loads(json.dumps(dict(payload))), datetime.now(timezone.utc)))
        return True

    def read_recent(self, limit=100):
        return tuple(reversed(self._events[-limit:]))
