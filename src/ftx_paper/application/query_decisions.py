"""Decision projection application use case."""

from ftx_paper.ports.repositories import EventQueryRepository


class QueryDecisions:
    def __init__(self, queries: EventQueryRepository) -> None:
        self.queries = queries

    def execute(self, *, session_date=None, limit=50, before_id=None, session_name=None):
        return self.queries.read_decisions(session_date, limit, before_id, session_name)
