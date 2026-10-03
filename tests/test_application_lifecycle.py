from ftx_paper.application import RecoverRuntime, StartRuntime, StopRuntime


class Status:
    def __init__(self, payload):
        self.payload = payload
    def patch(self, updates):
        self.payload.update(updates)
    def get(self):
        return self


class Uow:
    def __init__(self, state="RUNNING"):
        self.status = Status({"state": state})
        self.events = type("Events", (), {"append": lambda *_args: True})()
        self.commits = 0
    def __enter__(self): return self
    def __exit__(self, *_args): return None
    def commit(self): self.commits += 1
    def rollback(self): pass


def test_lifecycle_use_cases_commit_through_uow():
    uow = Uow()
    StartRuntime(lambda: uow).execute({"health_state": "STARTING"})
    StopRuntime(lambda: uow).execute()
    assert uow.commits == 2


def test_recovery_is_a_single_transaction():
    uow = Uow()
    assert RecoverRuntime(lambda: uow).execute()
    assert uow.status.payload["state"] == "STOPPED"
    assert uow.commits == 1
