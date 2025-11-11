from __future__ import annotations

import time

import pytest

from data_bridge.compute.vast import VastError, VastSession


class DummyClient:
    def __init__(self, states):
        self.states = list(states)
        self.calls = 0

    def get_instance(self, instance_id: int):  # noqa: D401 - mimic VastClient
        self.calls += 1
        if self.states:
            return self.states.pop(0)
        return {"state": "starting"}


def test_wait_for_ready_polls_until_running(monkeypatch):
    states = [{"state": "starting"}, {"state": "running"}]
    client = DummyClient(states)
    session = VastSession(client, instance_id=123)
    session.wait_for_ready(timeout=1, poll_interval=0)
    assert client.calls >= 2


def test_wait_for_ready_times_out(monkeypatch):
    states = [{"state": "starting"} for _ in range(3)]
    client = DummyClient(states)
    session = VastSession(client, instance_id=456)
    start = time.time()
    with pytest.raises(VastError):
        session.wait_for_ready(timeout=0.05, poll_interval=0)
    assert time.time() - start >= 0.05
