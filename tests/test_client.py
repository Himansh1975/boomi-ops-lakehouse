import pytest

from boomi_ops.client import BoomiApiError, BoomiClient, between


class Scripted:
    """Transport that returns canned responses and records the calls."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, path, body, content_type):
        self.calls.append((method, path, body, content_type))
        return self.responses.pop(0)


def client(transport):
    return BoomiClient(transport, base_delay=0.01, max_requests_per_second=0, sleep=lambda s: None)


def test_pages_through_query_more_with_plain_text_token():
    t = Scripted([
        (200, {"result": [{"id": 1}, {"id": 2}], "queryToken": "tok-1"}),
        (200, {"result": [{"id": 3}], "queryToken": "tok-2"}),
        (200, {"result": [{"id": 4}]}),
    ])
    rows = client(t).query_all("ExecutionRecord", between("executionTime", "a", "b"))
    assert [r["id"] for r in rows] == [1, 2, 3, 4]
    assert t.calls[0][1] == "/ExecutionRecord/query"
    assert t.calls[1] == ("POST", "/ExecutionRecord/queryMore", "tok-1", "text/plain")
    assert t.calls[2][2] == "tok-2"  # always the newest token


def test_retries_503_then_succeeds():
    t = Scripted([(503, {"message": "busy"}), (504, {}), (200, {"result": [{"id": 1}]})])
    c = client(t)
    assert c.query_all("ExecutionRecord", {}) == [{"id": 1}]
    assert c.retries_made == 2


def test_gives_up_after_max_retries():
    t = Scripted([(503, {})] * 3)
    c = BoomiClient(t, max_retries=2, max_requests_per_second=0, sleep=lambda s: None)
    with pytest.raises(BoomiApiError) as e:
        c.query_all("ExecutionRecord", {})
    assert e.value.status == 503


def test_does_not_retry_403():
    t = Scripted([(403, {"message": "Access denied"})])
    with pytest.raises(BoomiApiError):
        client(t).query_all("ExecutionRecord", {})
    assert len(t.calls) == 1


def test_rate_cap_sleeps_between_calls():
    slept = []
    now = [0.0]
    t = Scripted([(200, {"result": []})] * 3)
    c = BoomiClient(t, max_requests_per_second=4, sleep=slept.append, clock=lambda: now[0])
    for _ in range(3):
        c.call("POST", "/X/query", {})
    assert slept == [0.25, 0.25]


def test_between_filter_shape():
    f = between("executionTime", "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z")
    assert f == {"QueryFilter": {"expression": {
        "operator": "BETWEEN", "property": "executionTime",
        "argument": ["2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"]}}}
