"""
Unit tests for src/wistia_client/client.py. No network: HTTP is simulated by
FakeSession, which replays a scripted list of responses.

Run:  pytest -q
"""
import json
import re
from pathlib import Path

import pytest

from wistia_client import client as c


class FakeResponse:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body
        self.text = json.dumps(body) if not isinstance(body, str) else body

    def json(self):
        if isinstance(self._body, str):
            raise ValueError("not json")
        return self._body


class FakeSession:
    """Replays responses in order; records every call's params."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        if not self.responses:
            raise AssertionError("more calls than scripted responses")
        status, body = self.responses.pop(0)
        return FakeResponse(status, body)


no_sleep = lambda s: None  # noqa: E731


# ------------------------------------------------------------------ get_with_retry

def test_get_returns_json_on_200():
    s = FakeSession([(200, {"name": "x"})])
    status, body = c.get_with_retry(s, "medias/abc.json", None, sleep=no_sleep)
    assert status == 200 and body == {"name": "x"}
    assert s.calls[0][0] == f"{c.BASE_URL}/medias/abc.json"


def test_get_retries_on_503_then_succeeds():
    s = FakeSession([(503, "busy"), (503, "busy"), (200, [1, 2])])
    log = c.RunLog("2026-09-27")
    _, body = c.get_with_retry(s, "stats/events.json", {"media_id": "m"}, log, sleep=no_sleep)
    assert body == [1, 2]
    assert len(s.calls) == 3
    assert log.calls[-1].attempts == 3 and log.calls[-1].records == 2


def test_get_retries_on_429():
    s = FakeSession([(429, "rate"), (200, {})])
    _, body = c.get_with_retry(s, "account.json", None, sleep=no_sleep)
    assert body == {} and len(s.calls) == 2


def test_get_gives_up_after_max_attempts():
    s = FakeSession([(503, "busy")] * c.MAX_ATTEMPTS)
    with pytest.raises(c.WistiaApiError) as e:
        c.get_with_retry(s, "x.json", None, sleep=no_sleep)
    assert e.value.status is None and len(s.calls) == c.MAX_ATTEMPTS


@pytest.mark.parametrize("status", [401, 403, 404])
def test_get_fails_fast_on_fatal_status(status):
    s = FakeSession([(status, "no")])
    with pytest.raises(c.WistiaApiError) as e:
        c.get_with_retry(s, "medias/bad.json", None, sleep=no_sleep)
    assert e.value.status == status and len(s.calls) == 1


# ------------------------------------------------------------------ paginate

def test_paginate_stops_on_short_page():
    page1 = [{"i": i} for i in range(100)]
    page2 = [{"i": i} for i in range(40)]
    s = FakeSession([(200, page1), (200, page2)])
    pages = list(c.paginate(s, "stats/visitors.json", None, sleep=no_sleep))
    assert [p for p, _ in pages] == [1, 2]
    assert sum(len(r) for _, r in pages) == 140
    assert s.calls[0][1]["page"] == 1 and s.calls[1][1]["page"] == 2
    assert s.calls[0][1]["per_page"] == 100


def test_paginate_stops_on_empty_page_after_full_page():
    page1 = [{"i": i} for i in range(100)]
    s = FakeSession([(200, page1), (200, [])])
    pages = list(c.paginate(s, "stats/events.json", {"media_id": "m"}, sleep=no_sleep))
    assert len(pages) == 1 and len(s.calls) == 2


def test_paginate_respects_max_pages():
    full = [{"i": 1}] * 100
    s = FakeSession([(200, full)] * 5)
    pages = list(c.paginate(s, "stats/events.json", None, max_pages=2, sleep=no_sleep))
    assert len(pages) == 2 and len(s.calls) == 2


def test_paginate_preserves_filters_on_every_page():
    full = [{"i": 1}] * 100
    s = FakeSession([(200, full), (200, [])])
    list(c.paginate(s, "stats/events.json", {"media_id": "m", "start_date": "2026-09-01"}, sleep=no_sleep))
    for _, params in s.calls:
        assert params["media_id"] == "m" and params["start_date"] == "2026-09-01"


# ------------------------------------------------------------------ dates and watermarks

def test_window_start_uses_fallback_without_watermark():
    assert c.window_start(None, "2024-06-10", 3) == "2024-06-10"


def test_window_start_applies_lookback():
    assert c.window_start("2026-09-26", "2024-06-10", 3) == "2026-09-23"


def test_window_start_never_before_fallback():
    assert c.window_start("2024-06-11", "2024-06-10", 3) == "2024-06-10"


def test_window_start_accepts_timestamps():
    assert c.window_start("2026-09-26T12:00:00Z", "2024-06-10T04:56:20+00:00", 1) == "2026-09-25"


def test_new_watermark_advances_and_never_regresses():
    recs = [{"received_at": "2026-09-25T10:00:00Z"}, {"received_at": "2026-09-26T09:00:00Z"}, {}]
    assert c.new_watermark(recs, "received_at", None) == "2026-09-26T09:00:00Z"
    assert c.new_watermark(recs, "received_at", "2026-09-27T00:00:00Z") == "2026-09-27T00:00:00Z"
    assert c.new_watermark([], "received_at", "x") == "x"


def test_load_watermarks_handles_missing_file():
    assert c.load_watermarks(None) == {"by_date": {}, "events": {}, "visitors": None}
    assert c.load_watermarks("  ") == {"by_date": {}, "events": {}, "visitors": None}


def test_load_watermarks_fills_missing_keys():
    wm = c.load_watermarks(json.dumps({"events": {"m": "2026-01-01T00:00:00Z"}}))
    assert wm["events"]["m"] == "2026-01-01T00:00:00Z" and wm["by_date"] == {} and wm["visitors"] is None


def test_events_retention_floor_is_two_years():
    assert c.events_retention_floor("2026-09-27") == "2024-09-27"


@pytest.mark.parametrize("name,expected", [
    ("Our YouTube launch", "YouTube"),
    ("facebook ad cut", "Facebook"),
    ("The Gap Method", "Unknown"),
    ("rivas_-_de_testimonial (1080p) (1)", "Unknown"),
    (None, "Unknown"),
])
def test_derive_channel(name, expected):
    assert c.derive_channel(name) == expected


# ------------------------------------------------------------------ run log

def test_run_log_serializes():
    log = c.RunLog("2026-09-27")
    log.add(c.CallRecord("x.json", {"page": 1}, 200, 1, 0.1, 3))
    log.counts["events.m"] = 3
    d = json.loads(log.to_json())
    assert d["run_date"] == "2026-09-27" and d["calls"][0]["records"] == 3 and d["counts"]["events.m"] == 3


# ------------------------------------------------------------------ notebook embeds the same client

def test_notebook_embeds_identical_client():
    root = Path(__file__).resolve().parents[1]
    module = (root / "src" / "wistia_client" / "client.py").read_text(encoding="utf-8")
    nb = (root / "notebooks" / "00_ingest_wistia.py").read_text(encoding="utf-8")
    m = re.search(r"# ==== BEGIN wistia_client.client ====\n(.*?)\n# ==== END wistia_client.client ====", nb, re.S)
    assert m, "client block markers not found in notebook"
    assert m.group(1).strip() == module.strip(), "notebook client block differs from src/wistia_client/client.py"
