"""
wistia_client.client

Pure-Python helpers for the Wistia Stats API ingestion. No Azure or Spark
dependencies, so the module is unit-testable with mocked HTTP responses.
The Synapse notebook 00_ingest_wistia embeds an identical copy of this
module (see tests/test_client.py, which checks the two stay in sync).

Endpoints (api.wistia.com/v1):
  medias/{id}.json                          media metadata          full pull
  stats/medias/{id}.json                    cumulative stats        full pull (daily snapshot)
  stats/medias/{id}/by_date.json            daily stats             incremental, start_date/end_date
  stats/medias/{id}/engagement.json         engagement curve        full pull (daily snapshot)
  stats/events.json?media_id=               viewing sessions        paged, incremental, start_date/end_date
  stats/visitors.json                       visitors (account-wide) paged; backfill only
  stats/visitors/{visitor_key}.json         one visitor             daily, for new visitor keys
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Iterator

BASE_URL = "https://api.wistia.com/v1"
PER_PAGE = 100
RETRY_STATUSES = {429, 502, 503, 504}
MAX_ATTEMPTS = 5
BACKOFF_SECONDS = (1, 2, 4, 8, 16)
FATAL_STATUSES = {401, 403, 404}

log = logging.getLogger("wistia_client")


class WistiaApiError(RuntimeError):
    """Raised for non-retryable failures (401, 403, 404) or when retries are exhausted."""

    def __init__(self, status: int | None, url: str, body: str = ""):
        super().__init__(f"HTTP {status} for {url}: {body[:200]}")
        self.status = status
        self.url = url


@dataclass
class CallRecord:
    endpoint: str
    params: dict
    status: int | None
    attempts: int
    seconds: float
    records: int


@dataclass
class RunLog:
    run_date: str
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    calls: list[CallRecord] = field(default_factory=list)
    counts: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def add(self, rec: CallRecord) -> None:
        self.calls.append(rec)

    def to_json(self) -> str:
        return json.dumps(
            {
                "run_date": self.run_date,
                "started_at": self.started_at,
                "finished_at": datetime.utcnow().isoformat() + "Z",
                "calls": [c.__dict__ for c in self.calls],
                "counts": self.counts,
                "errors": self.errors,
            },
            indent=2,
        )


# --------------------------------------------------------------------------- HTTP

def get_with_retry(
    session,
    path: str,
    params: dict | None,
    run_log: RunLog | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[int, object]:
    """
    GET BASE_URL/path with exponential backoff on 429/5xx.
    Returns (status, parsed_json_or_text). Raises WistiaApiError on fatal
    statuses or when retries are exhausted. `session` must expose .get(url,
    params=..., timeout=...) returning an object with .status_code, .json(), .text.
    """
    url = f"{BASE_URL}/{path}"
    attempts = 0
    t0 = time.time()
    while attempts < MAX_ATTEMPTS:
        attempts += 1
        r = session.get(url, params=params, timeout=60)
        status = r.status_code
        if status in RETRY_STATUSES:
            wait = BACKOFF_SECONDS[min(attempts - 1, len(BACKOFF_SECONDS) - 1)]
            log.warning("HTTP %s for %s (attempt %s), retrying in %ss", status, path, attempts, wait)
            sleep(wait)
            continue
        if status in FATAL_STATUSES:
            if run_log:
                run_log.add(CallRecord(path, params or {}, status, attempts, round(time.time() - t0, 2), 0))
            raise WistiaApiError(status, url, r.text)
        try:
            body = r.json()
        except ValueError:
            body = r.text
        n = len(body) if isinstance(body, list) else (1 if isinstance(body, dict) else 0)
        if run_log:
            run_log.add(CallRecord(path, params or {}, status, attempts, round(time.time() - t0, 2), n))
        if status != 200:
            raise WistiaApiError(status, url, str(body))
        return status, body
    if run_log:
        run_log.add(CallRecord(path, params or {}, None, attempts, round(time.time() - t0, 2), 0))
    raise WistiaApiError(None, url, "retries exhausted")


def paginate(
    session,
    path: str,
    params: dict | None,
    run_log: RunLog | None = None,
    per_page: int = PER_PAGE,
    max_pages: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[tuple[int, list]]:
    """
    Yield (page_number, records) until a page shorter than per_page is
    returned, an empty page is returned, or max_pages is reached.
    """
    page = 0
    while True:
        page += 1
        if max_pages and page > max_pages:
            return
        p = dict(params or {}, page=page, per_page=per_page)
        _, body = get_with_retry(session, path, p, run_log, sleep)
        if not isinstance(body, list) or not body:
            return
        yield page, body
        if len(body) < per_page:
            return


# --------------------------------------------------------------------------- dates and watermarks

def parse_date(s: str) -> date:
    return date.fromisoformat(s[:10])


def window_start(watermark: str | None, fallback: str, lookback_days: int) -> str:
    """
    Start date for an incremental pull: watermark minus lookback, or the
    fallback (for example the media's created date) when there is no watermark.
    Never earlier than the fallback.
    """
    fb = parse_date(fallback)
    if not watermark:
        return fb.isoformat()
    start = parse_date(watermark) - timedelta(days=lookback_days)
    return max(start, fb).isoformat()


def new_watermark(records: Iterable[dict], key: str, current: str | None) -> str | None:
    """Advance a watermark to the maximum value of `key` seen, never backwards."""
    best = current
    for r in records:
        v = r.get(key)
        if isinstance(v, str) and (best is None or v > best):
            best = v
    return best


def load_watermarks(text: str | None) -> dict:
    """Parse the control file; an absent or empty file yields an empty structure."""
    if not text or not text.strip():
        return {"by_date": {}, "events": {}, "visitors": None}
    wm = json.loads(text)
    wm.setdefault("by_date", {})
    wm.setdefault("events", {})
    wm.setdefault("visitors", None)
    return wm


def derive_channel(name: str | None) -> str:
    """Program rule: media name containing YouTube or Facebook (case-insensitive); else Unknown."""
    if not name:
        return "Unknown"
    n = name.lower()
    if "youtube" in n:
        return "YouTube"
    if "facebook" in n:
        return "Facebook"
    return "Unknown"


def events_retention_floor(run_date: str, days: int = 730) -> str:
    """Wistia retains events for two years; a backfill cannot start before this date."""
    return (parse_date(run_date) - timedelta(days=days)).isoformat()
