# Synapse notebook 00_ingest_wistia, exported as .py for the repository and CI.
# Source of truth for import into Synapse Studio: 00_ingest_wistia.ipynb (same cells).

# %% [markdown]
# # 00_ingest_wistia
#
# Python ingestion of the Wistia Stats API into the bronze zone.
#
# * Reads the API token from Key Vault through the workspace managed identity (never in code).
# * Pulls six endpoints for each configured media with pagination, incremental watermarks, retries with backoff, and a run log.
# * Writes raw JSON, one file per page, under `bronze/wistia/<endpoint>/ingest_date=<run_date>/media_id=<id>/`.
# * Idempotent: the `ingest_date` partition for the run date is replaced on rerun; watermarks advance only after success.
#
# Parameters are supplied by the ADF pipeline; defaults allow interactive runs.

# %%
# Parameters cell (ADF overrides these; toggle "Parameters cell" on this cell in Synapse Studio)
run_date = ""                         # YYYY-MM-DD; empty = today (UTC)
backfill = False                      # True on the first run: full history and all visitors
media_ids = "8hunphufxp,9k4tbcdfg0"   # comma-separated
lookback_days = 3                     # overlap re-pulled each run; deduplicated in silver
max_pages = 0                         # 0 = no cap (cap is for testing only)

# %%
import json
import logging

from datetime import date

import requests

# ---- environment
STORAGE = "stwistiakcm"
KEYVAULT = "kv-wistia-kcm"
TOKEN_SECRET = "wistia-api-token"
BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"
CONTROL = f"{BRONZE}/control"
WATERMARKS_PATH = f"{CONTROL}/watermarks.json"

run_date = run_date or date.today().isoformat()
MEDIA_IDS = [m.strip() for m in media_ids.split(",") if m.strip()]
MAX_PAGES = int(max_pages) or None
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
print(f"run_date={run_date} backfill={backfill} media={MEDIA_IDS} lookback_days={lookback_days}")

# %% [markdown]
# ## Client (identical to src/wistia_client/client.py; tests/test_client.py checks the two match)

# %%
# ==== BEGIN wistia_client.client ====
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

# ==== END wistia_client.client ====

# %%
# ---- secret and session
token = mssparkutils.credentials.getSecret(KEYVAULT, TOKEN_SECRET)
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})
del token

# ---- bronze helpers
def bronze_path(endpoint, media_id=None):
    p = f"{BRONZE}/wistia/{endpoint}/ingest_date={run_date}"
    return f"{p}/media_id={media_id}" if media_id else p

def reset_partition(path):
    try:
        mssparkutils.fs.rm(path, True)
    except Exception:
        pass  # did not exist

def write_json(path, obj):
    mssparkutils.fs.put(path, json.dumps(obj), True)

def read_text(path):
    try:
        return mssparkutils.fs.head(path, 10_000_000)
    except Exception:
        return None

run_log = RunLog(run_date)
watermarks = load_watermarks(read_text(WATERMARKS_PATH))
print("watermarks in:", json.dumps(watermarks))

# %% [markdown]
# ## Per-media endpoints

# %%
media_meta = {}
new_wm = json.loads(json.dumps(watermarks))   # deep copy; written only at the end
new_visitor_keys = set()

for mid in MEDIA_IDS:
    log.info("media %s", mid)

    # FR3 media metadata (full pull)
    p = bronze_path("media", mid)
    reset_partition(p)
    _, meta = get_with_retry(session, f"medias/{mid}.json", None, run_log)
    write_json(f"{p}/page_0001.json", meta)
    media_meta[mid] = meta
    created = (meta.get("created") or run_date)[:10]
    run_log.counts[f"media.{mid}"] = 1

    # FR4 cumulative stats (daily snapshot)
    p = bronze_path("media_stats", mid)
    reset_partition(p)
    _, st = get_with_retry(session, f"stats/medias/{mid}.json", None, run_log)
    write_json(f"{p}/page_0001.json", st)
    run_log.counts[f"media_stats.{mid}"] = 1

    # FR4 engagement curve (daily snapshot)
    p = bronze_path("media_engagement", mid)
    reset_partition(p)
    _, eng = get_with_retry(session, f"stats/medias/{mid}/engagement.json", None, run_log)
    write_json(f"{p}/page_0001.json", eng)
    run_log.counts[f"media_engagement.{mid}"] = 1

    # FR4/FR7 daily stats, incremental with lookback; by_date needs an explicit start_date
    p = bronze_path("media_stats_daily", mid)
    reset_partition(p)
    start = created if backfill else window_start(watermarks["by_date"].get(mid), created, lookback_days)
    _, rows = get_with_retry(session, f"stats/medias/{mid}/by_date.json",
                             {"start_date": start, "end_date": run_date}, run_log)
    rows = rows if isinstance(rows, list) else []
    write_json(f"{p}/page_0001.json", rows)
    run_log.counts[f"media_stats_daily.{mid}"] = len(rows)
    new_wm["by_date"][mid] = new_watermark(rows, "date", new_wm["by_date"].get(mid))

    # FR5/FR6/FR7 events, paged, incremental with lookback; bounded by the two-year retention
    p = bronze_path("events", mid)
    reset_partition(p)
    floor = events_retention_floor(run_date)
    start = max(created, floor) if backfill else window_start(watermarks["events"].get(mid), max(created, floor), lookback_days)
    n = 0
    for page, recs in paginate(session, "stats/events.json",
                               {"media_id": mid, "start_date": start, "end_date": run_date},
                               run_log, max_pages=MAX_PAGES):
        write_json(f"{p}/page_{page:04d}.json", recs)
        n += len(recs)
        new_visitor_keys.update(r.get("visitor_key") for r in recs if r.get("visitor_key"))
        new_wm["events"][mid] = new_watermark(recs, "received_at", new_wm["events"].get(mid))
    run_log.counts[f"events.{mid}"] = n
    log.info("media %s: %s events from %s", mid, n, start)

# %% [markdown]
# ## Visitors
#
# Backfill: page the account-wide list once. Daily: fetch only the visitor keys seen in today's new events, one call each.

# %%
p = bronze_path("visitors")
reset_partition(p)
n = 0
if backfill:
    for page, recs in paginate(session, "stats/visitors.json", None, run_log, max_pages=MAX_PAGES):
        write_json(f"{p}/page_{page:04d}.json", recs)
        n += len(recs)
        new_wm["visitors"] = new_watermark(recs, "last_active_at", new_wm["visitors"])
else:
    batch, page = [], 0
    for key in sorted(new_visitor_keys):
        try:
            _, v = get_with_retry(session, f"stats/visitors/{key}.json", None, run_log)
        except WistiaApiError as e:
            run_log.errors.append(f"visitor {key}: {e}")
            continue
        batch.append(v)
        n += 1
        if len(batch) == PER_PAGE:
            page += 1
            write_json(f"{p}/page_{page:04d}.json", batch)
            batch = []
    if batch:
        page += 1
        write_json(f"{p}/page_{page:04d}.json", batch)
    new_wm["visitors"] = new_watermark([], "last_active_at", new_wm["visitors"])
run_log.counts["visitors"] = n
log.info("visitors written: %s", n)

# %% [markdown]
# ## Commit: run log, then watermarks (watermarks advance only if everything above succeeded)

# %%
write_json(f"{CONTROL}/run_log/run_date={run_date}.json", json.loads(run_log.to_json()))
mssparkutils.fs.put(WATERMARKS_PATH, json.dumps(new_wm, indent=2), True)
print("watermarks out:", json.dumps(new_wm))
print("counts:", json.dumps(run_log.counts, indent=2))
if run_log.errors:
    print("non-fatal errors:", run_log.errors)
mssparkutils.notebook.exit(json.dumps({"run_date": run_date, "counts": run_log.counts, "errors": len(run_log.errors)}))
