"""Generate notebooks/00_ingest_wistia.ipynb (for Synapse import) and .py (for the repo and CI)."""
import json
from pathlib import Path

client_src = Path("src/wistia_client/client.py").read_text(encoding="utf-8")

cells = []

def md(text): cells.append(("markdown", text))
def code(text): cells.append(("code", text))

md("""# 00_ingest_wistia

Python ingestion of the Wistia Stats API into the bronze zone.

* Reads the API token from Key Vault through the workspace managed identity (never in code).
* Pulls six endpoints for each configured media with pagination, incremental watermarks, retries with backoff, and a run log.
* Writes raw JSON, one file per page, under `bronze/wistia/<endpoint>/ingest_date=<run_date>/media_id=<id>/`.
* Idempotent: the `ingest_date` partition for the run date is replaced on rerun; watermarks advance only after success.

Parameters are supplied by the ADF pipeline; defaults allow interactive runs.""")

code("""# Parameters cell (ADF overrides these; toggle "Parameters cell" on this cell in Synapse Studio)
run_date = ""                         # YYYY-MM-DD; empty = today (UTC)
backfill = False                      # True on the first run: full history and all visitors
media_ids = "8hunphufxp,9k4tbcdfg0"   # comma-separated
lookback_days = 3                     # overlap re-pulled each run; deduplicated in silver
max_pages = 0                         # 0 = no cap (cap is for testing only)""")

code("""import json
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
print(f"run_date={run_date} backfill={backfill} media={MEDIA_IDS} lookback_days={lookback_days}")""")

md("## Client (identical to src/wistia_client/client.py; tests/test_client.py checks the two match)")
code("# ==== BEGIN wistia_client.client ====\n" + client_src + "\n# ==== END wistia_client.client ====")

code("""# ---- secret and session
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
print("watermarks in:", json.dumps(watermarks))""")

md("## Per-media endpoints")
code("""media_meta = {}
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
    log.info("media %s: %s events from %s", mid, n, start)""")

md("## Visitors\n\nBackfill: page the account-wide list once. Daily: fetch only the visitor keys seen in today's new events, one call each.")
code("""p = bronze_path("visitors")
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
log.info("visitors written: %s", n)""")

md("## Commit: run log, then watermarks (watermarks advance only if everything above succeeded)")
code("""write_json(f"{CONTROL}/run_log/run_date={run_date}.json", json.loads(run_log.to_json()))
mssparkutils.fs.put(WATERMARKS_PATH, json.dumps(new_wm, indent=2), True)
print("watermarks out:", json.dumps(new_wm))
print("counts:", json.dumps(run_log.counts, indent=2))
if run_log.errors:
    print("non-fatal errors:", run_log.errors)
mssparkutils.notebook.exit(json.dumps({"run_date": run_date, "counts": run_log.counts, "errors": len(run_log.errors)}))""")

# ---- write .ipynb (Synapse import) and .py (repo / CI)
nb = {
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {"kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
                 "language_info": {"name": "python"}},
    "cells": [
        {"cell_type": k, "metadata": ({"tags": ["parameters"]} if (k == "code" and t.startswith("# Parameters cell")) else {}),
         "source": t, **({"outputs": [], "execution_count": None} if k == "code" else {})}
        for k, t in cells
    ],
}
Path("notebooks").mkdir(exist_ok=True)
Path("notebooks/00_ingest_wistia.ipynb").write_text(json.dumps(nb, indent=1), encoding="utf-8")

py = ["# Synapse notebook 00_ingest_wistia, exported as .py for the repository and CI.",
      "# Source of truth for import into Synapse Studio: 00_ingest_wistia.ipynb (same cells)."]
for k, t in cells:
    if k == "markdown":
        py.append("\n# %% [markdown]\n" + "\n".join(("# " + l).rstrip() for l in t.splitlines()))
    else:
        py.append("\n# %%\n" + t)
Path("notebooks/00_ingest_wistia.py").write_text("\n".join(py) + "\n", encoding="utf-8")
print("wrote notebooks/00_ingest_wistia.ipynb and .py")
