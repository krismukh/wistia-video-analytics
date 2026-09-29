"""Generate notebooks/01_silver.ipynb (for Synapse import) and .py (for the repo and CI)."""
import json
from pathlib import Path

cells = []
def md(t): cells.append(("markdown", t))
def code(t): cells.append(("code", t))

md("""# 01_silver

PySpark transformation of the bronze JSON feeds into typed, deduplicated silver Parquet.

* Reads the bronze partition for `run_date` (all media, all endpoints).
* Types every column, derives `channel` from the media name (program rule), and explodes the engagement arrays.
* Merges into the existing silver tables on natural keys, keeping the most recently ingested record, so daily runs add only new data and reruns are idempotent.
* Generates `date_dim` covering the full history.
* Writes a reconciliation log to `silver/control/`.""")

code("""# Parameters cell
run_date = ""          # ingest_date partition to process; empty = today (UTC)
full_rebuild = False   # True: rebuild every silver table from all bronze partitions""")

code("""import json
from datetime import date

from pyspark.sql import Window
from pyspark.sql import functions as F

STORAGE = "stwistiakcm"
BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net"
spark.conf.set("spark.sql.session.timeZone", "UTC")
run_date = run_date or date.today().isoformat()
part = "*" if full_rebuild else run_date
counts = {}
print(f"run_date={run_date} full_rebuild={full_rebuild}")""")

md("## Helpers")
code("""def exists(path):
    try:
        mssparkutils.fs.ls(path)
        return True
    except Exception:
        return False


def read_bronze(endpoint, per_media=True, required=True):
    \"\"\"Read one endpoint's JSON pages for the run partition; adds ingest_date and media_id from the path.
    Per-media endpoints sit under media_id=<id>/; the account-wide visitors feed sits directly under ingest_date=.
    Optional feeds (events, visitors) may have no partition on a quiet day; with required=False that returns None.\"\"\"
    if not full_rebuild and not exists(f"{BRONZE}/wistia/{endpoint}/ingest_date={run_date}"):
        if required:
            raise FileNotFoundError(f"no bronze partition for {endpoint} on {run_date}")
        print(f"{endpoint}: no bronze partition on {run_date}; nothing new to merge")
        return None
    sub = "/*/*.json" if per_media else "/*.json"
    path = f"{BRONZE}/wistia/{endpoint}/ingest_date={part}{sub}"
    df = spark.read.option("multiLine", "true").json(path)
    f = F.input_file_name()
    return (df.withColumn("ingest_date", F.to_date(F.regexp_extract(f, r"ingest_date=(\\d{4}-\\d{2}-\\d{2})", 1)))
              .withColumn("path_media_id", F.regexp_extract(f, r"media_id=([^/]+)", 1)))


def upsert(df_new, table, keys, order_cols=("ingest_date",)):
    \"\"\"Merge into silver/<table>: union with the existing table, keep the latest record per key, swap in atomically.\"\"\"
    path = f"{SILVER}/wistia/{table}"
    tmp = f"{path}__tmp"
    df = df_new
    if exists(path) and not full_rebuild:
        df = spark.read.parquet(path).unionByName(df_new, allowMissingColumns=True)
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    df.write.mode("overwrite").parquet(tmp)
    if exists(path):
        mssparkutils.fs.rm(path, True)
    mssparkutils.fs.mv(tmp, path, True)
    n = spark.read.parquet(path).count()
    counts[table] = n
    print(f"{table}: {n} rows ({df_new.count()} new in this run)")
    return n


def existing_count(table):
    path = f"{SILVER}/wistia/{table}"
    return spark.read.parquet(path).count() if exists(path) else 0


def derive_channel(name_col):
    n = F.lower(F.coalesce(name_col, F.lit("")))
    return (F.when(n.contains("youtube"), "YouTube")
             .when(n.contains("facebook"), "Facebook")
             .otherwise("Unknown"))""")

md("## media (FR3): one row per media per ingest date; channel derived from the name")
code("""b = read_bronze("media")
media = b.select(
    F.col("hashed_id").alias("media_id"),
    F.col("id").cast("long").alias("wistia_numeric_id"),
    F.col("name").alias("title"),
    derive_channel(F.col("name")).alias("channel"),
    F.col("description"),
    F.col("duration").cast("double").alias("duration_seconds"),
    F.col("created").cast("timestamp").alias("created_at"),
    F.col("updated").cast("timestamp").alias("updated_at"),
    F.col("project.id").cast("long").alias("project_id"),
    F.col("project.name").alias("project_name"),
    F.col("share_link").alias("url"),
    F.col("status"), F.col("type"), F.col("archived").cast("boolean").alias("archived"),
    F.col("ingest_date"),
)
upsert(media, "media", ["media_id", "ingest_date"])
media.select("media_id", "title", "channel", "created_at", "duration_seconds").show(truncate=False)""")

md("## media_stats_snapshot (FR4): cumulative totals per media per ingest date")
code("""b = read_bronze("media_stats")
snap = b.select(
    F.col("path_media_id").alias("media_id"),
    F.col("load_count").cast("long"), F.col("play_count").cast("long"),
    F.col("play_rate").cast("double"), F.col("hours_watched").cast("double"),
    F.col("engagement").cast("double"), F.col("visitors").cast("long").alias("visitor_count"),
    F.col("ingest_date"),
)
upsert(snap, "media_stats_snapshot", ["media_id", "ingest_date"])""")

md("## media_stats_daily (FR4, FR7): one row per media per stats date; overlap from the lookback deduplicated")
code("""b = read_bronze("media_stats_daily")
daily = b.select(
    F.col("path_media_id").alias("media_id"),
    F.to_date("date").alias("date"),
    F.col("load_count").cast("long"), F.col("play_count").cast("long"),
    F.col("hours_watched").cast("double"),
    F.col("ingest_date"),
).filter("date is not null")
upsert(daily, "media_stats_daily", ["media_id", "date"])""")

md("## media_engagement (FR4): engagement and rewatch curves exploded by position bucket")
code("""b = read_bronze("media_engagement")
eng = (b.select(F.col("path_media_id").alias("media_id"), F.col("ingest_date"),
                F.col("engagement").cast("double").alias("overall_engagement"),
                F.posexplode_outer("engagement_data").alias("position", "engagement"))
         .join(b.select(F.col("path_media_id").alias("media_id"), F.col("ingest_date"),
                        F.posexplode_outer("rewatch_data").alias("position", "rewatch")),
               ["media_id", "ingest_date", "position"], "left")
         .withColumn("engagement", F.col("engagement").cast("double"))
         .withColumn("rewatch", F.col("rewatch").cast("double")))
upsert(eng, "media_engagement", ["media_id", "ingest_date", "position"])""")

md("## events (FR5, FR6, FR7): one row per event_key; personal fields retained in silver only")
code("""b = read_bronze("events", required=False)
if b is None:
    counts["events"] = existing_count("events")
    events = None
else:
  events = b.select(
    F.col("event_key"),
    F.col("media_id"),
    F.col("visitor_key"),
    F.col("received_at").cast("timestamp").alias("received_at"),
    F.col("percent_viewed").cast("double").alias("percent_viewed"),   # fraction 0..1
    F.col("ip").alias("ip_address"),
    F.col("country"), F.col("region"), F.col("city"),
    F.col("lat").cast("double").alias("latitude"), F.col("lon").cast("double").alias("longitude"),
    F.col("org").alias("organization"), F.col("email"),
    F.col("embed_url"), F.col("media_url"), F.col("media_name"),
    F.col("conversion_type"),
    F.col("user_agent_details.browser").alias("browser"),
    F.col("user_agent_details.platform").alias("platform"),
    F.col("user_agent_details.mobile").cast("boolean").alias("is_mobile"),
    F.col("ingest_date"),
  ).filter("event_key is not null")
  upsert(events, "events", ["event_key"])""")

md("## visitors (FR5): one row per visitor_key, latest activity retained")
code("""b = read_bronze("visitors", per_media=False, required=False)
if b is None:
    counts["visitors"] = existing_count("visitors")
    visitors = None
else:
  visitors = b.select(
    F.col("visitor_key"),
    F.col("created_at").cast("timestamp").alias("created_at"),
    F.col("last_active_at").cast("timestamp").alias("last_active_at"),
    F.col("load_count").cast("long"), F.col("play_count").cast("long"),
    F.col("identifying_event_key"), F.col("last_event_key"),
    F.col("visitor_identity.name").alias("name"),
    F.col("visitor_identity.email").alias("email"),
    F.col("visitor_identity.org").alias("organization"),
    F.col("user_agent_details.browser").alias("browser"),
    F.col("user_agent_details.platform").alias("platform"),
    F.col("ingest_date"),
  ).filter("visitor_key is not null")
  upsert(visitors, "visitors", ["visitor_key"], order_cols=("ingest_date", "last_active_at"))""")

md("## date_dim: generated calendar covering the full history and the production window")
code("""start, end = "2024-01-01", "2027-12-31"
date_dim = (spark.sql(f"select explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) as date")
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.quarter("date").alias("quarter"),
        F.year("date").alias("year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    ))
date_dim.write.mode("overwrite").parquet(f"{SILVER}/wistia/date_dim")
counts["date_dim"] = date_dim.count()
print("date_dim:", counts["date_dim"])""")

md("## Reconciliation: bronze record counts for this partition versus silver rows added")
code("""recon = {}
for ep, key in [("events", "event_key"), ("visitors", "visitor_key")]:
    bdf = read_bronze(ep, per_media=(ep != "visitors"), required=False)
    recon[ep] = {"bronze_records": bdf.count() if bdf is not None else 0,
                 "bronze_distinct_keys": bdf.select(key).distinct().count() if bdf is not None else 0,
                 "silver_rows": counts[ep]}
recon["media_stats_daily"] = {
    "bronze_records": read_bronze("media_stats_daily").count(),
    "silver_rows": counts["media_stats_daily"],
}
log = {"run_date": run_date, "full_rebuild": full_rebuild, "silver_counts": counts, "reconciliation": recon}
mssparkutils.fs.put(f"{SILVER}/control/silver_run_log/run_date={run_date}.json", json.dumps(log, indent=2), True)
print(json.dumps(log, indent=2))
mssparkutils.notebook.exit(json.dumps({"run_date": run_date, "counts": counts}))""")

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
Path("notebooks/01_silver.ipynb").write_text(json.dumps(nb, indent=1), encoding="utf-8")
py = ["# Synapse notebook 01_silver, exported as .py for the repository and CI.",
      "# Source of truth for import into Synapse Studio: 01_silver.ipynb (same cells)."]
for k, t in cells:
    if k == "markdown":
        py.append("\n# %% [markdown]\n" + "\n".join(("# " + l).rstrip() for l in t.splitlines()))
    else:
        py.append("\n# %%\n" + t)
Path("notebooks/01_silver.py").write_text("\n".join(py) + "\n", encoding="utf-8")
print("wrote notebooks/01_silver.ipynb and .py")
