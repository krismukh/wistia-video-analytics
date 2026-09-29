# Synapse notebook 02_gold, exported as .py for the repository and CI.
# Source of truth for import into Synapse Studio: 02_gold.ipynb (same cells).

# %% [markdown]
# # 02_gold
#
# PySpark build of the gold dimensional model from silver.
#
# * `dim_media`, `dim_visitor`, `dim_date`, `fact_media_engagement`, `fact_media_daily` as specified in the brief and the proposal.
# * Two supporting tables for reporting: `fact_media_cumulative` (daily snapshot of cumulative totals) and `fact_engagement_curve` (engagement by video position).
# * Gold is recomputed in full from silver on every run (tables are small), so reruns are idempotent.
# * Personal identity fields (email, name, organization) stay in silver. `dim_visitor` carries IP address and geography because the brief's model requires them, and covers only visitors with events on the tracked media.
# * Writes a reconciliation log to `gold/control/`.

# %%
# Parameters cell
run_date = ""   # for the run log; empty = today (UTC)

# %%
import json
from datetime import date

from pyspark.sql import Window
from pyspark.sql import functions as F

STORAGE = "stwistiakcm"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net/wistia"
GOLD = f"abfss://gold@{STORAGE}.dfs.core.windows.net/wistia"
spark.conf.set("spark.sql.session.timeZone", "UTC")
run_date = run_date or date.today().isoformat()
counts = {}


def read_silver(table):
    return spark.read.parquet(f"{SILVER}/{table}")


def write_gold(df, table, partition_by=None):
    w = df.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.parquet(f"{GOLD}/{table}")
    counts[table] = spark.read.parquet(f"{GOLD}/{table}").count()
    print(f"{table}: {counts[table]} rows")


print(f"run_date={run_date}")

# %% [markdown]
# ## Silver inputs

# %%
media = read_silver("media")
events = read_silver("events")
visitors = read_silver("visitors")
daily = read_silver("media_stats_daily")
snapshot = read_silver("media_stats_snapshot")
curve = read_silver("media_engagement")
date_dim = read_silver("date_dim")
tracked_media = [r.media_id for r in media.select("media_id").distinct().collect()]
print("tracked media:", tracked_media)

# %% [markdown]
# ## dim_media: latest metadata per media

# %%
w = Window.partitionBy("media_id").orderBy(F.col("ingest_date").desc())
dim_media = (media.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    .select("media_id", "wistia_numeric_id", "title", "url", "channel",
            "created_at", "updated_at", "duration_seconds", "project_id", "project_name", "status",
            F.col("ingest_date").alias("as_of_date")))
write_gold(dim_media, "dim_media")
dim_media.select("media_id", "title", "channel", "duration_seconds").show(truncate=False)

# %% [markdown]
# ## dim_date

# %%
write_gold(date_dim, "dim_date")

# %% [markdown]
# ## dim_visitor: visitors with events on the tracked media
#
# Geography and IP come from the visitor's most recent event (the visitor endpoint does not carry them); activity totals come from the visitor record where available.

# %%
ev = events.filter(F.col("media_id").isin(tracked_media) & F.col("visitor_key").isNotNull())
w_last = Window.partitionBy("visitor_key").orderBy(F.col("received_at").desc())
last_event = (ev.withColumn("_rn", F.row_number().over(w_last)).filter("_rn = 1")
    .select("visitor_key", "ip_address", "country", "region", "city", "browser", "platform", "is_mobile"))
event_agg = ev.groupBy("visitor_key").agg(
    F.min("received_at").alias("first_seen_at"),
    F.max("received_at").alias("last_seen_at"),
    F.count("*").alias("tracked_event_count"),
    F.countDistinct("media_id").alias("tracked_media_count"),
)
dim_visitor = (event_agg.join(last_event, "visitor_key", "left")
    .join(visitors.select("visitor_key", "created_at", "last_active_at", "load_count", "play_count"), "visitor_key", "left")
    .select(
        F.col("visitor_key").alias("visitor_id"),
        "ip_address", "country", "region", "city",
        F.coalesce("created_at", "first_seen_at").alias("first_seen_at"),
        F.coalesce("last_active_at", "last_seen_at").alias("last_active_at"),
        F.col("load_count").alias("total_loads"),
        F.col("play_count").alias("total_plays"),
        "tracked_event_count", "tracked_media_count",
        "browser", "platform", "is_mobile",
    ))
write_gold(dim_visitor, "dim_visitor")

# %% [markdown]
# ## fact_media_daily: one row per media per date, from Wistia's daily stats plus distinct visitors from events

# %%
ev_daily = (ev.withColumn("date", F.to_date("received_at"))
    .groupBy("media_id", "date")
    .agg(F.countDistinct("visitor_key").alias("unique_visitors"), F.count("*").alias("event_count")))
fact_media_daily = (daily.join(ev_daily, ["media_id", "date"], "left")
    .withColumn("play_rate", F.when(F.col("load_count") > 0, F.col("play_count") / F.col("load_count")))
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .select("media_id", "date_key", "date", "load_count", "play_count", "play_rate", "hours_watched",
            F.coalesce("unique_visitors", F.lit(0)).alias("unique_visitors"),
            F.coalesce("event_count", F.lit(0)).alias("event_count"))
    .fillna({"unique_visitors": 0, "event_count": 0}))
write_gold(fact_media_daily, "fact_media_daily", partition_by="media_id")

# %% [markdown]
# ## fact_media_engagement: one row per media, visitor, and date
#
# `watched_percent` is the mean `percent_viewed` (0 to 100); `total_watch_time_seconds` is estimated as `percent_viewed` times media duration, since exact watch time is published only at media-day grain.

# %%
dur = dim_media.select("media_id", "duration_seconds")
fact_media_engagement = (ev.join(dur, "media_id", "left")
    .withColumn("date", F.to_date("received_at"))
    .groupBy("media_id", F.col("visitor_key").alias("visitor_id"), "date")
    .agg(
        F.count("*").alias("play_count"),
        (F.avg("percent_viewed") * 100).alias("watched_percent"),
        (F.max("percent_viewed") * 100).alias("max_watched_percent"),
        F.sum(F.col("percent_viewed") * F.col("duration_seconds")).alias("total_watch_time_seconds"),
        F.min("received_at").alias("first_play_at"),
        F.max("received_at").alias("last_play_at"),
    )
    .join(fact_media_daily.select("media_id", "date", "play_rate"), ["media_id", "date"], "left")
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .select("media_id", "visitor_id", "date_key", "date", "play_count", "play_rate",
            "watched_percent", "max_watched_percent", "total_watch_time_seconds", "first_play_at", "last_play_at"))
write_gold(fact_media_engagement, "fact_media_engagement", partition_by="media_id")

# %% [markdown]
# ## Supporting tables for reporting

# %%
fact_media_cumulative = snapshot.select(
    "media_id", F.col("ingest_date").alias("snapshot_date"),
    "load_count", "play_count", "play_rate", "hours_watched", "engagement", "visitor_count")
write_gold(fact_media_cumulative, "fact_media_cumulative")

fact_engagement_curve = curve.select(
    "media_id", F.col("ingest_date").alias("snapshot_date"), "position", "engagement", "rewatch", "overall_engagement")
write_gold(fact_engagement_curve, "fact_engagement_curve")

# %% [markdown]
# ## Reconciliation: gold facts against silver

# %%
silver_events = ev.count()
gold_plays = fact_media_engagement.agg(F.sum("play_count")).first()[0] or 0
silver_daily_plays = daily.agg(F.sum("play_count")).first()[0] or 0
gold_daily_plays = fact_media_daily.agg(F.sum("play_count")).first()[0] or 0
silver_visitors_tracked = ev.select("visitor_key").distinct().count()
recon = {
    "events_silver_vs_gold_play_count": {"silver": silver_events, "gold": int(gold_plays), "match": silver_events == gold_plays},
    "daily_play_count_silver_vs_gold": {"silver": int(silver_daily_plays), "gold": int(gold_daily_plays), "match": silver_daily_plays == gold_daily_plays},
    "tracked_visitors_silver_vs_dim_visitor": {"silver": silver_visitors_tracked, "gold": counts["dim_visitor"], "match": silver_visitors_tracked == counts["dim_visitor"]},
    "media_days_silver_vs_gold": {"silver": daily.count(), "gold": counts["fact_media_daily"], "match": daily.count() == counts["fact_media_daily"]},
}
log = {"run_date": run_date, "gold_counts": counts, "reconciliation": recon}
mssparkutils.fs.put(f"abfss://gold@{STORAGE}.dfs.core.windows.net/control/gold_run_log/run_date={run_date}.json",
                    json.dumps(log, indent=2), True)
print(json.dumps(log, indent=2))
failed = [k for k, v in recon.items() if not v["match"]]
if failed:
    raise RuntimeError(f"gold reconciliation failed: {failed}")
mssparkutils.notebook.exit(json.dumps({"run_date": run_date, "counts": counts}))
