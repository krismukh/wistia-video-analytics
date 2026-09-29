"""
Local execution of notebooks/01_silver.py against synthetic bronze JSON shaped
like the real Wistia responses, with a stub for mssparkutils and local paths.
Not part of CI (needs a JVM); run manually:  python tests/local_silver_smoke.py
"""
import json
import re
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace

from pyspark.sql import SparkSession

root = Path(__file__).resolve().parents[1]
work = Path(tempfile.mkdtemp(prefix="silver_"))
bronze = work / "bronze"
silver = work / "silver"
RUN = "2026-09-27"


def put(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj), encoding="utf-8")


def make_bronze(ingest_date, media, events, visitors, daily):
    for mid, name, created in media:
        base = bronze / "wistia"
        put(base / f"media/ingest_date={ingest_date}/media_id={mid}/page_0001.json",
            {"hashed_id": mid, "id": 1, "name": name, "description": "", "duration": 988.7,
             "created": created, "updated": "2026-06-29T22:50:14+00:00",
             "project": {"id": 9180604, "name": "Testimonials", "hashedId": None},
             "share_link": f"https://x.wistia.com/medias/{mid}", "status": "ready", "type": "Video",
             "archived": False, "tags": [], "assets": [], "thumbnail": {}, "progress": 1.0})
        put(base / f"media_stats/ingest_date={ingest_date}/media_id={mid}/page_0001.json",
            {"load_count": 117127, "play_count": 935, "play_rate": 0.0106, "hours_watched": 37.19,
             "engagement": 0.1448, "visitors": 86147})
        put(base / f"media_engagement/ingest_date={ingest_date}/media_id={mid}/page_0001.json",
            {"engagement": 0.1448, "engagement_data": [0.5, 0.4, 0.3], "rewatch_data": [0.1, 0.0, 0.0]})
        put(base / f"media_stats_daily/ingest_date={ingest_date}/media_id={mid}/page_0001.json", daily[mid])
        for i, page in enumerate(events[mid], 1):
            put(base / f"events/ingest_date={ingest_date}/media_id={mid}/page_{i:04d}.json", page)
    put(bronze / f"wistia/visitors/ingest_date={ingest_date}/page_0001.json", visitors)


def ev(key, mid, vk, ts, pct):
    return {"event_key": key, "media_id": mid, "visitor_key": vk, "received_at": ts, "percent_viewed": pct,
            "ip": "23.120.165.116", "country": "US", "region": "North Carolina", "city": "Dallas",
            "lat": 35.0, "lon": -80.0, "org": None, "email": None, "embed_url": "https://e", "media_url": "https://m",
            "media_name": "n", "conversion_type": None, "conversion_data": {}, "iframe_heatmap_url": "",
            "thumbnail": {}, "user_agent_details": {"browser": "Chrome", "platform": "Windows", "mobile": False}}


def vis(vk, last):
    return {"visitor_key": vk, "created_at": "2026-09-01T00:00:00.000Z", "last_active_at": last,
            "load_count": 1, "play_count": 2, "identifying_event_key": None, "last_event_key": "k",
            "visitor_identity": {"name": None, "email": None, "org": None},
            "user_agent_details": {"browser": "Chrome", "platform": "Windows", "mobile": False}}


# Day 1: backfill-like partition
make_bronze(
    "2026-09-26",
    [("8hunphufxp", "rivas_-_de_testimonial (1080p) (1)", "2024-06-10T04:56:20+00:00"),
     ("9k4tbcdfg0", "The Gap Method YouTube cut", "2025-01-04T00:35:10+00:00")],
    {"8hunphufxp": [[ev("e1", "8hunphufxp", "v1", "2026-09-25T12:24:05.000Z", 0.5),
                     ev("e2", "8hunphufxp", "v2", "2026-09-24T12:24:05.000Z", 0.2)]],
     "9k4tbcdfg0": [[ev("e3", "9k4tbcdfg0", "v3", "2026-07-26T10:27:31.000Z", 0.0)]]},
    [vis("v1", "2026-09-25T12:24:05.000Z"), vis("v2", "2026-09-24T12:24:05.000Z"), vis("v3", "2026-07-26T10:27:31.000Z")],
    {"8hunphufxp": [{"date": "2026-09-24", "load_count": 10, "play_count": 1, "hours_watched": 0.1},
                    {"date": "2026-09-25", "load_count": 12, "play_count": 0, "hours_watched": 0.0}],
     "9k4tbcdfg0": [{"date": "2026-09-25", "load_count": 0, "play_count": 0, "hours_watched": 0.0}]},
)
# Day 2: incremental partition overlapping day 1 (lookback), plus one new event and a restated daily row
make_bronze(
    RUN,
    [("8hunphufxp", "rivas_-_de_testimonial (1080p) (1)", "2024-06-10T04:56:20+00:00"),
     ("9k4tbcdfg0", "The Gap Method YouTube cut", "2025-01-04T00:35:10+00:00")],
    {"8hunphufxp": [[ev("e1", "8hunphufxp", "v1", "2026-09-25T12:24:05.000Z", 0.5),
                     ev("e4", "8hunphufxp", "v4", "2026-09-27T09:00:00.000Z", 0.9)]],
     "9k4tbcdfg0": [[]]},
    [vis("v4", "2026-09-27T09:00:00.000Z"), vis("v1", "2026-09-27T08:00:00.000Z")],
    {"8hunphufxp": [{"date": "2026-09-25", "load_count": 15, "play_count": 1, "hours_watched": 0.2},
                    {"date": "2026-09-26", "load_count": 8, "play_count": 0, "hours_watched": 0.0},
                    {"date": "2026-09-27", "load_count": 3, "play_count": 0, "hours_watched": 0.0}],
     "9k4tbcdfg0": [{"date": "2026-09-27", "load_count": 0, "play_count": 0, "hours_watched": 0.0}]},
)

spark = SparkSession.builder.master("local[2]").appName("silver-smoke").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


class _FS:
    def ls(self, p):
        p = Path(p)
        if not p.exists():
            raise FileNotFoundError(p)
        return list(p.iterdir())

    def rm(self, p, recurse=False):
        shutil.rmtree(p, ignore_errors=True)

    def mv(self, a, b, create_path=False):
        shutil.move(a, b)

    def put(self, p, content, overwrite=False):
        Path(p).parent.mkdir(parents=True, exist_ok=True)
        Path(p).write_text(content, encoding="utf-8")


class _NB:
    def exit(self, v):
        print("EXIT:", v)


mssparkutils = SimpleNamespace(fs=_FS(), notebook=_NB())

src = (root / "notebooks" / "01_silver.py").read_text(encoding="utf-8")
src = src.replace('BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"', f'BRONZE = "file://{bronze}"')
src = src.replace('SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net"', f'SILVER = "file://{silver}"')
src = re.sub(r'^run_date = ""', f'run_date = "{RUN}"', src, flags=re.M)
# local fs stub needs plain paths, not file:// URIs
src = src.replace("mssparkutils.fs.ls(path)", "mssparkutils.fs.ls(path.replace('file://',''))")
src = src.replace("mssparkutils.fs.rm(path, True)", "mssparkutils.fs.rm(path.replace('file://',''), True)")
src = src.replace("mssparkutils.fs.mv(tmp, path, True)", "mssparkutils.fs.mv(tmp.replace('file://',''), path.replace('file://',''), True)")
src = src.replace('exists(f"{BRONZE}/wistia/{endpoint}/ingest_date={run_date}")', 'exists(f"{BRONZE}/wistia/{endpoint}/ingest_date={run_date}".replace("file://",""))')

g = {"spark": spark, "mssparkutils": mssparkutils, "__name__": "__main__"}

def run(day, rebuild):
    s = re.sub(r'^run_date = ".*"', f'run_date = "{day}"', src, flags=re.M)
    s = s.replace("full_rebuild = False", f"full_rebuild = {rebuild}")
    exec(compile(s, "01_silver.py", "exec"), g)

print("=== day 1 (2026-09-26), fresh silver")
run("2026-09-26", False)
print("=== day 2 (2026-09-27), incremental merge")
run(RUN, False)
print("=== day 3 (2026-09-28), quiet day: media feeds only, no events or visitors partition")
QUIET = "2026-09-28"
for mid in ("8hunphufxp", "9k4tbcdfg0"):
    for ep in ("media", "media_stats", "media_engagement", "media_stats_daily"):
        srcdir = bronze / f"wistia/{ep}/ingest_date={RUN}/media_id={mid}"
        dst = bronze / f"wistia/{ep}/ingest_date={QUIET}/media_id={mid}"
        shutil.copytree(srcdir, dst)
run(QUIET, False)

ev_df = spark.read.parquet(f"file://{silver}/wistia/events")
dl_df = spark.read.parquet(f"file://{silver}/wistia/media_stats_daily")
vi_df = spark.read.parquet(f"file://{silver}/wistia/visitors")
me_df = spark.read.parquet(f"file://{silver}/wistia/media")
assert ev_df.count() == 4, ev_df.count()                                  # e1..e4, e1 deduplicated; quiet day adds none
assert dl_df.count() == 6, dl_df.count()                                  # m1: 24,25,26,27; m2: 25,27
row = dl_df.filter("media_id='8hunphufxp' and date='2026-09-25'").first()
assert row.load_count == 15, row                                          # restated value from the later ingest wins
assert vi_df.count() == 4
assert vi_df.filter("visitor_key='v1'").first().last_active_at.isoformat().startswith("2026-09-27")
ch = {r.media_id: r.channel for r in me_df.filter(f"ingest_date='{RUN}'").collect()}
assert ch == {"8hunphufxp": "Unknown", "9k4tbcdfg0": "YouTube"}, ch
print("\nALL SILVER ASSERTIONS PASSED")
print("silver tables:", sorted(p.name for p in (silver / "wistia").iterdir()))

# ---- gold
gold = work / "gold"
gsrc = (root / "notebooks" / "02_gold.py").read_text(encoding="utf-8")
gsrc = gsrc.replace('SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net/wistia"', f'SILVER = "file://{silver}/wistia"')
gsrc = gsrc.replace('GOLD = f"abfss://gold@{STORAGE}.dfs.core.windows.net/wistia"', f'GOLD = "file://{gold}/wistia"')
gsrc = gsrc.replace('f"abfss://gold@{STORAGE}.dfs.core.windows.net/control/gold_run_log/run_date={run_date}.json"',
                    f'"{gold}/control/gold_run_log/run_date=" + run_date + ".json"')
gsrc = re.sub(r'^run_date = ""', f'run_date = "{RUN}"', gsrc, flags=re.M)
exec(compile(gsrc, "02_gold.py", "exec"), g)
dv = spark.read.parquet(f"file://{gold}/wistia/dim_visitor")
fe = spark.read.parquet(f"file://{gold}/wistia/fact_media_engagement")
fd = spark.read.parquet(f"file://{gold}/wistia/fact_media_daily")
dm = spark.read.parquet(f"file://{gold}/wistia/dim_media")
assert dm.count() == 2
assert dv.count() == 4, dv.count()
assert "email" not in dv.columns and "name" not in dv.columns
assert fe.agg({"play_count": "sum"}).first()[0] == 4
assert fd.count() == 6
r = fd.filter("media_id='8hunphufxp' and date='2026-09-25'").first()
assert r.unique_visitors == 1 and abs(r.play_rate - 1/15) < 1e-9, r
print("ALL GOLD ASSERTIONS PASSED")
print("gold tables:", sorted(p.name for p in (gold / "wistia").iterdir()))
spark.stop()
shutil.rmtree(work, ignore_errors=True)
