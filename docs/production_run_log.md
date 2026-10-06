# Production run log

Seven consecutive unattended daily runs of `pl_wistia_daily` (trigger `trg_daily_0300`, 03:00 Eastern), September 30 to October 6, 2026.

Pre-window verification: manual debug run of the full pipeline on 2026-09-29 (run 5ecbe7a3) succeeded: nb_ingest 3m55s, nb_silver 2m57s, nb_gold 2m54s.

| Day | run_date | ADF run id | Status | Duration | Notes |
|---|---|---|---|---|---|
| 1 | 2026-09-30 | 50d3a483 | Succeeded | 9m 49s (03:00:01 to 03:09:49 ET) | Triggered by trg_daily_0300 |
| 2 | 2026-10-01 | bdee9b29 | Succeeded | 8m 45s (03:00:01 to 03:08:45 ET) | Triggered by trg_daily_0300 |
| 3 | 2026-10-02 | 4f10be1b | Succeeded | 9m 15s (03:00:00 to 03:09:15 ET) | Triggered by trg_daily_0300 |
| 4 | 2026-10-03 | c6df1a79 | Succeeded | 9m 18s (03:00:01 to 03:09:18 ET) | Triggered by trg_daily_0300 |
| 5 | 2026-10-04 | 7ce834b4 | Succeeded | 9m 8s (03:00:00 to 03:09:08 ET) | Triggered by trg_daily_0300 |
| 6 | 2026-10-05 | abffb3ab | Succeeded | 9m 31s (03:00:00 to 03:09:31 ET) | Triggered by trg_daily_0300 |
| 7 | 2026-10-06 | afe660b8 | Succeeded | 9m 7s (03:00:00 to 03:09:06 ET) | Triggered by trg_daily_0300 |

## Result

All seven scheduled runs succeeded without manual intervention. Durations ranged from 8m 45s to 9m 31s, of which roughly 2 to 3 minutes per notebook is Spark session start-up on the auto-paused pool. Evidence: `docs/evidence/adf_pipeline_runs_7_days.png` (ADF Monitor, Pipeline runs, 30-day view).
