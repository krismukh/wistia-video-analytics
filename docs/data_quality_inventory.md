# Data quality inventory

Findings from API exploration (September 26, 2026), the backfill (September 27), and the seven-day production window (September 30 to October 6). Each item records what was observed, how the pipeline handles it, and what remains open.

| # | Finding | Severity | Handling |
|---|---|---|---|
| 1 | Neither media name contains "YouTube" or "Facebook," so the program's channel rule yields `channel = "Unknown"` for both media. | Informational | Rule applied as specified in silver (`derive_channel`); value carried to `dim_media`. Confirmed with the program lead before implementation. |
| 2 | Two event records in the backfill shared an `event_key` with another record (1,224 records, 1,222 distinct keys). | Low | Silver deduplicates on `event_key`, keeping the record from the latest ingest. |
| 3 | For `The Gap Method`, the daily series from `by_date` sums to 915 loads and 320 plays against cumulative counters of 934 and 328 (about 2 percent short). `rivas_-_de_testimonial` reconciles to within 5 loads and 0 plays. | Low | Reported on the Data quality dashboard page and in the final report. Cause: `by_date` omits the earliest sparse days for this media; cumulative counters are treated as authoritative for totals, daily series for trends. |
| 4 | `stats/medias/{id}/by_date` returns only two rows when called without `start_date`. | Medium (would silently truncate history) | Ingestion always passes an explicit `start_date` (media creation date on backfill, watermark minus lookback thereafter). |
| 5 | Wistia retains events for two years; older sessions are not retrievable. Daily load and play counts are retained indefinitely. | Informational | Backfill start for events is floored at `run_date - 730 days`. Visitor-level facts therefore begin in 2024 for the older media. |
| 6 | `percent_viewed`, `play_rate`, and `engagement` are fractions (0 to 1) in the API. | Informational | Documented in silver; gold multiplies to a 0 to 100 `watched_percent` where a percentage is displayed. |
| 7 | On a quiet day the events window may return no records, and no new visitor keys are fetched, so no bronze partition exists for `events` or `visitors` on that date. | Medium (caused one failed debug run on September 29) | Silver treats a missing partition for these two feeds as "nothing new to merge" and keeps the existing tables. All other feeds remain required. |
| 8 | The `visitors` feed is account-wide (104,882 records) and includes visitors with no activity on the tracked media. | Informational | Pulled in full once at backfill; thereafter only visitor keys seen in the day's new events are fetched. `dim_visitor` includes only visitors with events on tracked media (1,154 as of October 6). |
| 9 | `events` includes personal fields: IP address, latitude and longitude, email, name, and organization (where Wistia identified the visitor). | Policy | Retained in silver only, inside the private storage account. `dim_visitor` carries IP and geography because the brief's model requires them; email, name, organization, and coordinates are excluded from gold. Dashboards show country, platform, and browser grain only. |
| 10 | Pipeline notebooks run under the Data Factory managed identity, not the Synapse workspace identity. | Operational | Both identities hold `Key Vault Secrets User`; the infra scripts assign both. |
| 11 | Exact per-visitor watch time is not published; only `percent_viewed` per event and `hours_watched` per media-day. | Informational | `fact_media_engagement.total_watch_time_seconds` is estimated as `percent_viewed x duration`, summed over the visitor-day's events, and labeled as an estimate. |

## Reconciliation checks that run on every gold build

Each check raises and fails the run on mismatch:

1. Sum of `play_count` in `fact_media_engagement` equals the count of distinct play events in silver.
2. Row count of `fact_media_daily` equals media-days in `media_stats_daily`.
3. Distinct `visitor_id` in `fact_media_engagement` equals rows in `dim_visitor`.
4. Every `media_id` in the facts exists in `dim_media`.

Results are written to `gold/control/gold_run_log/run_date=<d>.json`.
