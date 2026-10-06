# Wistia Stats API exploration

Run on September 26, 2026 with `explore_api.py` against `https://api.wistia.com/v1` for media `8hunphufxp` (rivas_-_de_testimonial) and `9k4tbcdfg0` (The Gap Method). Sample records are omitted from this copy because they contain visitor IP addresses and emails; the raw output lives in the git-ignored `exploration/` folder.

## Authentication and limits

- Bearer token in the `Authorization` header. The token is read from Key Vault at run time and is never written to disk or source.
- Rate limit 600 requests per minute; 429 is retried with exponential backoff.
- Responses are JSON. Lists page with `page` and `per_page` (maximum 100); a short or empty page ends the list.

## Endpoints used

| Endpoint | Shape | Paged | Filters accepted | Pull strategy | Bronze folder |
|---|---|---|---|---|---|
| `medias/{id}.json` | object | no | none | full, daily | `media` |
| `stats/medias/{id}.json` | object | no | none | full, daily snapshot | `media_stats` |
| `stats/medias/{id}/by_date.json` | list of day objects | no | `start_date`, `end_date` | incremental; `start_date` is mandatory in practice (two rows without it) | `media_stats_daily` |
| `stats/medias/{id}/engagement.json` | object with `engagement_data` and `rewatch_data` arrays | no | none | full, daily snapshot | `media_engagement` |
| `stats/events.json` | list of event objects | yes | `media_id`, `start_date`, `end_date`; newest first | incremental with three-day lookback; floored at two-year retention | `events` |
| `stats/visitors.json` | list of visitor objects | yes | none useful | backfill only (account-wide, 104,882) | `visitors` |
| `stats/visitors/{visitor_key}.json` | object | no | none | daily, for new visitor keys | `visitors` |

## Fields of interest

- Media: `hashed_id`, `id`, `name`, `description`, `duration` (seconds), `created`, `updated`, `project.id`, `project.name`, `status`, `type`, `archived`, `share_link`.
- Cumulative stats: `load_count`, `play_count`, `play_rate` (fraction), `hours_watched`, `engagement` (fraction), `visitors`.
- Daily stats: `date`, `load_count`, `play_count`, `hours_watched`.
- Engagement: `engagement` (fraction) and two arrays indexed by position bucket.
- Events: `event_key`, `media_id`, `visitor_key`, `received_at`, `percent_viewed` (fraction), `ip`, `country`, `region`, `city`, `lat`, `lon`, `org`, `email`, `embed_url`, `media_url`, `media_name`, `conversion_type`, `user_agent_details.{browser,platform,mobile}`.
- Visitors: `visitor_key`, `created_at`, `last_active_at`, `load_count`, `play_count`, `identifying_event_key`, `last_event_key`, `visitor_identity.{name,email,org}`, `user_agent_details.{browser,platform}`.

## Counts at exploration time

| Media | Created | Duration | Cumulative loads | Cumulative plays | Events (two-year window) |
|---|---|---|---|---|---|
| 8hunphufxp | 2024-06-10 | 16.5 min | 117,151 | 936 | 898 |
| 9k4tbcdfg0 | 2025-01-04 | 36.6 min | 932 | 328 | 326 |

## Observations that shaped the design

1. `by_date` without `start_date` is effectively useless; the ingestion passes it on every call.
2. Events are returned newest first, so a watermark on `received_at` plus a lookback window is sufficient for incremental loads.
3. Fractions, not percentages, throughout; conversion happens once, in gold.
4. The visitors feed is account-wide; filtering to tracked media is done by joining on event visitor keys.
5. Personal fields appear in events and visitors and are handled as described in `data_quality_inventory.md`.
