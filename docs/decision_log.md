# Decision log

| Date | Decision | Rationale | Approved by |
|---|---|---|---|
| 2026-09-15 | Azure rather than AWS. | Prior projects and the author's platform experience; Synapse provides Python, PySpark, and serverless SQL in one workspace. | Program (architecture proposal) |
| 2026-09-17 | Medallion layout in ADLS Gen2 (bronze raw JSON, silver Parquet, gold Parquet) with serverless SQL views as the warehouse interface. | Keeps raw API responses replayable, isolates typing and deduplication in silver, and serves the dimensional model to SQL clients without a dedicated pool. | SME |
| 2026-09-18 | PySpark on a Synapse Spark pool for transformation, retained after review. | The brief requires PySpark; the program lead confirmed Spark should stay even though the data volume would fit pandas. | Program lead |
| 2026-09-19 | Channel derived from the media name containing "YouTube" or "Facebook" (case-insensitive), otherwise "Unknown." | Program guidance; Wistia does not publish a channel attribute. Both media resolve to "Unknown." | Program lead |
| 2026-09-20 | API token held only in Key Vault; read at run time by managed identity; CI scans for secrets. | Requirement that the token never appear in code or the repository. | Author |
| 2026-09-26 | Incremental ingestion by per-endpoint watermarks with a three-day lookback; duplicates resolved in silver on natural keys. | Wistia restates recent days; overlap plus deduplication is simpler and safer than exact change detection. | Author |
| 2026-09-26 | `by_date` always called with an explicit `start_date`. | Unfiltered calls return only two rows (finding 4). | Author |
| 2026-09-27 | Visitors pulled in full only at backfill; daily runs fetch only visitor keys seen in new events. | The account-wide feed is 104,882 records and mostly irrelevant to the two tracked media. | Author |
| 2026-09-27 | Personal fields retained in silver only; `dim_visitor` carries IP and geography (required by the brief's model) but not email, name, organization, or coordinates; dashboards use country grain. | Minimize personal data in the serving layer. | Author |
| 2026-09-28 | Azure Data Factory as orchestrator, calling Synapse notebooks through a linked service with managed identity. | More widely used orchestrator than Synapse pipelines; demonstrates cross-service identity. | Author |
| 2026-09-29 | Silver treats a missing bronze partition for `events` or `visitors` as "nothing new" rather than an error. | A quiet day legitimately produces no new events (finding 7). | Author |
| 2026-09-29 | Data Factory identity granted `Key Vault Secrets User` in addition to the Synapse identity. | Notebooks started by ADF read Key Vault as the ADF identity (finding 10). | Author |
| 2026-09-29 | Production window set to September 30 through October 6, trigger at 03:00 Eastern. | First clean end-to-end pipeline run completed on September 29. | Author |
| 2026-10-06 | Dashboards delivered as a local Streamlit app over a synced copy of gold, not a hosted service. | Optional requirement; avoids standing cost and public exposure of visitor-level data. | Author |
