# Wistia Video Analytics

End-to-end data engineering pipeline on Azure that ingests video engagement data from the
Wistia Stats API for two media, transforms it with PySpark into a dimensional model, runs
unattended daily, and serves the results through serverless SQL views and a Streamlit dashboard.

Author: Krishna C. Mukherjee. Architecture approved by the program SME (see `docs/`).

## Architecture

```
Wistia Stats API  -->  00_ingest_wistia (Python)  -->  ADLS Gen2 bronze (raw JSON, one file per page)
                  -->  01_silver (PySpark)         -->  ADLS Gen2 silver (typed, deduplicated Parquet)
                  -->  02_gold (PySpark)           -->  ADLS Gen2 gold (dimensional model) + serverless SQL views
                  -->  Streamlit dashboards (local, reading synced gold Parquet)

Orchestration: Azure Data Factory pl_wistia_daily, trigger trg_daily_0300 (daily, 03:00 Eastern)
Secrets:       Azure Key Vault kv-wistia-kcm (Wistia token read by the Synapse managed identity)
CI:            GitHub Actions (ruff, pytest, notebook syntax, ADF JSON, secret scan)
```

The full design, the assumptions confirmed with the SME, and the scalability section are in
`docs/Wistia Video Analytics Architecture Proposal.docx`; the diagram is `docs/Wistia Pipeline Architecture.drawio`.

| Layer | Azure resource | Contents |
|---|---|---|
| Storage | `stwistiakcm` (ADLS Gen2, West US 3) | containers `bronze`, `silver`, `gold`, `synapse` |
| Secrets | `kv-wistia-kcm` | secret `wistia-api-token`; read by the Synapse identity (interactive) and the Data Factory identity (pipeline runs) |
| Processing | `syn-wistia-kcm`, Spark pool `sparkpool1` (Spark 3.4, Small, 3 nodes, auto-pause 15 min) | notebooks `00_ingest_wistia`, `01_silver`, `02_gold` |
| Warehouse | `syn-wistia-kcm` built-in serverless pool, database `wistia_gold` | views `gold.*` over the gold Parquet |
| Orchestration | `adf-wistia-kcm` | pipeline `pl_wistia_daily`, trigger `trg_daily_0300` |

## Repository layout

```
infra/            create_environment.sh, resume_environment.sh   Azure CLI scripts (idempotent)
src/wistia_client/client.py   pure-Python API client: retry with backoff, pagination, watermarks, channel rule
notebooks/        00_ingest_wistia, 01_silver, 02_gold as .ipynb (import into Synapse) and .py (lint, CI)
build_*.py        generate both notebook forms from one source; run after editing a notebook
tests/            test_client.py (unit tests, mocked HTTP); local_silver_smoke.py (manual, needs a JVM)
sql/              create_gold_views.sql   serverless SQL database and views
adf/              linked service, pipeline, and trigger definitions
dashboards/       Streamlit app and gold sync script
docs/             proposal, diagram, API exploration report, data quality inventory, production run log
scripts/          check_no_secrets.py
.github/workflows/ci.yml
```

## How the pipeline works

**Ingestion (FR2 to FR7).** `00_ingest_wistia` authenticates with a Bearer token from Key Vault
and pulls, for each media, `medias/{id}`, `stats/medias/{id}`, `stats/medias/{id}/by_date`,
`stats/medias/{id}/engagement`, and `stats/events?media_id=`, plus `stats/visitors`. Paged endpoints
are walked with `page` and `per_page=100` until a short page. Incremental pulls use per-endpoint
watermarks in `bronze/control/watermarks.json` with a three-day lookback; `by_date` is always
requested with an explicit `start_date`; events are bounded by Wistia's two-year retention. HTTP 429
and 5xx are retried with exponential backoff (1, 2, 4, 8, 16 s); 401, 403, and 404 fail fast. Every
call is recorded in `bronze/control/run_log/`. The run-date partition is replaced on rerun and the
watermarks advance only after the whole run succeeds. Visitors are pulled in full once at backfill
and thereafter only for visitor keys seen in the day's new events.

**Silver.** `01_silver` types every column, derives `channel` from the media name (contains
"YouTube" or "Facebook", case-insensitive; otherwise "Unknown", per program guidance), explodes the
engagement curves, and merges each feed into its silver table on natural keys, keeping the most
recently ingested record. Personal fields (IP, coordinates, email, name, organization) are retained
in silver only, inside the private storage account. A generated `date_dim` covers 2024 to 2027.

**Gold.** `02_gold` rebuilds `dim_media`, `dim_visitor`, `dim_date`, `fact_media_engagement`, and
`fact_media_daily`, plus `fact_media_cumulative` and `fact_engagement_curve` for reporting, and
fails the run if silver and gold disagree on play counts, media-days, or tracked visitors.

**Orchestration.** `pl_wistia_daily` runs the three notebooks in sequence on `sparkpool1` with one
retry each; `trg_daily_0300` passes the scheduled date as `run_date`. Run history in ADF and the
logs under `*/control/` are the evidence for the seven-day production run (FR8).

## Setup from scratch

1. `bash infra/create_environment.sh` in Azure Cloud Shell (prompts for the Synapse SQL password and the Wistia token; never stores either).
2. Add your workstation IP to the Synapse workspace firewall.
3. In Synapse Studio, import the three notebooks from `notebooks/*.ipynb`, attach to `sparkpool1`, mark the first code cell of each as the parameters cell, and publish.
4. Run `00_ingest_wistia` once with `backfill = True`, then `01_silver` and `02_gold`.
5. Run `sql/create_gold_views.sql` against the built-in serverless pool.
6. In ADF Studio, create `ls_synapse_wistia` (managed identity) and the pipeline and trigger from `adf/`; debug, publish, start the trigger.

## Development

```
pip install -r requirements-dev.txt
pytest -q                          # 26 unit tests
ruff check src tests notebooks
python build_notebook.py && python build_silver.py && python build_gold.py   # after editing a notebook
```

The notebook `.py` files are generated; edit the `build_*.py` sources. `tests/test_client.py` fails
if the client embedded in the ingestion notebook drifts from `src/wistia_client/client.py`.

## Data quality findings

Recorded in `docs/data_quality_inventory.md`:

- Neither media name contains "YouTube" or "Facebook"; `channel` is "Unknown" for both.
- Two event records in the backfill shared an `event_key` with another and were deduplicated (1,224 to 1,222).
- For `The Gap Method`, the daily series sums to 913 loads and 320 plays against cumulative counters of 932 and 328 (about 2 percent short); `rivas_-_de_testimonial` reconciles exactly (935 plays).
- Events older than two years are not retained by Wistia; daily load and play counts are.
- `percent_viewed`, `play_rate`, and `engagement` are fractions (0 to 1); gold multiplies to percentages where displayed.

## Security notes

The API token is in Key Vault only. `exploration/` (raw API output including visitor IPs and emails)
and `dashboards/data/` are ignored by Git, and CI fails if a token-like string is committed.
