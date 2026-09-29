-- =============================================================================
-- Wistia Video Analytics : serverless SQL views over the gold layer (FR10)
--
-- Run in Synapse Studio: Develop > + > SQL script, "Connect to" = Built-in,
-- run each batch (GO separates them). Authentication is Azure AD pass-through:
-- the signed-in user needs Storage Blob Data Reader or Contributor on
-- stwistiakcm, which the environment script granted.
--
-- Views read the Parquet files in place; no data is copied. fact_media_daily
-- and fact_media_engagement are partitioned by media_id, which Spark stores in
-- the folder name rather than in the files, so those views recover it with
-- filepath(1).
-- =============================================================================

IF DB_ID('wistia_gold') IS NULL
    CREATE DATABASE wistia_gold COLLATE Latin1_General_100_BIN2_UTF8;
GO

USE wistia_gold;
GO

IF SCHEMA_ID('gold') IS NULL
    EXEC('CREATE SCHEMA gold');
GO

CREATE OR ALTER VIEW gold.dim_media AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stwistiakcm.dfs.core.windows.net/gold/wistia/dim_media/*.parquet',
    FORMAT = 'PARQUET') AS r;
GO

CREATE OR ALTER VIEW gold.dim_date AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stwistiakcm.dfs.core.windows.net/gold/wistia/dim_date/*.parquet',
    FORMAT = 'PARQUET') AS r;
GO

CREATE OR ALTER VIEW gold.dim_visitor AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stwistiakcm.dfs.core.windows.net/gold/wistia/dim_visitor/*.parquet',
    FORMAT = 'PARQUET') AS r;
GO

CREATE OR ALTER VIEW gold.fact_media_daily AS
SELECT r.filepath(1) AS media_id, r.*
FROM OPENROWSET(
    BULK 'https://stwistiakcm.dfs.core.windows.net/gold/wistia/fact_media_daily/media_id=*/*.parquet',
    FORMAT = 'PARQUET') AS r;
GO

CREATE OR ALTER VIEW gold.fact_media_engagement AS
SELECT r.filepath(1) AS media_id, r.*
FROM OPENROWSET(
    BULK 'https://stwistiakcm.dfs.core.windows.net/gold/wistia/fact_media_engagement/media_id=*/*.parquet',
    FORMAT = 'PARQUET') AS r;
GO

CREATE OR ALTER VIEW gold.fact_media_cumulative AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stwistiakcm.dfs.core.windows.net/gold/wistia/fact_media_cumulative/*.parquet',
    FORMAT = 'PARQUET') AS r;
GO

CREATE OR ALTER VIEW gold.fact_engagement_curve AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stwistiakcm.dfs.core.windows.net/gold/wistia/fact_engagement_curve/*.parquet',
    FORMAT = 'PARQUET') AS r;
GO

-- ---------------------------------------------------------------- smoke test
SELECT 'dim_media' AS v, COUNT(*) AS n FROM gold.dim_media
UNION ALL SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL SELECT 'dim_visitor', COUNT(*) FROM gold.dim_visitor
UNION ALL SELECT 'fact_media_daily', COUNT(*) FROM gold.fact_media_daily
UNION ALL SELECT 'fact_media_engagement', COUNT(*) FROM gold.fact_media_engagement
UNION ALL SELECT 'fact_media_cumulative', COUNT(*) FROM gold.fact_media_cumulative
UNION ALL SELECT 'fact_engagement_curve', COUNT(*) FROM gold.fact_engagement_curve;
GO

-- Example analytical query: plays, play rate, and hours watched by media and month
SELECT m.title, m.channel, d.year, d.month,
       SUM(f.load_count) AS loads, SUM(f.play_count) AS plays,
       CAST(SUM(f.play_count) AS FLOAT) / NULLIF(SUM(f.load_count), 0) AS play_rate,
       SUM(f.hours_watched) AS hours_watched
FROM gold.fact_media_daily f
JOIN gold.dim_media m ON m.media_id = f.media_id
JOIN gold.dim_date d ON d.date_key = f.date_key
GROUP BY m.title, m.channel, d.year, d.month
ORDER BY m.title, d.year, d.month;
GO
