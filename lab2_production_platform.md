# Production-Grade Data & AI Platform

**⏱ ~130 min (14:00–17:00) · ▲ Advanced · ☰ 4 challenge blocks**

> This is the Markdown version of the guide, for reading on GitHub. For **Copy** buttons and progress tracking, download and open `lab2_production_platform.html` in your browser.

## Contents

- [Step 1: Setup & Environment (~10 min)](#step-1-setup--environment-10-min)
- [Step 2: Resilient Ingestion (~15 min)](#step-2-resilient-ingestion-15-min)
- [Step 3: CDC with Streams + Tasks (~15 min)](#step-3-cdc-with-streams--tasks-15-min)
- [Step 4: Declarative Pipelines with Dynamic Tables (~20 min)](#step-4-declarative-pipelines-with-dynamic-tables-20-min)
- [Step 5: Dev Lifecycle: Clone, Swap, Undrop (Optional)](#step-5-dev-lifecycle-clone-swap-undrop-optional)
- [Step 6: Security & Roles (~25 min)](#step-6-security--roles-25-min)
- [Step 7: Semantic View via SQL (~10 min)](#step-7-semantic-view-via-sql-10-min)
- [Step 8: Cortex Search + Cortex Agent via SQL (~20 min)](#step-8-cortex-search--cortex-agent-via-sql-20-min)
- [Step 9: Consumption APIs (SQL API & Agents REST API) (~7 min · Instructor demo)](#step-9-consumption-apis-sql-api--agents-rest-api-7-min--instructor-demo)
- [Step 10: React App on Snowflake App Runtime (~8 min · Instructor demo)](#step-10-react-app-on-snowflake-app-runtime-8-min--instructor-demo)
- [Step 11: Cleanup (After the event)](#step-11-cleanup-after-the-event)

## Overview

In this lab you build an end-to-end, production-grade pharma market analytics platform on Snowflake: from resilient file ingestion to an AI agent consumed through REST APIs and a React app.

### Learning objectives

- Size warehouses, leverage caching and control cost with resource monitors and budgets
- Build resilient, idempotent ingestion with schema evolution and error handling
- Implement CDC with Streams + Tasks (task DAGs with finalizers)
- Build declarative pipelines with Dynamic Tables (incremental, SCD2, custom REFRESH USING)
- Use zero-copy cloning, SWAP and UNDROP for the dev lifecycle
- Design RBAC with database roles, functional roles, managed access and future grants; diagnose common errors
- Apply masking and row access policies (and Standard Edition alternatives)
- Create a Semantic View, Cortex Search service and Cortex Agent entirely in SQL
- Consume data and the agent via the SQL API and Cortex Agents REST API, and deploy a React app on Snowflake App Runtime

### Architecture

Source System → Internal Stage → RAW → Streams/Tasks & Dynamic Tables → SILVER → GOLD → Semantic View + Cortex Search → Cortex Agent → REST API → React app on Snowflake App Runtime

### Technologies

Internal Stages · COPY INTO · Schema Evolution · Streams · Tasks · Dynamic Tables · Zero-Copy Clone · RBAC · Database Roles · Masking Policies · Row Access Policies · Semantic Views · Cortex Search · Cortex AI Functions · Cortex Agents · SQL API · Agents REST API · PAT · Snowflake App Runtime · React

## Session plan (26/11, 14:00–17:00)

| Time | Block | Steps |
| --- | --- | --- |
| 14:00 | Opening: why Snowflake in Close-Up projects | — |
| 14:15 | The challenge: end-to-end architecture, checkpoints and account access | — |
| 14:30 | Block 1 — Data: resilient ingestion, CDC with Streams + Tasks, Dynamic Tables and SCD | 1–4 |
| 15:30 | Break | — |
| 15:40 | Block 2 — Security & roles | 6 |
| 16:05 | Block 3 — AI: semantic view and agent with a dedicated role | 7–8 |
| 16:35 | Block 4 — Integrations (instructor demo): SQL API, Agents REST API, React app on Snowflake App Runtime | 9–10 |
| 16:50 | Q&A and wrap-up | — |

> **Skipped in the session:**  step 1.5 (budgets), step 5 (clone/swap/undrop) and
> step 11 (cleanup). They stay in this guide so you can do them later at your own pace.

> **⚠ Synthetic data:**  All data in this lab (pharmacies, doctors, products, laboratories, sales, field notes) is synthetically generated for training purposes. It does not represent real Close-Up or customer data.

> **Note:**  This lab uses Enterprise features (masking, row access). Step 6 shows Standard Edition alternatives.

## Challenge Block 1 — Data (60 min · steps 1–4)

**Mission:** Build a production-grade pipeline: land the 7 source files with ingestion that survives bad rows and new columns, then propagate changes automatically with a Streams + Tasks DAG (CDC) and a Dynamic Table pipeline (incremental + SCD2).

> **Checkpoint ✓**  Two checkpoints: **(1)** after step 2, the *Verify* blocks of steps 1 and 2 return the expected counts and the rejected rows are captured, not lost; **(2)** after step 4, an insert in RAW shows up downstream without running anything manually and the step 4 *Verify* passes. When your team reaches the checkpoint, raise your hand (or post in the chat)
> — the first team gets the point for this block.

---

## Step 1: Setup & Environment (~10 min)

**What this step does:** creates the platform skeleton (one schema per layer, from source to apps) and the 7-table source system you will ingest from.

```text
SOURCE_SYSTEM (simulated source) ──► RAW (bronze: as landed) ──► SILVER (clean, deduplicated) ──► GOLD (business aggregates) ──► SEMANTIC (semantic view, search) ──► APPS (agent and apps)
```

### 1.1 Create the platform skeleton

Every production platform starts with clear boundaries: one dedicated warehouse (compute) and one database with a schema per layer of the medallion architecture. `SOURCE_SYSTEM` simulates Close-Up's operational source, `RAW` is the landing zone, `SILVER`/`GOLD` hold curated data, `SEMANTIC` holds the business layer for AI, and `APPS` holds agents and applications.

```sql
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS CLOSEUP_HOL2_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Close-Up HOL 2 - production platform warehouse';

CREATE DATABASE IF NOT EXISTS CLOSEUP_HOL2 COMMENT = 'Close-Up HOL 2 - Pharma Market Intelligence platform';

CREATE SCHEMA IF NOT EXISTS CLOSEUP_HOL2.SOURCE_SYSTEM COMMENT = 'Simulated operational source system';
CREATE SCHEMA IF NOT EXISTS CLOSEUP_HOL2.RAW           COMMENT = 'Landing zone (bronze)';
CREATE SCHEMA IF NOT EXISTS CLOSEUP_HOL2.SILVER        COMMENT = 'Cleaned, deduplicated data';
CREATE SCHEMA IF NOT EXISTS CLOSEUP_HOL2.GOLD          COMMENT = 'Business-ready aggregates';
CREATE SCHEMA IF NOT EXISTS CLOSEUP_HOL2.SEMANTIC      COMMENT = 'Semantic views, search services, AI enrichment';
CREATE SCHEMA IF NOT EXISTS CLOSEUP_HOL2.APPS          COMMENT = 'Agents and applications';

USE WAREHOUSE CLOSEUP_HOL2_WH;
USE DATABASE CLOSEUP_HOL2;
USE SCHEMA SOURCE_SYSTEM;
```

### 1.2 Generate the source system (7 tables)

We generate synthetic but realistic data with `GENERATOR`: IDs with `LPAD`, foreign keys with `UNIFORM` inside the parent PK range, and dates relative to `CURRENT_DATE()` so the data always looks "fresh". All laboratories and products are fictional.

```sql
CREATE OR REPLACE TABLE SOURCE_SYSTEM.LABORATORIES AS
SELECT
  'LAB' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4()), 3, '0') AS LAB_ID,
  ARRAY_CONSTRUCT('Andina Pharma','Rioplata Labs','Austral BioScience','Pampa Health','Cordillera Therapeutics',
                  'Atlantica Farma','Lumen Pharmaceuticals','Vitalis LATAM','Sur Genomics','Medicor Labs',
                  'Patagonia Pharma','Brasilis Saude','Oriental Biotech','Novaterra Pharma','Pacifico Labs')[ROW_NUMBER() OVER (ORDER BY SEQ4())-1]::VARCHAR AS LAB_NAME,
  ARRAY_CONSTRUCT('Argentina','Brazil','Uruguay','Chile','Switzerland','USA','Germany')[UNIFORM(0,6,RANDOM())]::VARCHAR AS HQ_COUNTRY,
  UNIFORM(0,9,RANDOM()) < 7 AS IS_CLOSEUP_CLIENT
FROM TABLE(GENERATOR(ROWCOUNT => 15));

CREATE OR REPLACE TABLE SOURCE_SYSTEM.PRODUCTS AS
WITH b AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n FROM TABLE(GENERATOR(ROWCOUNT => 80))
)
SELECT
  'PRD' || LPAD(n, 3, '0') AS PRODUCT_ID,
  'LAB' || LPAD(UNIFORM(1,15,RANDOM()), 3, '0') AS LAB_ID,
  ARRAY_CONSTRUCT('Cardio','Gluco','Onco','Pulmo','Neuro','Gastro','Derma','Algi')[MOD(n-1,8)]::VARCHAR
    || ARRAY_CONSTRUCT('vex','lin','tra','max','plus','zen','dol','fort','prex','tal')[MOD(n,10)]::VARCHAR AS BRAND_NAME,
  ARRAY_CONSTRUCT(
    ARRAY_CONSTRUCT('Atorvastatin','Losartan','Bisoprolol','Rivaroxaban'),
    ARRAY_CONSTRUCT('Metformin','Sitagliptin','Empagliflozin','Insulin Glargine'),
    ARRAY_CONSTRUCT('Imatinib','Capecitabine','Letrozole','Pembrolizumab'),
    ARRAY_CONSTRUCT('Salbutamol','Budesonide','Montelukast','Tiotropium'),
    ARRAY_CONSTRUCT('Sertraline','Escitalopram','Quetiapine','Levetiracetam'),
    ARRAY_CONSTRUCT('Omeprazole','Pantoprazole','Mesalazine','Domperidone'),
    ARRAY_CONSTRUCT('Isotretinoin','Clobetasol','Adapalene','Terbinafine'),
    ARRAY_CONSTRUCT('Ibuprofen','Paracetamol','Tramadol','Diclofenac')
  )[MOD(n-1,8)][UNIFORM(0,3,RANDOM())]::VARCHAR AS MOLECULE,
  ARRAY_CONSTRUCT('C10','A10','L01','R03','N06','A02','D10','M01')[MOD(n-1,8)]::VARCHAR AS ATC_CLASS,
  ARRAY_CONSTRUCT('Cardiology','Diabetes','Oncology','Respiratory','CNS','Gastroenterology','Dermatology','Analgesics')[MOD(n-1,8)]::VARCHAR AS THERAPEUTIC_AREA,
  ARRAY_CONSTRUCT('Tablets 10mg x30','Tablets 20mg x30','Capsules 40mg x28','Inhaler 200 doses','Cream 30g','Injectable 1ml x1','Tablets 500mg x20')[UNIFORM(0,6,RANDOM())]::VARCHAR AS PRESENTATION,
  ROUND(CASE WHEN MOD(n-1,8) = 2 THEN UNIFORM(150,900,RANDOM()) ELSE UNIFORM(4,60,RANDOM()) END + UNIFORM(0,99,RANDOM())/100, 2)::NUMBER(10,2) AS LIST_PRICE_USD,
  DATEADD(day, -UNIFORM(200, 5000, RANDOM()), CURRENT_DATE()) AS LAUNCH_DATE
FROM b;

CREATE OR REPLACE TABLE SOURCE_SYSTEM.PHARMACIES AS
WITH b AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n, UNIFORM(0,3,RANDOM()) AS c FROM TABLE(GENERATOR(ROWCOUNT => 400))
)
SELECT
  'PH' || LPAD(n, 4, '0') AS PHARMACY_ID,
  CASE WHEN ch = 'Hospital' THEN 'Hospital ' ELSE 'Farmacia ' END
    || ARRAY_CONSTRUCT('Central','del Sol','San Martin','Belgrano','Libertad','Santa Rosa','Las Flores','Del Parque','Norte','Sur')[UNIFORM(0,9,RANDOM())]::VARCHAR
    || ' ' || n AS PHARMACY_NAME,
  CASE WHEN ch = 'Retail Chain' THEN ARRAY_CONSTRUCT('FarmaPlus','Drogasil Sim','SaludYa','Cruz Verde Sim','Farmashop Sim')[UNIFORM(0,4,RANDOM())]::VARCHAR ELSE 'None' END AS CHAIN,
  ARRAY_CONSTRUCT(
    ARRAY_CONSTRUCT('Buenos Aires','Cordoba','Rosario','Mendoza'),
    ARRAY_CONSTRUCT('Sao Paulo','Rio de Janeiro','Belo Horizonte','Curitiba'),
    ARRAY_CONSTRUCT('Montevideo','Salto','Punta del Este','Paysandu'),
    ARRAY_CONSTRUCT('Santiago','Valparaiso','Concepcion','Antofagasta'))[c][MOD(n,4)]::VARCHAR AS CITY,
  ARRAY_CONSTRUCT(
    ARRAY_CONSTRUCT('AMBA','Centro','Litoral','Cuyo'),
    ARRAY_CONSTRUCT('Sudeste','Sudeste','Sudeste','Sul'),
    ARRAY_CONSTRUCT('Capital','Norte','Este','Litoral'),
    ARRAY_CONSTRUCT('Metropolitana','Valparaiso','Biobio','Norte'))[c][MOD(n,4)]::VARCHAR AS REGION,
  ARRAY_CONSTRUCT('Argentina','Brazil','Uruguay','Chile')[c]::VARCHAR AS COUNTRY,
  ch AS CHANNEL
FROM (SELECT b.*, CASE WHEN UNIFORM(0,9,RANDOM()) < 5 THEN 'Retail Chain'
                       WHEN UNIFORM(0,9,RANDOM()) < 8 THEN 'Independent' ELSE 'Hospital' END AS ch FROM b);

CREATE OR REPLACE TABLE SOURCE_SYSTEM.DOCTORS AS
WITH b AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n, UNIFORM(0,3,RANDOM()) AS c FROM TABLE(GENERATOR(ROWCOUNT => 600))
)
SELECT
  'DR' || LPAD(n, 4, '0') AS DOCTOR_ID,
  'Dr. ' || ARRAY_CONSTRUCT('Lucia','Martin','Sofia','Joao','Camila','Diego','Valentina','Pedro','Ana','Mateo','Julia','Tomas')[UNIFORM(0,11,RANDOM())]::VARCHAR
    || ' ' || ARRAY_CONSTRUCT('Gonzalez','Silva','Rodriguez','Santos','Fernandez','Oliveira','Lopez','Pereira','Martinez','Rojas','Diaz','Costa')[UNIFORM(0,11,RANDOM())]::VARCHAR AS DOCTOR_NAME,
  ARRAY_CONSTRUCT('Cardiologist','Endocrinologist','Oncologist','Pulmonologist','Psychiatrist','Gastroenterologist','Dermatologist','General Practitioner')[UNIFORM(0,7,RANDOM())]::VARCHAR AS SPECIALTY,
  ARRAY_CONSTRUCT(
    ARRAY_CONSTRUCT('Buenos Aires','Cordoba','Rosario','Mendoza'),
    ARRAY_CONSTRUCT('Sao Paulo','Rio de Janeiro','Belo Horizonte','Curitiba'),
    ARRAY_CONSTRUCT('Montevideo','Salto','Punta del Este','Paysandu'),
    ARRAY_CONSTRUCT('Santiago','Valparaiso','Concepcion','Antofagasta'))[c][MOD(n,4)]::VARCHAR AS CITY,
  ARRAY_CONSTRUCT('Argentina','Brazil','Uruguay','Chile')[c]::VARCHAR AS COUNTRY
FROM b;

CREATE OR REPLACE TABLE SOURCE_SYSTEM.SELL_OUT AS
WITH s AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n,
    DATEADD(day, -UNIFORM(0, 729, RANDOM()), CURRENT_DATE()) AS SALE_DATE,
    'PH'  || LPAD(UNIFORM(1, 400, RANDOM()), 4, '0') AS PHARMACY_ID,
    'PRD' || LPAD(UNIFORM(1, 80, RANDOM()), 3, '0')  AS PRODUCT_ID,
    UNIFORM(1, 60, RANDOM()) AS UNITS
  FROM TABLE(GENERATOR(ROWCOUNT => 150000))
)
SELECT
  'S' || LPAD(s.n, 7, '0') AS SALE_ID,
  s.SALE_DATE, s.PHARMACY_ID, s.PRODUCT_ID, s.UNITS,
  ROUND(s.UNITS * p.LIST_PRICE_USD * UNIFORM(85, 100, RANDOM()) / 100, 2)::NUMBER(12,2) AS NET_AMOUNT_USD
FROM s JOIN SOURCE_SYSTEM.PRODUCTS p ON p.PRODUCT_ID = s.PRODUCT_ID;

CREATE OR REPLACE TABLE SOURCE_SYSTEM.PRESCRIPTIONS AS
SELECT
  'RX' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4()), 6, '0') AS RX_ID,
  DATEADD(day, -UNIFORM(0, 729, RANDOM()), CURRENT_DATE()) AS RX_DATE,
  'DR'  || LPAD(UNIFORM(1, 600, RANDOM()), 4, '0') AS DOCTOR_ID,
  'PRD' || LPAD(UNIFORM(1, 80, RANDOM()), 3, '0')  AS PRODUCT_ID,
  UNIFORM(1, 6, RANDOM()) AS QUANTITY
FROM TABLE(GENERATOR(ROWCOUNT => 60000));

CREATE OR REPLACE TABLE SOURCE_SYSTEM.FIELD_NOTES AS
WITH b AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n,
         UNIFORM(0,4,RANDOM()) AS t,
         'PRD' || LPAD(UNIFORM(1, 80, RANDOM()), 3, '0') AS PRODUCT_ID
  FROM TABLE(GENERATOR(ROWCOUNT => 300))
)
SELECT
  'FN' || LPAD(b.n, 4, '0') AS NOTE_ID,
  DATEADD(day, -UNIFORM(0, 365, RANDOM()), CURRENT_DATE()) AS NOTE_DATE,
  b.PRODUCT_ID,
  ARRAY_CONSTRUCT('Argentina','Brazil','Uruguay','Chile')[UNIFORM(0,3,RANDOM())]::VARCHAR AS COUNTRY,
  ARRAY_CONSTRUCT('Medical Rep','Key Account Manager','Market Analyst','Regional Manager')[UNIFORM(0,3,RANDOM())]::VARCHAR AS AUTHOR_ROLE,
  ARRAY_CONSTRUCT(
    'A competitor launched a generic version of ' || p.MOLECULE || ' at a lower price. ',
    'Several pharmacies report a stock-out of ' || p.BRAND_NAME || ' for the last two weeks. ',
    'The list price of ' || p.BRAND_NAME || ' was increased this month. ',
    p.THERAPEUTIC_AREA || ' specialists gave feedback on ' || p.BRAND_NAME || '. ',
    'The health regulator published new guidance affecting ' || p.MOLECULE || '. ')[b.t]::VARCHAR
  || ARRAY_CONSTRUCT(
    'Retail chains are pushing the cheaper alternative at the counter.',
    'Patients are switching to competing brands while supply recovers.',
    'Independent pharmacies expect volume to drop in the short term.',
    'Doctors highlighted good tolerability and strong adherence.',
    'Reimbursement lists may change in the next quarter.')[UNIFORM(0,4,RANDOM())]::VARCHAR AS NOTE_TEXT
FROM b JOIN SOURCE_SYSTEM.PRODUCTS p ON p.PRODUCT_ID = b.PRODUCT_ID;
```

> **Why join PRODUCTS when generating SELL\_OUT?**  `NET_AMOUNT_USD` is derived from the product list price with a random discount (0–15%), so revenue numbers are internally consistent with the catalog.

### 1.3 Warehouse scaling demo

Compute and storage are separated: you can resize a warehouse in seconds, without moving data. Run an expensive aggregation on XSMALL, resize to MEDIUM, run it again, then scale back down. Disable the result cache first so we measure real compute.

```sql
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Run on XSMALL
SELECT p.THERAPEUTIC_AREA, ph.COUNTRY, COUNT(DISTINCT s.PHARMACY_ID) AS PHARMACIES,
       SUM(s.UNITS) AS UNITS, SUM(s.NET_AMOUNT_USD) AS SALES_USD
FROM SOURCE_SYSTEM.SELL_OUT s
JOIN SOURCE_SYSTEM.PRODUCTS p    ON p.PRODUCT_ID = s.PRODUCT_ID
JOIN SOURCE_SYSTEM.PHARMACIES ph ON ph.PHARMACY_ID = s.PHARMACY_ID
GROUP BY 1, 2 ORDER BY SALES_USD DESC;

-- Scale UP (takes effect for new queries immediately)
ALTER WAREHOUSE CLOSEUP_HOL2_WH SET WAREHOUSE_SIZE = 'MEDIUM';

SELECT p.THERAPEUTIC_AREA, ph.COUNTRY, COUNT(DISTINCT s.PHARMACY_ID) AS PHARMACIES,
       SUM(s.UNITS) AS UNITS, SUM(s.NET_AMOUNT_USD) AS SALES_USD
FROM SOURCE_SYSTEM.SELL_OUT s
JOIN SOURCE_SYSTEM.PRODUCTS p    ON p.PRODUCT_ID = s.PRODUCT_ID
JOIN SOURCE_SYSTEM.PHARMACIES ph ON ph.PHARMACY_ID = s.PHARMACY_ID
GROUP BY 1, 2 ORDER BY SALES_USD DESC;

-- Scale back DOWN
ALTER WAREHOUSE CLOSEUP_HOL2_WH SET WAREHOUSE_SIZE = 'XSMALL';
```

> **Scale up vs scale out.**  Scaling *up* (bigger size) makes individual complex queries faster; each size doubles credits per hour. Scaling *out* (multi-cluster, `MIN_CLUSTER_COUNT`/`MAX_CLUSTER_COUNT`) adds clusters for concurrency (many users/dashboards). With 150k rows the difference is small — on billions of rows it is dramatic.

### 1.4 Result cache

The cloud services layer keeps query results for 24 hours. If the same query is executed again and the underlying data has not changed, Snowflake returns the cached result **without using the warehouse** (0 credits). This is controlled by the session parameter `USE_CACHED_RESULT`.

```sql
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- 1st run: computed by the warehouse
SELECT COUNT(*) AS ROWS_, SUM(NET_AMOUNT_USD) AS SALES_USD FROM SOURCE_SYSTEM.SELL_OUT;
-- 2nd run (identical text): served from the result cache in milliseconds
SELECT COUNT(*) AS ROWS_, SUM(NET_AMOUNT_USD) AS SALES_USD FROM SOURCE_SYSTEM.SELL_OUT;

-- Compare both executions
SELECT QUERY_TEXT, WAREHOUSE_SIZE, TOTAL_ELAPSED_TIME AS ELAPSED_MS, BYTES_SCANNED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 20))
WHERE QUERY_TEXT ILIKE 'SELECT COUNT(*) AS ROWS_%'
ORDER BY START_TIME DESC
LIMIT 2;
```

> In Query Profile, the cached execution shows a single **QUERY RESULT REUSE**  node and `BYTES_SCANNED = 0`. Keep `USE_CACHED_RESULT = TRUE` in production (the default); set it to FALSE only for benchmarking.

> **Optional — skip during the SnowDay session.**  Budgets need a few minutes to activate and do not affect the next steps. Come back to it after the event.

### 1.5 Cost guardrails: resource monitor + budget

**Resource monitors** control warehouse credits and can *suspend* warehouses. **Budgets** track spend (including serverless features such as tasks and dynamic tables) and send notifications when projected spend exceeds a monthly limit.

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR CLOSEUP_HOL2_RM
  WITH CREDIT_QUOTA = 20
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE CLOSEUP_HOL2_WH SET RESOURCE_MONITOR = CLOSEUP_HOL2_RM;

-- Custom budget (class SNOWFLAKE.CORE.BUDGET) for the HOL objects
CREATE OR REPLACE SNOWFLAKE.CORE.BUDGET CLOSEUP_HOL2.APPS.CLOSEUP_HOL2_BUDGET()
  COMMENT = 'Monthly budget for the Close-Up HOL 2 platform';

CALL CLOSEUP_HOL2.APPS.CLOSEUP_HOL2_BUDGET!SET_SPENDING_LIMIT(50);

-- Track the warehouse and the whole database (serverless tasks, DTs, storage...)
CALL CLOSEUP_HOL2.APPS.CLOSEUP_HOL2_BUDGET!ADD_RESOURCE(
  SYSTEM$REFERENCE('WAREHOUSE', 'CLOSEUP_HOL2_WH', 'SESSION', 'APPLYBUDGET'));
CALL CLOSEUP_HOL2.APPS.CLOSEUP_HOL2_BUDGET!ADD_RESOURCE(
  SYSTEM$REFERENCE('DATABASE', 'CLOSEUP_HOL2', 'SESSION', 'APPLYBUDGET'));

CALL CLOSEUP_HOL2.APPS.CLOSEUP_HOL2_BUDGET!GET_SPENDING_LIMIT();
CALL CLOSEUP_HOL2.APPS.CLOSEUP_HOL2_BUDGET!GET_LINKED_RESOURCES();
```

> **Notifications.**  To receive budget emails, create an email notification integration with a verified address and call `!SET_EMAIL_NOTIFICATIONS`. Budget usage data refreshes periodically (not real-time), so new budgets may show 0 spend for a few hours.

### 1.6 Verify

```sql
SELECT 'LABORATORIES' AS TABLE_NAME, COUNT(*) AS ROWS_ FROM SOURCE_SYSTEM.LABORATORIES UNION ALL
SELECT 'PRODUCTS',      COUNT(*) FROM SOURCE_SYSTEM.PRODUCTS      UNION ALL
SELECT 'PHARMACIES',    COUNT(*) FROM SOURCE_SYSTEM.PHARMACIES    UNION ALL
SELECT 'DOCTORS',       COUNT(*) FROM SOURCE_SYSTEM.DOCTORS       UNION ALL
SELECT 'SELL_OUT',      COUNT(*) FROM SOURCE_SYSTEM.SELL_OUT      UNION ALL
SELECT 'PRESCRIPTIONS', COUNT(*) FROM SOURCE_SYSTEM.PRESCRIPTIONS UNION ALL
SELECT 'FIELD_NOTES',   COUNT(*) FROM SOURCE_SYSTEM.FIELD_NOTES;
```

Expected: 15 / 80 / 400 / 600 / 150,000 / 60,000 / 300 rows.

---

### Recap: what you built in Step 1

- **You built:** warehouse `CLOSEUP_HOL2_WH`, database `CLOSEUP_HOL2` with the layers `SOURCE_SYSTEM`, `RAW` (bronze), `SILVER`, `GOLD`, `SEMANTIC` and `APPS`, and the 7-table simulated source system. Optionally, the resource monitor `CLOSEUP_HOL2_RM` and the budget `CLOSEUP_HOL2_BUDGET`.
- **You saw:** scaling the warehouse up and down in seconds, and the result cache returning a repeated query with no compute.
- **Take it to your projects:** size warehouses per workload, keep auto-suspend short, and add cost guardrails (resource monitor for warehouses, budget for total spend including serverless and AI) before the first production load.

## Step 2: Resilient Ingestion (~15 min)

**What this step does:** lands the sell-out JSON files with an ingestion that survives bad rows, new columns and repeated files, and keeps the rejected rows instead of losing them.

```text
JSON batches (batch_1 · batch_2 · batch_3) ──► INGEST_STAGE (files in Snowflake) ──► Dry run (2.4: what will fail?) ──► COPY INTO (MATCH_BY_COLUMN_NAME · ON_ERROR = CONTINUE) ──► SELL_OUT_LANDING (typed · schema evolution)
Bad rows (UNITS = N/A) ──► SELL_OUT_REJECTS (quarantine, 2.7)
```

| Real-world problem | What handles it | Step |
| --- | --- | --- |
| A new column appears at the source | Schema evolution adds it (`PROMO_FLAG`) | 2.3, 2.6 |
| Some values do not fit the type | Rows rejected, the rest loads; rejects go to quarantine | 2.6, 2.7 |
| The same files arrive again | `COPY` skips files it already loaded | 2.8 |
| "Did that file load?" | `COPY_HISTORY` / `LOAD_HISTORY` | 2.9 |

### 2.1 Stage and file format

We simulate an upstream system that drops JSON files into an internal stage in batches. Production pipelines must survive three classic problems: **schema drift** (new fields appear), **bad records** (malformed values) and **re-deliveries** (the same file arrives twice).

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE CLOSEUP_HOL2_WH;
USE DATABASE CLOSEUP_HOL2;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT RAW.FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE
  COMPRESSION = AUTO;

CREATE OR REPLACE STAGE RAW.INGEST_STAGE
  FILE_FORMAT = RAW.FF_JSON
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Landing stage for sell-out JSON batches';
```

### 2.2 Produce three JSON batches (the "upstream" system)

We unload `SOURCE_SYSTEM.SELL_OUT` into three folders. `batch_3` carries a new field `PROMO_FLAG` and ~20 rows where `UNITS = 'N/A'` (a typical data-entry error).

```sql
-- batch_1: SALE_ID S0000001..S0060000
COPY INTO @RAW.INGEST_STAGE/sell_out/batch_1/
FROM (
  SELECT OBJECT_CONSTRUCT('SALE_ID', SALE_ID, 'SALE_DATE', SALE_DATE, 'PHARMACY_ID', PHARMACY_ID,
                          'PRODUCT_ID', PRODUCT_ID, 'UNITS', UNITS, 'NET_AMOUNT_USD', NET_AMOUNT_USD)
  FROM SOURCE_SYSTEM.SELL_OUT WHERE SALE_ID <= 'S0060000'
)
FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP) OVERWRITE = TRUE;

-- batch_2: SALE_ID S0060001..S0120000
COPY INTO @RAW.INGEST_STAGE/sell_out/batch_2/
FROM (
  SELECT OBJECT_CONSTRUCT('SALE_ID', SALE_ID, 'SALE_DATE', SALE_DATE, 'PHARMACY_ID', PHARMACY_ID,
                          'PRODUCT_ID', PRODUCT_ID, 'UNITS', UNITS, 'NET_AMOUNT_USD', NET_AMOUNT_USD)
  FROM SOURCE_SYSTEM.SELL_OUT WHERE SALE_ID > 'S0060000' AND SALE_ID <= 'S0120000'
)
FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP) OVERWRITE = TRUE;

-- batch_3: new field PROMO_FLAG + ~20 malformed rows (UNITS = 'N/A')
COPY INTO @RAW.INGEST_STAGE/sell_out/batch_3/
FROM (
  SELECT OBJECT_CONSTRUCT('SALE_ID', SALE_ID, 'SALE_DATE', SALE_DATE, 'PHARMACY_ID', PHARMACY_ID,
                          'PRODUCT_ID', PRODUCT_ID,
                          'UNITS', IFF(MOD(TO_NUMBER(SUBSTR(SALE_ID, 2)), 1500) = 0, 'N/A'::VARIANT, UNITS::VARIANT),
                          'NET_AMOUNT_USD', NET_AMOUNT_USD,
                          'PROMO_FLAG', UNIFORM(0, 9, RANDOM()) < 2)
  FROM SOURCE_SYSTEM.SELL_OUT WHERE SALE_ID > 'S0120000'
)
FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP) OVERWRITE = TRUE;

LIST @RAW.INGEST_STAGE/sell_out/;

-- Peek inside batch_3 without loading it
SELECT $1 FROM @RAW.INGEST_STAGE/sell_out/batch_3/ (FILE_FORMAT => 'RAW.FF_JSON')
WHERE $1:UNITS::VARCHAR = 'N/A' LIMIT 5;
```

### 2.3 Landing table with schema evolution

The landing table is typed and has two audit columns: `LOADED_AT` (column default) and `SOURCE_FILE` (filled with `INCLUDE_METADATA`). `ENABLE_SCHEMA_EVOLUTION = TRUE` lets `COPY` add new columns automatically when files bring new fields.

```sql
CREATE OR REPLACE TABLE RAW.SELL_OUT_LANDING (
  SALE_ID         VARCHAR,
  SALE_DATE       DATE,
  PHARMACY_ID     VARCHAR,
  PRODUCT_ID      VARCHAR,
  UNITS           NUMBER,
  NET_AMOUNT_USD  NUMBER(12,2),
  SOURCE_FILE     VARCHAR,
  LOADED_AT       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
ENABLE_SCHEMA_EVOLUTION = TRUE
CHANGE_TRACKING = TRUE
COMMENT = 'Sell-out landing table (bronze). Schema evolves with incoming files.';
```

> **Schema evolution requirements.**  (1) The table must have `ENABLE_SCHEMA_EVOLUTION = TRUE`; (2) the `COPY` must use `MATCH_BY_COLUMN_NAME`; (3) the role running the COPY must **own the table** (or have EVOLVE SCHEMA + ALTER privileges). With ON\_ERROR=CONTINUE, a file can add at most 10 columns per COPY.

### 2.4 Dry run before loading

**Why a dry run?** Batch 3 is the "bad" delivery from the source system: it brings a new column and some broken values. Before loading it, we want to know **what will go wrong, where, and how much**, so we can decide whether to load it (and accept the rejects) or send it back to the source. Nothing is written to the landing table in this step.

A JSON file can fail in two different ways, and each needs its own check:

| Problem | Example | What happens at load time | Checked by |
| --- | --- | --- | --- |
| The file cannot be read | broken JSON (missing brace, truncated file) | the record or file is rejected | query (1) |
| The file is readable, but a value does not fit the column type | `"UNITS": "N/A"` going into a `NUMBER` column | that **row** is rejected | query (2) |

`VALIDATION_MODE = RETURN_ERRORS` is Snowflake's built-in dry run: it reads the files and returns the errors without loading anything. Here it only covers the first problem, for two reasons: it **cannot be combined with `MATCH_BY_COLUMN_NAME`** (which our real load needs for schema evolution), and it loads into a single `VARIANT` column, where any valid JSON fits, so it never sees a type error. Query (2) covers the second problem by simulating the cast ourselves.

```sql
-- (1) Parse-level validation: is every record valid JSON? (expect 0 errors)
CREATE OR REPLACE TEMPORARY TABLE RAW.SELL_OUT_PREFLIGHT (V VARIANT);

COPY INTO RAW.SELL_OUT_PREFLIGHT
FROM @RAW.INGEST_STAGE/sell_out/batch_3/
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
VALIDATION_MODE = RETURN_ERRORS;

-- (2) Type-level validation: which records will fail the cast into the typed table?
SELECT METADATA$FILENAME AS FILE_NAME, METADATA$FILE_ROW_NUMBER AS ROW_NUM,
       $1:SALE_ID::VARCHAR AS SALE_ID, $1:UNITS::VARCHAR AS BAD_UNITS
FROM @RAW.INGEST_STAGE/sell_out/batch_3/ (FILE_FORMAT => 'RAW.FF_JSON')
WHERE TRY_TO_NUMBER($1:UNITS::VARCHAR) IS NULL
   OR TRY_TO_DATE($1:SALE_DATE::VARCHAR) IS NULL;
```

> **Reading the results.** Query (1) returns no rows: the JSON is well-formed. Query (2) returns about 20 **rows** (not whole files), each pointing to its file and row number, all with `UNITS = 'N/A'`. These are exactly the rows the real load will reject in step 2.6 (`errors_seen = 20`) and that we will move to quarantine in step 2.7. The other ~29,980 rows of the batch are fine, so the decision here is: load the batch with `ON_ERROR = CONTINUE` and handle the 20 rejects, instead of blocking the whole delivery.
>
> For CSV loads without column matching, `VALIDATION_MODE` alone also reports type errors, so query (2) is not needed.

### 2.5 Load batches 1 and 2

```sql
COPY INTO RAW.SELL_OUT_LANDING
FROM @RAW.INGEST_STAGE/sell_out/
PATTERN = '.*batch_[12]/.*'
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (SOURCE_FILE = METADATA$FILENAME)
ON_ERROR = CONTINUE;

SELECT COUNT(*) AS ROWS_LOADED FROM RAW.SELL_OUT_LANDING;   -- 120,000
```

### 2.6 Load batch 3: schema drift + bad rows

```sql
COPY INTO RAW.SELL_OUT_LANDING
FROM @RAW.INGEST_STAGE/sell_out/batch_3/
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (SOURCE_FILE = METADATA$FILENAME)
ON_ERROR = CONTINUE;

-- PROMO_FLAG was added automatically
DESC TABLE RAW.SELL_OUT_LANDING;

SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
FROM CLOSEUP_HOL2.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = 'SELL_OUT_LANDING'
ORDER BY ORDINAL_POSITION;

-- Rows from batches 1-2 have PROMO_FLAG = NULL
SELECT PROMO_FLAG, COUNT(*) FROM RAW.SELL_OUT_LANDING GROUP BY 1;
```

### 2.7 Rejected rows: COPY result, VALIDATE() and a quarantine table

The COPY output of batch\_3 already reports `PARTIALLY_LOADED`, `rows_loaded = 29,980`, `errors_seen = 20` and the first error (*Failed to cast variant value "N/A" to FIXED*, line 1500).

`VALIDATE(<table>, JOB_ID => '_last')` is the usual way to list the rejected rows of a previous COPY, but it only works for COPY **without transforms**. Our load uses `MATCH_BY_COLUMN_NAME` (required for schema evolution), which Snowflake treats as a transform, so `VALIDATE()` cannot be used here.

The production pattern for column-matched loads is a **quarantine table** built from the staged files with the same rules the typed table enforces:

```sql
CREATE OR REPLACE TABLE RAW.SELL_OUT_REJECTS AS
SELECT METADATA$FILENAME AS FILE_NAME, METADATA$FILE_ROW_NUMBER AS LINE,
       $1 AS REJECTED_RECORD, 'UNITS is not numeric' AS ERROR, CURRENT_TIMESTAMP() AS DETECTED_AT
FROM @RAW.INGEST_STAGE/sell_out/ (FILE_FORMAT => 'RAW.FF_JSON')
WHERE TRY_TO_NUMBER($1:UNITS::VARCHAR) IS NULL;

SELECT * FROM RAW.SELL_OUT_REJECTS LIMIT 10;   -- 20 rows
```

> Alert on `COUNT(*) > 0` in the quarantine table (Snowflake Alerts) and send it back to the data owner instead of silently dropping records. Snowpipe users get the same information via `VALIDATE_PIPE_LOAD` / `COPY_HISTORY`, and Snowflake *error tables* (DML error logging) are another option for INSERT-based pipelines.

### 2.8 Idempotency: reloading is a no-op

Snowflake keeps **load metadata for 64 days** per table: files already loaded (same name + checksum) are skipped. Re-running the COPY is therefore safe.

```sql
COPY INTO RAW.SELL_OUT_LANDING
FROM @RAW.INGEST_STAGE/sell_out/
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (SOURCE_FILE = METADATA$FILENAME)
ON_ERROR = CONTINUE;
-- Result: "Copy executed with 0 files processed."
```

> **FORCE = TRUE**  ignores load metadata and reloads every file, creating duplicates. Use it only for deliberate backfills into an empty/truncated table. `LOAD_UNCERTAIN_FILES = TRUE` is the safer option for files older than 64 days.

### 2.9 Load auditing: COPY\_HISTORY and LOAD\_HISTORY

```sql
SELECT FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, ERROR_COUNT, FIRST_ERROR_MESSAGE, LAST_LOAD_TIME
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'RAW.SELL_OUT_LANDING',
       START_TIME => DATEADD(hour, -2, CURRENT_TIMESTAMP())))
ORDER BY LAST_LOAD_TIME;

SELECT FILE_NAME, STATUS, ROW_COUNT, ERROR_COUNT
FROM CLOSEUP_HOL2.INFORMATION_SCHEMA.LOAD_HISTORY
WHERE SCHEMA_NAME = 'RAW' AND TABLE_NAME = 'SELL_OUT_LANDING'
ORDER BY LAST_LOAD_TIME;
```

batch\_3 files show `PARTIALLY_LOADED` with their error counts; batches 1–2 show `LOADED`.

### 2.10 Dimensions and price events into RAW

Dimensions change slowly, so we copy them typed with CTAS. We also build `RAW.PRODUCT_PRICE_EVENTS`: 3 price changes per product over the last 18 months, which will feed an SCD2 table in Step 4.

```sql
CREATE OR REPLACE TABLE RAW.LABORATORIES  AS SELECT * FROM SOURCE_SYSTEM.LABORATORIES;
CREATE OR REPLACE TABLE RAW.PRODUCTS      AS SELECT * FROM SOURCE_SYSTEM.PRODUCTS;
CREATE OR REPLACE TABLE RAW.PHARMACIES    AS SELECT * FROM SOURCE_SYSTEM.PHARMACIES;
CREATE OR REPLACE TABLE RAW.DOCTORS       AS SELECT * FROM SOURCE_SYSTEM.DOCTORS;
CREATE OR REPLACE TABLE RAW.PRESCRIPTIONS AS SELECT * FROM SOURCE_SYSTEM.PRESCRIPTIONS;
CREATE OR REPLACE TABLE RAW.FIELD_NOTES   AS SELECT * FROM SOURCE_SYSTEM.FIELD_NOTES;

CREATE OR REPLACE TABLE RAW.PRODUCT_PRICE_EVENTS AS
SELECT
  p.PRODUCT_ID,
  ROUND(p.LIST_PRICE_USD * (0.85 + 0.05 * e.k + UNIFORM(0, 5, RANDOM()) / 100), 2)::NUMBER(10,2) AS LIST_PRICE_USD,
  DATEADD(day, -(540 - 180 * (e.k - 1)) + UNIFORM(0, 60, RANDOM()), CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS EVENT_TS
FROM RAW.PRODUCTS p
CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3) e;
```

### 2.11 Verify

```sql
SELECT
  (SELECT COUNT(*) FROM RAW.SELL_OUT_LANDING)                              AS LANDING_ROWS,     -- ~149,980
  (SELECT COUNT(*) FROM SOURCE_SYSTEM.SELL_OUT) - (SELECT COUNT(*) FROM RAW.SELL_OUT_LANDING) AS REJECTED_ROWS, -- ~20
  (SELECT COUNT(*) FROM RAW.SELL_OUT_REJECTS)                              AS QUARANTINED,      -- 20
  (SELECT COUNT(*) FROM RAW.SELL_OUT_LANDING WHERE PROMO_FLAG IS NOT NULL)  AS ROWS_WITH_PROMO_FLAG, -- 29,980
  (SELECT COUNT(*) FROM RAW.PRODUCT_PRICE_EVENTS)                           AS PRICE_EVENTS,     -- 240
  (SELECT COUNT(DISTINCT SOURCE_FILE) FROM RAW.SELL_OUT_LANDING)            AS FILES_LOADED;
```

---

### Recap: what you built in Step 2

- **You built:** stage `RAW.INGEST_STAGE` and format `RAW.FF_JSON`; the typed landing table `RAW.SELL_OUT_LANDING` with schema evolution and audit columns; the quarantine table `RAW.SELL_OUT_REJECTS`; and the RAW dimensions plus `RAW.PRODUCT_PRICE_EVENTS`.
- **What the pipeline now survives:** a new column at the source (`PROMO_FLAG` added automatically), bad values (20 rows rejected and kept in quarantine instead of lost), and the same files arriving twice (reload is a no-op).
- **The checks you used:** dry run before loading (step 2.4), the COPY result and quarantine (2.7), `COPY_HISTORY` / `LOAD_HISTORY` for auditing (2.9).
- **Take it to your projects:** this is the template for any file-based source: typed landing table + `MATCH_BY_COLUMN_NAME` + `ON_ERROR = CONTINUE` + quarantine table + an alert when the quarantine is not empty.

## Step 3: CDC with Streams + Tasks (~15 min)

**What this step does, in one sentence:** every 5 minutes, a small pipeline copies **only the new or changed rows** from the landing table `RAW.SELL_OUT_LANDING` into a clean SILVER table, without duplicates and without reprocessing what was already done.

```text
 RAW.SELL_OUT_LANDING ──► SELL_OUT_LANDING_STREAM ──► T_MERGE_SELL_OUT (MERGE) ──► SILVER.SELL_OUT_TASKS
   (landing table)         "what changed since          runs only if the             (clean, one row
                            the last run?"              stream has data              per SALE_ID)

 T_ROOT (every 5 min) ──► T_MERGE_SELL_OUT ──► T_FINALIZER ──► RAW.PIPELINE_RUN_LOG
   the clock                 the work            always runs        one line per run
```

### 3.1 Concepts

| Piece | Object in this lab | What it does | Think of it as |
| --- | --- | --- | --- |
| Stream | `RAW.SELL_OUT_LANDING_STREAM` | Returns only the rows inserted, updated or deleted in the landing table since the last time it was consumed. | A bookmark: it remembers where you stopped reading. |
| Root task | `RAW.T_ROOT` | Holds the schedule (every 5 minutes) and starts the graph. It does no real work. | The clock. |
| Child task | `RAW.T_MERGE_SELL_OUT` | Reads the stream and MERGEs the changes into SILVER. Skipped (no cost) when the stream is empty. | The worker. |
| Finalizer | `RAW.T_FINALIZER` | Runs at the end of every graph run, success or failure, and writes a log line. | The person who closes the shop and fills in the logbook. |
| Target table | `SILVER.SELL_OUT_TASKS` | The clean result: one row per `SALE_ID`. | The shelf where the finished goods go. |

The details behind each piece:

- **Stream** — a change-tracking offset on a table. Querying it returns the rows changed since the last *consumed* offset, with `METADATA$ACTION`, `METADATA$ISUPDATE` and `METADATA$ROW_ID`. The offset only advances when the stream is read inside a **committed DML** transaction — this gives **exactly-once** processing: if the MERGE fails, the offset does not move and the changes are reprocessed next run.
- **Task** — scheduled SQL. Omitting `WAREHOUSE` makes it **serverless**: Snowflake sizes compute automatically and bills per second of actual use.
- **Task graph (DAG)** — one *root* task with the schedule, *child* tasks via `AFTER`, and an optional *finalizer* (`FINALIZE = <root>`) that always runs at the end, even when a child fails — perfect for logging and cleanup.
- `SYSTEM$STREAM_HAS_DATA()` in a `WHEN` clause skips the task when there is nothing to process — **no compute is consumed** for skipped runs.

### 3.2 Stream, target table and run log

`SHOW_INITIAL_ROWS = TRUE` makes the first consumption of the stream return all existing rows, so the first DAG run doubles as the initial backfill.

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE CLOSEUP_HOL2_WH;
USE DATABASE CLOSEUP_HOL2;
USE SCHEMA RAW;

CREATE OR REPLACE STREAM RAW.SELL_OUT_LANDING_STREAM
  ON TABLE RAW.SELL_OUT_LANDING
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC on the sell-out landing table';

CREATE OR REPLACE TABLE SILVER.SELL_OUT_TASKS (
  SALE_ID         VARCHAR PRIMARY KEY,
  SALE_DATE       DATE,
  PHARMACY_ID     VARCHAR,
  PRODUCT_ID      VARCHAR,
  UNITS           NUMBER,
  NET_AMOUNT_USD  NUMBER(12,2),
  PROMO_FLAG      BOOLEAN,
  SOURCE_FILE     VARCHAR,
  LOADED_AT       TIMESTAMP_LTZ,
  UPDATED_AT      TIMESTAMP_LTZ
) COMMENT = 'Silver sell-out maintained by Streams + Tasks (imperative CDC)';

CREATE OR REPLACE TABLE RAW.PIPELINE_RUN_LOG (
  RUN_GROUP_ID    VARCHAR,
  PIPELINE        VARCHAR,
  FINISHED_AT     TIMESTAMP_LTZ,
  SILVER_ROWS     NUMBER,
  PENDING_IN_STREAM BOOLEAN
);

SELECT METADATA$ACTION, COUNT(*) FROM RAW.SELL_OUT_LANDING_STREAM GROUP BY 1;  -- read only: offset does NOT move
```

### 3.3 Build the task DAG

**Order of execution in each run:** `T_ROOT` starts → `T_MERGE_SELL_OUT` runs only if the stream has data → `T_FINALIZER` always runs last. The three `CREATE TASK` statements below build exactly this graph: `AFTER RAW.T_ROOT` makes the merge a child of the root, and `FINALIZE = RAW.T_ROOT` makes the finalizer the last step.

The MERGE keeps only `INSERT` actions (an UPDATE in the source appears as a DELETE + INSERT pair with `METADATA$ISUPDATE = TRUE`), and dedups by `SALE_ID` with `QUALIFY` so a re-sent record never creates duplicates.

```sql
-- Root: owns the schedule (serverless: no WAREHOUSE parameter)
CREATE OR REPLACE TASK RAW.T_ROOT
  SCHEDULE = '5 MINUTE'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  TASK_AUTO_RETRY_ATTEMPTS = 2
  COMMENT = 'Root of the sell-out CDC pipeline'
AS
  SELECT CURRENT_TIMESTAMP();

-- Child: incremental upsert from the stream
CREATE OR REPLACE TASK RAW.T_MERGE_SELL_OUT
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AFTER RAW.T_ROOT
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.SELL_OUT_LANDING_STREAM')
AS
MERGE INTO SILVER.SELL_OUT_TASKS t
USING (
  SELECT SALE_ID, SALE_DATE, PHARMACY_ID, PRODUCT_ID, UNITS, NET_AMOUNT_USD,
         PROMO_FLAG, SOURCE_FILE, LOADED_AT
  FROM RAW.SELL_OUT_LANDING_STREAM
  WHERE METADATA$ACTION = 'INSERT' AND SALE_ID IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY LOADED_AT DESC) = 1
) s
ON t.SALE_ID = s.SALE_ID
WHEN MATCHED THEN UPDATE SET
  t.SALE_DATE = s.SALE_DATE, t.PHARMACY_ID = s.PHARMACY_ID, t.PRODUCT_ID = s.PRODUCT_ID,
  t.UNITS = s.UNITS, t.NET_AMOUNT_USD = s.NET_AMOUNT_USD, t.PROMO_FLAG = s.PROMO_FLAG,
  t.SOURCE_FILE = s.SOURCE_FILE, t.LOADED_AT = s.LOADED_AT, t.UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (SALE_ID, SALE_DATE, PHARMACY_ID, PRODUCT_ID, UNITS, NET_AMOUNT_USD, PROMO_FLAG, SOURCE_FILE, LOADED_AT, UPDATED_AT)
VALUES
  (s.SALE_ID, s.SALE_DATE, s.PHARMACY_ID, s.PRODUCT_ID, s.UNITS, s.NET_AMOUNT_USD, s.PROMO_FLAG, s.SOURCE_FILE, s.LOADED_AT, CURRENT_TIMESTAMP());

-- Finalizer: always runs at the end of the graph (success or failure)
CREATE OR REPLACE TASK RAW.T_FINALIZER
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  FINALIZE = RAW.T_ROOT
AS
INSERT INTO RAW.PIPELINE_RUN_LOG
SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
       'SELL_OUT_CDC',
       CURRENT_TIMESTAMP(),
       (SELECT COUNT(*) FROM SILVER.SELL_OUT_TASKS),
       SYSTEM$STREAM_HAS_DATA('RAW.SELL_OUT_LANDING_STREAM');
```

### 3.4 Resume (children first, then root) and run now

Tasks are created *suspended*. Resume children and finalizer before the root — or use `SYSTEM$TASK_DEPENDENTS_ENABLE('RAW.T_ROOT')` to resume the whole graph at once. `EXECUTE TASK` triggers an immediate run without waiting for the schedule.

```sql
ALTER TASK RAW.T_FINALIZER RESUME;
ALTER TASK RAW.T_MERGE_SELL_OUT RESUME;
ALTER TASK RAW.T_ROOT RESUME;

EXECUTE TASK RAW.T_ROOT;

SHOW TASKS IN SCHEMA RAW;
```

### 3.5 Monitor: TASK\_HISTORY and COMPLETE\_TASK\_GRAPHS

Wait ~1 minute and re-run until `T_FINALIZER` shows `SUCCEEDED`. You can also open **Monitoring » Task History** in Snowsight to see the graph.

```sql
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE, GRAPH_RUN_GROUP_ID
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.TASK_HISTORY(
       SCHEDULED_TIME_RANGE_START => DATEADD(hour, -1, CURRENT_TIMESTAMP()),
       RESULT_LIMIT => 50))
WHERE DATABASE_NAME = 'CLOSEUP_HOL2'
ORDER BY SCHEDULED_TIME DESC;

SELECT ROOT_TASK_NAME, STATE, FIRST_ERROR_TASK_NAME, FIRST_ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS(RESULT_LIMIT => 10))
WHERE ROOT_TASK_NAME = 'T_ROOT'
ORDER BY SCHEDULED_TIME DESC;

SELECT COUNT(*) AS SILVER_ROWS FROM SILVER.SELL_OUT_TASKS;
SELECT * FROM RAW.PIPELINE_RUN_LOG ORDER BY FINISHED_AT DESC;
SELECT COUNT(*) AS PENDING FROM RAW.SELL_OUT_LANDING_STREAM;   -- 0: the stream offset advanced
```

**What you should see after the first run:** `T_ROOT`, `T_MERGE_SELL_OUT` and `T_FINALIZER` with `STATE = SUCCEEDED`; `SILVER_ROWS` equal to the number of distinct `SALE_ID`s in the landing table; one line in `PIPELINE_RUN_LOG`; and `PENDING = 0` in the stream, because the bookmark moved forward.

### 3.6 Late data, corrections and exactly-once

Now simulate the real world: 5 late sales arrive, one existing sale is corrected, and one record is re-sent twice. Only the delta is processed.

| Event (simulated below) | What lands in RAW | What the stream shows | Result in SILVER |
| --- | --- | --- | --- |
| 5 late sales arrive | 5 new rows (`S9000001`…`S9000005`) | 5 `INSERT` rows | 5 new rows |
| An existing sale is corrected | `S0000001` updated to `UNITS = 999` | a `DELETE` + `INSERT` pair with `METADATA$ISUPDATE = TRUE` | the same row updated in place (`UNITS = 999`) |
| A late sale is re-sent | a 2nd copy of `S9000001` | 1 more `INSERT` row | no new row: the MERGE dedups by `SALE_ID` |
| The 150k historical rows | unchanged | nothing | not reprocessed |

```sql
-- 5 late sales (new SALE_IDs)
INSERT INTO RAW.SELL_OUT_LANDING (SALE_ID, SALE_DATE, PHARMACY_ID, PRODUCT_ID, UNITS, NET_AMOUNT_USD, PROMO_FLAG, SOURCE_FILE)
SELECT 'S9' || LPAD(SEQ4() + 1, 6, '0'), DATEADD(day, -1, CURRENT_DATE()),
       'PH' || LPAD(UNIFORM(1, 400, RANDOM()), 4, '0'), 'PRD' || LPAD(UNIFORM(1, 80, RANDOM()), 3, '0'),
       10, 250.00, FALSE, 'late_delivery'
FROM TABLE(GENERATOR(ROWCOUNT => 5));

-- Correction of an existing sale (UPDATE => DELETE + INSERT pair in the stream)
UPDATE RAW.SELL_OUT_LANDING SET UNITS = 999, NET_AMOUNT_USD = 99999.00 WHERE SALE_ID = 'S0000001';

-- The same late record re-sent (duplicate)
INSERT INTO RAW.SELL_OUT_LANDING (SALE_ID, SALE_DATE, PHARMACY_ID, PRODUCT_ID, UNITS, NET_AMOUNT_USD, PROMO_FLAG, SOURCE_FILE)
SELECT SALE_ID, SALE_DATE, PHARMACY_ID, PRODUCT_ID, UNITS, NET_AMOUNT_USD, PROMO_FLAG, 'late_delivery_resend'
FROM RAW.SELL_OUT_LANDING WHERE SALE_ID = 'S9000001';

-- What the stream sees (only the delta)
SELECT SALE_ID, UNITS, METADATA$ACTION, METADATA$ISUPDATE
FROM RAW.SELL_OUT_LANDING_STREAM ORDER BY SALE_ID, METADATA$ACTION;

EXECUTE TASK RAW.T_ROOT;
```

After the run completes (check 3.5 again), verify:

```sql
SELECT
  (SELECT COUNT(*) FROM SILVER.SELL_OUT_TASKS)                          AS SILVER_ROWS,
  (SELECT COUNT(DISTINCT SALE_ID) FROM RAW.SELL_OUT_LANDING)            AS DISTINCT_LANDING_IDS,
  (SELECT COUNT(*) FROM SILVER.SELL_OUT_TASKS WHERE SALE_ID LIKE 'S9%') AS LATE_ROWS,        -- 5 (not 6)
  (SELECT UNITS FROM SILVER.SELL_OUT_TASKS WHERE SALE_ID = 'S0000001')  AS CORRECTED_UNITS;  -- 999
```

> **Exactly-once:**  `SILVER_ROWS = DISTINCT_LANDING_IDS`, the duplicate re-send produced no extra row, and the correction was upserted in place. The 150k historical rows were not re-processed.

### 3.7 Error handling in production

- `SUSPEND_TASK_AFTER_NUM_FAILURES = 3` (on the root) auto-suspends the graph after 3 consecutive failed runs, avoiding endless failing runs.
- `TASK_AUTO_RETRY_ATTEMPTS = 2` retries a failed graph run automatically from the failed task.
- Use the finalizer to log status, and attach an `ERROR_INTEGRATION` (cloud notification) or a Snowflake **Alert** on `COMPLETE_TASK_GRAPHS` where `STATE = 'FAILED'`.
- Streams become *stale* if not consumed within the table's data retention window (+ `MAX_DATA_EXTENSION_TIME_IN_DAYS`); monitor `STALE_AFTER` in `SHOW STREAMS`.
- To change a task, suspend the root first, `ALTER`/recreate, then resume.

### 3.8 Suspend the pipeline

Dynamic Tables will take over in Step 4; suspend the root (it stops scheduling the whole graph) and the children.

```sql
ALTER TASK RAW.T_ROOT SUSPEND;
ALTER TASK RAW.T_MERGE_SELL_OUT SUSPEND;
ALTER TASK RAW.T_FINALIZER SUSPEND;

SHOW TASKS IN SCHEMA RAW;   -- state = suspended for all three
```

---

### Recap: what you built in Step 3

- **You built:** the stream `RAW.SELL_OUT_LANDING_STREAM`, the target `SILVER.SELL_OUT_TASKS`, the run log `RAW.PIPELINE_RUN_LOG`, and a task DAG: `T_ROOT` → `T_MERGE_SELL_OUT` → `T_FINALIZER`.
- **Key idea:** the stream records only what changed since the last read, and it advances only when the transaction commits, so a failed run reprocesses nothing twice and loses nothing.
- **Take it to your projects:** use Streams + Tasks when you need procedural control: merges with custom rules, calls to other systems, logging, and a finalizer that always runs. Remember to suspend tasks you are not using.

## Step 4: Declarative Pipelines with Dynamic Tables (~20 min)

**What this step does:** rebuilds the pipeline **declaratively**: you write the SELECT and the freshness you need, and Snowflake keeps each table up to date and processes only what changed.

```text
RAW.SELL_OUT_LANDING (landing) ──► SILVER.DT_SELL_OUT_CLEAN (incremental · lag DOWNSTREAM) ──► GOLD.DT_MARKET_SHARE_MONTHLY (adaptive · 1 hour)
SILVER.DT_SELL_OUT_CLEAN ──► GOLD.DT_PHARMACY_PERFORMANCE (1 hour (+ RAW dimensions))
RAW.PRODUCT_PRICE_EVENTS (price changes) ──► SILVER.DT_PRODUCT_PRICE_SCD2 (price history (SCD2))
RAW.SELL_OUT_LANDING CHANGES (change feed) ──► SILVER.DT_SELL_OUT_CDC_CUSTOM (custom REFRESH USING)
```

|  | Streams + Tasks (Step 3) | Dynamic Tables (Step 4) |
| --- | --- | --- |
| You write | Stream, MERGE, task graph, log | One SELECT per table |
| Who schedules | Your task schedule | Snowflake, to meet the target lag |
| Incremental | You code it (stream + MERGE) | Automatic (or `REFRESH USING`) |
| Best for | Procedural logic, side effects | SQL-defined tables and pipelines |

In Step 3 you built CDC *imperatively*: a stream, a MERGE, a task DAG, a finalizer and a run log. It works, but you own every moving part. In this step you rebuild the same pipeline **declaratively** with **Dynamic Tables (DTs)**: you write a `SELECT`, declare how fresh the result must be (`TARGET_LAG`), and Snowflake handles change tracking, scheduling, dependency ordering, retries and incremental computation.

### 4.1 Pipeline design

| Layer | Dynamic Table | Source | Refresh mode | Target lag |
| --- | --- | --- | --- | --- |
| Silver | `SILVER.DT_SELL_OUT_CLEAN` | RAW.SELL\_OUT\_LANDING | INCREMENTAL | DOWNSTREAM |
| Silver | `SILVER.DT_PRODUCT_PRICE_SCD2` | RAW.PRODUCT\_PRICE\_EVENTS | AUTO | 1 hour |
| Silver | `SILVER.DT_SELL_OUT_CDC_CUSTOM` | RAW.SELL\_OUT\_LANDING `CHANGES()` | CUSTOM\_INCREMENTAL (REFRESH USING) | 1 hour |
| Gold | `GOLD.DT_MARKET_SHARE_MONTHLY` | DT\_SELL\_OUT\_CLEAN + dims | ADAPTIVE | 1 hour |
| Gold | `GOLD.DT_PHARMACY_PERFORMANCE` | DT\_SELL\_OUT\_CLEAN + PHARMACIES | AUTO | 1 hour |

> **Golden rule of DT pipelines:**  intermediate (silver) tables use `TARGET_LAG = DOWNSTREAM` and refresh only when a consumer needs them; terminal (gold) tables own a time-based lag that drives the whole graph. If *every* table is DOWNSTREAM, nothing ever triggers a refresh and no error is raised.

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE CLOSEUP_HOL2_WH;
USE DATABASE CLOSEUP_HOL2;

-- Inspect the landing table first: UNITS can contain bad values such as 'N/A'
DESCRIBE TABLE RAW.SELL_OUT_LANDING;
```

### 4.2 Silver: clean, typed and deduplicated sell-out (INCREMENTAL)

This DT casts every column defensively (`::VARCHAR` first, so the same code works whether schema evolution typed the column as NUMBER or left it as text) with `TRY_TO_*` (bad values become NULL instead of failing the refresh), filters invalid rows, and keeps the latest version of each `SALE_ID` with `QUALIFY ROW_NUMBER() = 1`. A top-level `QUALIFY ... = 1` on a single base table is one of the operators Snowflake maintains most efficiently in incremental mode.

```sql
CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_SELL_OUT_CLEAN
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = CLOSEUP_HOL2_WH
  REFRESH_MODE = INCREMENTAL
  COMMENT      = 'Silver: typed, validated and deduplicated sell-out (1 row per SALE_ID)'
AS
SELECT
    SALE_ID::VARCHAR                                   AS SALE_ID,
    TRY_TO_DATE(SALE_DATE::VARCHAR)                    AS SALE_DATE,
    DATE_TRUNC('month', TRY_TO_DATE(SALE_DATE::VARCHAR)) AS SALE_MONTH,
    PHARMACY_ID::VARCHAR                               AS PHARMACY_ID,
    PRODUCT_ID::VARCHAR                                AS PRODUCT_ID,
    TRY_TO_NUMBER(UNITS::VARCHAR)                      AS UNITS,
    TRY_TO_NUMBER(NET_AMOUNT_USD::VARCHAR, 12, 2)      AS NET_AMOUNT_USD,
    COALESCE(TRY_TO_BOOLEAN(PROMO_FLAG::VARCHAR), FALSE) AS PROMO_FLAG,
    LOADED_AT                                          -- keep ORDER BY column in the projection (fast incremental path)
FROM RAW.SELL_OUT_LANDING
WHERE SALE_ID IS NOT NULL
  AND TRY_TO_NUMBER(UNITS::VARCHAR) > 0
  AND TRY_TO_NUMBER(NET_AMOUNT_USD::VARCHAR, 12, 2) >= 0
  AND TRY_TO_DATE(SALE_DATE::VARCHAR) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SALE_ID
    ORDER BY LOADED_AT DESC          -- latest load wins (late corrections overwrite)
) = 1;
```

> **Why `REFRESH_MODE = INCREMENTAL` explicitly?**  With `AUTO`, Snowflake picks the mode at creation time and may silently choose FULL. Declaring INCREMENTAL makes creation *fail* if the query cannot be incrementalized, which is what you want in production: a loud error at deploy time rather than a surprise bill later.

### 4.3 Silver: SCD Type 2 price history

Each price event opens a validity window that closes when the next event for the same product arrives (`LEAD`). The current row has `VALID_TO` = NULL and `IS_CURRENT` = TRUE.

```sql
CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_PRODUCT_PRICE_SCD2
  TARGET_LAG = '1 hour'
  WAREHOUSE  = CLOSEUP_HOL2_WH
  COMMENT    = 'Silver: SCD2 list-price history per product'
AS
SELECT
    PRODUCT_ID,
    LIST_PRICE_USD,
    EVENT_TS                                                          AS VALID_FROM,
    LEAD(EVENT_TS) OVER (PARTITION BY PRODUCT_ID ORDER BY EVENT_TS)   AS VALID_TO,
    LEAD(EVENT_TS) OVER (PARTITION BY PRODUCT_ID ORDER BY EVENT_TS) IS NULL AS IS_CURRENT
FROM RAW.PRODUCT_PRICE_EVENTS;

-- Point-in-time price lookup: what was the price of PRD001 at a given moment?
SELECT * FROM SILVER.DT_PRODUCT_PRICE_SCD2 WHERE PRODUCT_ID = 'PRD001' ORDER BY VALID_FROM;
```

### 4.4 Gold: monthly market share (ADAPTIVE, 1 hour)

Following the "Dynamic Tables: CTE + pre-aggregation" rule, we first aggregate sell-out to *(month, pharmacy country, product)* in a CTE and only then join the small dimensions. Market share is computed with a window over the therapeutic-area total for that month and country.

`REFRESH_MODE = ADAPTIVE` refreshes incrementally by default, but automatically falls back to a full recompute when it detects a large upstream change (for example a big backfill), then returns to incremental. It is the pragmatic choice for gold aggregates whose change volume is spiky.

```sql
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_MARKET_SHARE_MONTHLY
  TARGET_LAG   = '1 hour'
  WAREHOUSE    = CLOSEUP_HOL2_WH
  REFRESH_MODE = ADAPTIVE
  COMMENT      = 'Gold: monthly market share by country, therapeutic area and laboratory'
AS
WITH sales_agg AS (            -- pre-aggregate before joining: avoids join fan-out
    SELECT s.SALE_MONTH, s.PHARMACY_ID, s.PRODUCT_ID,
           SUM(s.UNITS) AS UNITS, SUM(s.NET_AMOUNT_USD) AS SALES_USD
    FROM SILVER.DT_SELL_OUT_CLEAN s
    GROUP BY s.SALE_MONTH, s.PHARMACY_ID, s.PRODUCT_ID
),
by_lab AS (
    SELECT a.SALE_MONTH        AS MONTH,
           ph.COUNTRY,
           p.THERAPEUTIC_AREA,
           l.LAB_ID,
           l.LAB_NAME,
           SUM(a.UNITS)        AS UNITS,
           SUM(a.SALES_USD)    AS SALES_USD
    FROM sales_agg a
    JOIN RAW.PHARMACIES   ph ON ph.PHARMACY_ID = a.PHARMACY_ID
    JOIN RAW.PRODUCTS     p  ON p.PRODUCT_ID   = a.PRODUCT_ID
    JOIN RAW.LABORATORIES l  ON l.LAB_ID       = p.LAB_ID
    GROUP BY 1, 2, 3, 4, 5
)
SELECT MONTH, COUNTRY, THERAPEUTIC_AREA, LAB_ID, LAB_NAME, UNITS, SALES_USD,
       ROUND(100 * SALES_USD / NULLIF(SUM(SALES_USD) OVER (PARTITION BY MONTH, COUNTRY, THERAPEUTIC_AREA), 0), 2)
           AS MARKET_SHARE_PCT
FROM by_lab;
```

### 4.5 Gold: pharmacy performance

```sql
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_PHARMACY_PERFORMANCE
  TARGET_LAG = '1 hour'
  WAREHOUSE  = CLOSEUP_HOL2_WH
  COMMENT    = 'Gold: monthly KPIs per pharmacy with country ranking'
AS
WITH ph_month AS (
    SELECT SALE_MONTH, PHARMACY_ID,
           COUNT(*)                    AS TRANSACTIONS,
           COUNT(DISTINCT PRODUCT_ID)  AS DISTINCT_PRODUCTS,
           SUM(UNITS)                  AS UNITS,
           SUM(NET_AMOUNT_USD)         AS SALES_USD,
           SUM(IFF(PROMO_FLAG, NET_AMOUNT_USD, 0)) AS PROMO_SALES_USD
    FROM SILVER.DT_SELL_OUT_CLEAN
    GROUP BY SALE_MONTH, PHARMACY_ID
)
SELECT m.SALE_MONTH AS MONTH, m.PHARMACY_ID, ph.PHARMACY_NAME, ph.CHAIN, ph.CITY,
       ph.COUNTRY, ph.CHANNEL,
       m.TRANSACTIONS, m.DISTINCT_PRODUCTS, m.UNITS, m.SALES_USD, m.PROMO_SALES_USD,
       ROUND(m.SALES_USD / NULLIF(m.TRANSACTIONS, 0), 2) AS AVG_TICKET_USD,
       RANK() OVER (PARTITION BY m.SALE_MONTH, ph.COUNTRY ORDER BY m.SALES_USD DESC) AS RANK_IN_COUNTRY
FROM ph_month m
JOIN RAW.PHARMACIES ph ON ph.PHARMACY_ID = m.PHARMACY_ID;

-- Which refresh mode did Snowflake actually choose, and why?
SHOW DYNAMIC TABLES IN DATABASE CLOSEUP_HOL2;
SELECT "name", "schema_name", "target_lag", "refresh_mode", "refresh_mode_reason", "scheduling_state"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "schema_name", "name";
```

> **Read `refresh_mode_reason`.**  You will see that `DT_PHARMACY_PERFORMANCE` (AUTO) resolved to **FULL**: *"This dynamic table contains a complex query. Refresh mode has been automatically set to FULL for more predictable performance."* It combines a GROUP BY, a join and a RANK window in one table. That is fine for a 10k-row gold table, but if it grows, split it (aggregate in one DT, join + rank in a downstream DT) and declare `REFRESH_MODE = INCREMENTAL`. Also note that AUTO on `DT_PRODUCT_PRICE_SCD2` resolved to ADAPTIVE (compare `refresh_mode` with `configured_refresh_mode`). `refresh_mode_reason` is the first place to look when a DT is "unexpectedly expensive".

### 4.6 Custom incremental DT with `REFRESH USING`

Some logic is not a SELECT: you need to decide per row whether to insert, update or **delete** (true CDC). Custom incrementalization lets you write the single `MERGE` you would have put in a task, and Snowflake still owns scheduling, retries and the transaction. Key points:

- An explicit column list is required (Snowflake cannot infer it from DML).
- `SELF` is the dynamic table itself; you cannot reference it by name inside REFRESH USING.
- `CHANGES(INFORMATION => DEFAULT)` replaces the stream: Snowflake binds the change interval to the refresh boundaries automatically. Updates arrive as a DELETE + INSERT pair (`METADATA$ISUPDATE` = TRUE).
- The base table needs **change tracking**; we enable it explicitly so the dependency is visible in code reviews.
- Deduplicate in the USING subquery: MERGE with several source rows per key is nondeterministic.

```sql
ALTER TABLE RAW.SELL_OUT_LANDING SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_SELL_OUT_CDC_CUSTOM (
    SALE_ID          VARCHAR,
    SALE_DATE        DATE,
    PHARMACY_ID      VARCHAR,
    PRODUCT_ID       VARCHAR,
    UNITS            NUMBER(38,0),
    NET_AMOUNT_USD   NUMBER(12,2),
    LAST_CDC_ACTION  VARCHAR,
    PROCESSED_AT     TIMESTAMP_LTZ
)
  TARGET_LAG = '1 hour'
  WAREHOUSE  = CLOSEUP_HOL2_WH
  COMMENT    = 'Silver: custom incremental CDC (MERGE INTO SELF over CHANGES)'
  REFRESH USING (
    MERGE INTO SELF AS tgt
    USING (
        SELECT c.SALE_ID::VARCHAR                              AS SALE_ID,
               TRY_TO_DATE(c.SALE_DATE::VARCHAR)               AS SALE_DATE,
               c.PHARMACY_ID::VARCHAR                          AS PHARMACY_ID,
               c.PRODUCT_ID::VARCHAR                           AS PRODUCT_ID,
               TRY_TO_NUMBER(c.UNITS::VARCHAR)                 AS UNITS,
               TRY_TO_NUMBER(c.NET_AMOUNT_USD::VARCHAR, 12, 2) AS NET_AMOUNT_USD,
               c.METADATA$ACTION                               AS CDC_ACTION
        FROM RAW.SELL_OUT_LANDING CHANGES(INFORMATION => DEFAULT) AS c
        WHERE c.SALE_ID IS NOT NULL
        -- one row per key; when an update produces DELETE+INSERT keep the INSERT
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY c.SALE_ID
            ORDER BY CASE c.METADATA$ACTION WHEN 'INSERT' THEN 0 ELSE 1 END
        ) = 1
    ) AS src
    ON tgt.SALE_ID = src.SALE_ID
    WHEN MATCHED AND src.CDC_ACTION = 'DELETE' THEN DELETE
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' THEN UPDATE SET
        tgt.SALE_DATE = src.SALE_DATE, tgt.PHARMACY_ID = src.PHARMACY_ID,
        tgt.PRODUCT_ID = src.PRODUCT_ID, tgt.UNITS = src.UNITS,
        tgt.NET_AMOUNT_USD = src.NET_AMOUNT_USD,
        tgt.LAST_CDC_ACTION = 'UPDATE', tgt.PROCESSED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.CDC_ACTION = 'INSERT'
         AND src.UNITS > 0 AND src.SALE_DATE IS NOT NULL THEN
        INSERT (SALE_ID, SALE_DATE, PHARMACY_ID, PRODUCT_ID, UNITS, NET_AMOUNT_USD, LAST_CDC_ACTION, PROCESSED_AT)
        VALUES (src.SALE_ID, src.SALE_DATE, src.PHARMACY_ID, src.PRODUCT_ID, src.UNITS,
                src.NET_AMOUNT_USD, 'INSERT', CURRENT_TIMESTAMP())
  );

SELECT LAST_CDC_ACTION, COUNT(*) AS ROWS_ FROM SILVER.DT_SELL_OUT_CDC_CUSTOM GROUP BY 1;
```

> **Initial load cost.**  Without `BACKFILL FROM`, the first refresh replays every existing source row through your MERGE as an INSERT. For very large tables, clone an existing table with `BACKFILL FROM <table>` and start reading changes from creation time.

### 4.7 Prove it is incremental: insert new rows and refresh

Simulate a late batch (if the Step 3 tasks are still resumed they will also pick these rows up, that is fine): 500 new sales plus a correction to an existing sale. Then refresh manually (in production the scheduler does it) and read the `STATISTICS` column of the refresh history: it shows how many rows were inserted/deleted in each refresh, i.e. only the delta was processed.

```sql
-- 500 brand-new sales (new SALE_IDs) copied from existing ones
INSERT INTO RAW.SELL_OUT_LANDING (SALE_ID, SALE_DATE, PHARMACY_ID, PRODUCT_ID, UNITS, NET_AMOUNT_USD, SOURCE_FILE)
SELECT 'DT' || LPAD(SEQ4()::VARCHAR, 8, '0'), CURRENT_DATE() - 1, PHARMACY_ID, PRODUCT_ID, UNITS, NET_AMOUNT_USD, 'manual/late_batch'
FROM (SELECT * FROM RAW.SELL_OUT_LANDING WHERE TRY_TO_NUMBER(UNITS::VARCHAR) > 0 LIMIT 500);

-- A correction: update one existing sale (arrives as DELETE+INSERT in CHANGES())
UPDATE RAW.SELL_OUT_LANDING
   SET NET_AMOUNT_USD = NET_AMOUNT_USD + 1, LOADED_AT = CURRENT_TIMESTAMP()
 WHERE SALE_ID = (SELECT MIN(SALE_ID) FROM RAW.SELL_OUT_LANDING WHERE SALE_ID NOT LIKE 'DT%');

-- Refresh the gold table: DOWNSTREAM silver upstream is refreshed first, automatically
ALTER DYNAMIC TABLE GOLD.DT_MARKET_SHARE_MONTHLY REFRESH;
ALTER DYNAMIC TABLE GOLD.DT_PHARMACY_PERFORMANCE REFRESH;
ALTER DYNAMIC TABLE SILVER.DT_SELL_OUT_CDC_CUSTOM REFRESH;

-- Incremental refresh statistics (rows inserted/deleted per refresh)
SELECT NAME, REFRESH_ACTION, REFRESH_TRIGGER, STATE,
       STATISTICS:numInsertedRows::NUMBER AS INSERTED_ROWS,
       STATISTICS:numDeletedRows::NUMBER  AS DELETED_ROWS,
       DATEDIFF('millisecond', REFRESH_START_TIME, REFRESH_END_TIME) AS DURATION_MS,
       REFRESH_START_TIME
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME IN ('DT_SELL_OUT_CLEAN','DT_MARKET_SHARE_MONTHLY','DT_PHARMACY_PERFORMANCE','DT_SELL_OUT_CDC_CUSTOM')
ORDER BY REFRESH_START_TIME DESC
LIMIT 20;
```

The output of `ALTER DYNAMIC TABLE ... REFRESH` itself returns statistics too, e.g. for the CDC table `{"insertedRows":500,"deletedRows":0,"updatedRows":1}`. In the history you should see `REFRESH_ACTION = INCREMENTAL` for `DT_SELL_OUT_CLEAN` with roughly 500 inserted rows (plus 1 deleted / 1 inserted for the correction), not 150,000. In the CDC table the corrected sale now shows `LAST_CDC_ACTION = 'UPDATE'`.

### 4.8 Streams + Tasks vs Dynamic Tables vs REFRESH USING

| Aspect | Streams + Tasks (Step 3) | Dynamic Table (declarative SELECT) | Dynamic Table with REFRESH USING |
| --- | --- | --- | --- |
| You write | Stream, MERGE, task DAG, schedule, finalizer, logging | One SELECT + TARGET\_LAG | One MERGE / INSERT INTO SELF + TARGET\_LAG |
| Incremental logic | Hand-written | Inferred by Snowflake | Hand-written, change interval managed by Snowflake |
| Scheduling and dependencies | CRON / AFTER chains you maintain | Automatic from TARGET\_LAG and the DAG | Automatic |
| Deletes / soft deletes / audit trails | Full control | Limited to what a SELECT expresses | Full control (WHEN MATCHED ... DELETE) |
| Semantics guarantee | Yours | Result equals the query at a point in time | Yours (user-defined) |
| Procedures, multi-statement, external calls | Yes | No | No (single DML only) |
| Monitoring | TASK\_HISTORY + your own log table | DYNAMIC\_TABLE\_REFRESH\_HISTORY, graph in Snowsight | Same as DTs |
| Best for | Orchestration, side effects, non-SQL steps | Most transformation pipelines (default choice) | CDC with deletes, stream-static joins, migrating stream+task MERGEs |

### 4.9 Monitoring dynamic tables

```sql
-- 1) Freshness SLO: how much of the time was each DT within its target lag?
SELECT QUALIFIED_NAME, TARGET_LAG_SEC, MEAN_LAG_SEC, MAXIMUM_LAG_SEC,
       TIME_WITHIN_TARGET_LAG_RATIO, SCHEDULING_STATE:state::VARCHAR AS SCHEDULING_STATE,
       LATEST_DATA_TIMESTAMP
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE QUALIFIED_NAME ILIKE 'CLOSEUP_HOL2.%'
ORDER BY QUALIFIED_NAME;

-- 2) Only failures (empty result = healthy pipeline)
SELECT NAME, STATE, STATE_CODE, STATE_MESSAGE, REFRESH_START_TIME
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(ERROR_ONLY => TRUE))
WHERE QUALIFIED_NAME ILIKE 'CLOSEUP_HOL2.%'
ORDER BY REFRESH_START_TIME DESC;

-- 3) Incremental vs full: is incremental actually paying off?
SELECT NAME, REFRESH_ACTION, COUNT(*) AS REFRESHES,
       AVG(DATEDIFF('millisecond', REFRESH_START_TIME, REFRESH_END_TIME)) AS AVG_MS
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE STATE = 'SUCCEEDED' AND QUALIFIED_NAME ILIKE 'CLOSEUP_HOL2.%'
GROUP BY 1, 2
ORDER BY 1, 2;
```

> **Operations cheat-sheet:**  `ALTER DYNAMIC TABLE ... SUSPEND / RESUME` pauses a DT (downstream tables pause with it); `ALTER DYNAMIC TABLE ... SET TARGET_LAG = '15 minutes'` changes freshness without recreating; in Snowsight, open any DT and use the **Graph** and **Refresh History** tabs to see the DAG visually. For account-wide history (beyond 7 days) use `SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY`.

### Verification

```sql
SELECT 'DT_SELL_OUT_CLEAN' AS DT, COUNT(*) AS ROWS_ FROM SILVER.DT_SELL_OUT_CLEAN
UNION ALL SELECT 'DT_PRODUCT_PRICE_SCD2', COUNT(*) FROM SILVER.DT_PRODUCT_PRICE_SCD2
UNION ALL SELECT 'DT_SELL_OUT_CDC_CUSTOM', COUNT(*) FROM SILVER.DT_SELL_OUT_CDC_CUSTOM
UNION ALL SELECT 'DT_MARKET_SHARE_MONTHLY', COUNT(*) FROM GOLD.DT_MARKET_SHARE_MONTHLY
UNION ALL SELECT 'DT_PHARMACY_PERFORMANCE', COUNT(*) FROM GOLD.DT_PHARMACY_PERFORMANCE;

-- Market shares must sum to ~100% per month / country / therapeutic area
SELECT MONTH, COUNTRY, THERAPEUTIC_AREA, SUM(MARKET_SHARE_PCT) AS TOTAL_SHARE
FROM GOLD.DT_MARKET_SHARE_MONTHLY
GROUP BY 1, 2, 3
ORDER BY MONTH DESC, COUNTRY, THERAPEUTIC_AREA
LIMIT 10;
```

---

### Recap: what you built in Step 4

- **You built:** the Dynamic Tables `SILVER.DT_SELL_OUT_CLEAN` (incremental, deduplicated), `SILVER.DT_PRODUCT_PRICE_SCD2` (price history), `GOLD.DT_MARKET_SHARE_MONTHLY` and `GOLD.DT_PHARMACY_PERFORMANCE`, plus the custom `SILVER.DT_SELL_OUT_CDC_CUSTOM` with `REFRESH USING`.
- **The complete data flow:** files → `RAW.SELL_OUT_LANDING` → (stream + tasks **or** Dynamic Tables) → SILVER → GOLD. You proved it: a new row in RAW reached GOLD with nothing run by hand.
- **Choosing the approach:** Dynamic Tables when the result is a SQL-defined table; Streams + Tasks for procedural logic and side effects; `REFRESH USING` for custom incremental logic inside a Dynamic Table.
- **Take it to your projects:** start declarative (Dynamic Tables) and drop to Streams + Tasks only where you need the extra control.

## Step 5: Dev Lifecycle: Clone, Swap, Undrop (Optional)

**What this step does:** shows the development lifecycle without copying data: clone production into DEV, change it, swap it back, and recover from mistakes.

```text
CLOSEUP_HOL2 (production) ──► CLONE (zero-copy, seconds) ──► CLOSEUP_HOL2_DEV (develop & test) ──► SWAP (atomic promotion) ──► CLOSEUP_HOL2 (updated)
DROP TABLE (oops) ──► UNDROP (back in seconds)
Bad statement ──► Time Travel (clone BEFORE (STATEMENT => …)) ──► Restored table
```

> **Optional — skip during the SnowDay session.**  Nothing later in the lab depends on this step. Come back to it after the event.

Production-grade means you can change things safely. Snowflake gives you three primitives that replace most "copy the database to test" workflows: **zero-copy cloning** (metadata-only copies, no storage duplicated until data diverges), **atomic SWAP** (promote a validated object in one metadata operation) and **Time Travel / UNDROP** (recover from mistakes).

### 5.1 Zero-copy clone of the whole database

Cloning a database clones every schema, table, view, dynamic table, stage and grant on child objects in seconds, regardless of size. You pay only for micro-partitions that later change in either copy.

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE CLOSEUP_HOL2_WH;

CREATE OR REPLACE DATABASE CLOSEUP_HOL2_DEV CLONE CLOSEUP_HOL2;

-- Same row counts, no extra storage yet
SELECT 'PROD' AS ENV, COUNT(*) AS LABS FROM CLOSEUP_HOL2.RAW.LABORATORIES
UNION ALL
SELECT 'DEV', COUNT(*) FROM CLOSEUP_HOL2_DEV.RAW.LABORATORIES;
```

> **Dynamic tables in clones.**  Cloned DTs keep their definition and refresh state; they point to objects *inside the clone* when the references are in the same database. Suspend them in DEV if you do not want DEV refreshes to consume credits: `ALTER DYNAMIC TABLE ... SUSPEND`.

### 5.2 Develop the change in DEV

Business request: add a `SEGMENT` attribute to laboratories (Multinational vs Local). We build and test it in DEV only.

```sql
USE DATABASE CLOSEUP_HOL2_DEV;

ALTER TABLE RAW.LABORATORIES ADD COLUMN SEGMENT VARCHAR;

UPDATE RAW.LABORATORIES
   SET SEGMENT = IFF(HQ_COUNTRY IN ('Argentina','Brazil','Uruguay','Chile'), 'Local', 'Multinational');

-- Test in DEV
SELECT SEGMENT, COUNT(*) AS LABS FROM RAW.LABORATORIES GROUP BY 1;
```

### 5.3 Promote with an atomic SWAP

`ALTER TABLE a SWAP WITH b` exchanges the two tables (data, columns, metadata) in a single transaction: consumers never see a half-loaded table, and the old version stays available on the other side for an instant rollback (swap again).

```sql
ALTER TABLE CLOSEUP_HOL2.RAW.LABORATORIES SWAP WITH CLOSEUP_HOL2_DEV.RAW.LABORATORIES;

-- PROD now has SEGMENT; DEV holds the previous version (rollback = swap again)
SELECT LAB_ID, LAB_NAME, HQ_COUNTRY, SEGMENT FROM CLOSEUP_HOL2.RAW.LABORATORIES ORDER BY LAB_ID LIMIT 5;
SELECT COUNT(*) AS OLD_VERSION_ROWS FROM CLOSEUP_HOL2_DEV.RAW.LABORATORIES;
```

> **Grants follow the object, not the name.**  After a SWAP the privileges granted on each table travel with it. If DEV and PROD tables have different grants, re-apply them (or rely on FUTURE / database-role grants, see Step 6). Dynamic tables reading a swapped base table reinitialize on their next refresh.

### 5.4 Oops: DROP and UNDROP

```sql
USE DATABASE CLOSEUP_HOL2;

DROP TABLE RAW.FIELD_NOTES;

-- Fails: the table is gone
SELECT COUNT(*) FROM RAW.FIELD_NOTES;

-- Dropped objects remain recoverable for the Time Travel retention period
SHOW TABLES HISTORY LIKE 'FIELD_NOTES' IN SCHEMA CLOSEUP_HOL2.RAW;

UNDROP TABLE RAW.FIELD_NOTES;

SELECT COUNT(*) AS NOTES_RECOVERED FROM RAW.FIELD_NOTES;
```

The `SELECT` right after the DROP fails with `Object 'RAW.FIELD_NOTES' does not exist or not authorized.` That is expected. `UNDROP` restores it instantly with its data and grants. The same works for schemas and databases (`UNDROP SCHEMA`, `UNDROP DATABASE`).

### 5.5 Time Travel: query and restore the past

Simulate a bad update in DEV, then read the table as it was before the statement and restore it with a clone "at" that point.

```sql
USE DATABASE CLOSEUP_HOL2_DEV;

-- Bad update: someone zeroes every list price
UPDATE RAW.PRODUCTS SET LIST_PRICE_USD = 0;
SET bad_stmt = LAST_QUERY_ID();

SELECT SUM(LIST_PRICE_USD) AS NOW_TOTAL FROM RAW.PRODUCTS;

-- The table just before the bad statement
SELECT SUM(LIST_PRICE_USD) AS BEFORE_TOTAL FROM RAW.PRODUCTS BEFORE (STATEMENT => $bad_stmt);

-- Relative time travel: the table as it was 30 seconds ago
-- (works once the table is at least that old; use a smaller offset if it errors)
SELECT SUM(LIST_PRICE_USD) AS TOTAL_30S_AGO FROM RAW.PRODUCTS AT (OFFSET => -30);

-- Restore: clone the past version and swap it in
CREATE OR REPLACE TABLE RAW.PRODUCTS_RESTORED CLONE RAW.PRODUCTS BEFORE (STATEMENT => $bad_stmt);
ALTER TABLE RAW.PRODUCTS SWAP WITH RAW.PRODUCTS_RESTORED;
DROP TABLE RAW.PRODUCTS_RESTORED;

SELECT SUM(LIST_PRICE_USD) AS RESTORED_TOTAL FROM RAW.PRODUCTS;
```

> **Retention:**  default Time Travel is 1 day; Enterprise Edition allows up to 90 days per object (`DATA_RETENTION_TIME_IN_DAYS`). Keep it long on RAW tables that feed `CHANGES()` or streams: if retention expires before the next refresh, the consumer goes stale.

### 5.6 Clean up the DEV environment

```sql
USE DATABASE CLOSEUP_HOL2;
DROP DATABASE CLOSEUP_HOL2_DEV;

-- Verification: PROD has the promoted column and the recovered table; DEV is gone
SELECT (SELECT COUNT(*) FROM CLOSEUP_HOL2.RAW.LABORATORIES WHERE SEGMENT IS NOT NULL) AS LABS_WITH_SEGMENT,
       (SELECT COUNT(*) FROM CLOSEUP_HOL2.RAW.FIELD_NOTES)                            AS FIELD_NOTES_ROWS,
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = 'CLOSEUP_HOL2_DEV') AS DEV_DB_EXISTS;
```

### Recap: what you built in Step 5

- **You built:** the development database `CLOSEUP_HOL2_DEV` as a zero-copy clone, promoted a change with an atomic `SWAP`, recovered a dropped table with `UNDROP`, and restored a table to its state before a bad statement with Time Travel.
- **Key idea:** clones cost no extra storage until data changes, so a full dev or test copy of production takes seconds.
- **Take it to your projects:** clone production for every release test, and keep Time Travel retention long enough to recover from mistakes.

## Challenge Block 2 — Security & roles (25 min · step 6)

**Mission:** Set up functional and access roles, reproduce and fix the classic privilege errors, then make sure an analyst only sees what they should.

> **Checkpoint ✓**  Querying as `HOL_ANALYST` returns masked/filtered data and the step 6 *Verify* passes. When your team reaches the checkpoint, raise your hand (or post in the chat)
> — the first team gets the point for this block.

---

## Step 6: Security & Roles (~25 min)

**What this step does:** builds a role model you can copy to any project: people get **functional roles**, functional roles get **access roles**, and only access roles hold privileges on objects. Then it protects sensitive columns and rows with policies.

```text
Users ──► Functional roles (HOL_ENGINEER · HOL_ANALYST · AGENT_USER_ROLE) ──► Access roles (DR_SILVER_RW · DR_GOLD_READ) ──► Schemas (RAW · SILVER · GOLD)
DOCTORS.DOCTOR_NAME (sensitive column) ──► MP_PERSON_NAME (masking policy) ──► Masked unless allowed
Market data rows (by country) ──► RAP_COUNTRY + ROLE_COUNTRY_MAP (row access policy) ──► Only your countries
```

| Functional role | Access roles it gets | So it can |
| --- | --- | --- |
| `HOL_ENGINEER` | `DR_SILVER_RW` + `DR_GOLD_READ` | Read RAW, build and write SILVER, read GOLD |
| `HOL_ANALYST` | `DR_GOLD_READ` | Read GOLD only |
| `AGENT_USER_ROLE` | `DR_GOLD_READ` | Read GOLD and use the agent |

Most "roles always give us problems" stories come from three habits: granting privileges directly to people-facing roles, creating objects with whatever role happens to be active, and forgetting that new objects do not inherit old grants. This step fixes all three with a design you can copy, then breaks it on purpose so you recognize the error messages, and finishes with column masking and row-level security.

### 6.1 The model: access roles vs functional roles

|  | Access roles | Functional roles |
| --- | --- | --- |
| Represents | A set of privileges on objects ("read GOLD", "write SILVER") | A job ("data engineer", "analyst", "agent end-user") |
| Implemented as | **Database roles** (live inside the database, travel with clones and replication) | **Account roles** |
| Receives | Object privileges (USAGE, SELECT, INSERT, CREATE ...) incl. FUTURE grants | Access roles + warehouse usage |
| Granted to | Functional roles only | Users, and to SYSADMIN (hierarchy) |
| Never | Granted to users directly | Granted object privileges directly |

The hierarchy we build:

| ACCOUNTADMIN | SYSADMIN (owns databases, warehouses, functional roles roll up here) | | |
| --- | --- | --- | --- |
| Functional (account) roles | **HOL\_ENGINEER** | **HOL\_ANALYST** | **AGENT\_USER\_ROLE** |
| DR\_SILVER\_RW + DR\_GOLD\_READ | DR\_GOLD\_READ | DR\_GOLD\_READ + semantic-layer reads |
| USAGE + OPERATE on CLOSEUP\_HOL2\_WH | USAGE on CLOSEUP\_HOL2\_WH | USAGE on CLOSEUP\_HOL2\_WH |
| Access (database) roles | `CLOSEUP_HOL2.DR_GOLD_READ`: USAGE db + GOLD, SELECT on all/future tables, DTs, views `CLOSEUP_HOL2.DR_SILVER_RW`: USAGE db + RAW + SILVER, SELECT RAW, DML + CREATE in SILVER | | |

> **Which system role does what:**  `USERADMIN` creates users and roles; `SECURITYADMIN` (has MANAGE GRANTS) grants anything to anyone; `SYSADMIN` creates and owns databases and warehouses. `ACCOUNTADMIN` is for account-level settings only, never for day-to-day object creation. In this lab the database was created as ACCOUNTADMIN, so we use it for the database-role grants; in your real environment use SYSADMIN as the owner.

### 6.2 Create the functional roles and the hierarchy

```sql
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS HOL_ENGINEER    COMMENT = 'Functional role: data engineers (build silver, read gold)';
CREATE ROLE IF NOT EXISTS HOL_ANALYST     COMMENT = 'Functional role: analysts (read gold only)';
CREATE ROLE IF NOT EXISTS AGENT_USER_ROLE COMMENT = 'Functional role: end users of the Cortex agent / apps';

USE ROLE SECURITYADMIN;
-- Hierarchy: every custom role rolls up to SYSADMIN, so admins can manage what they create
GRANT ROLE HOL_ENGINEER    TO ROLE SYSADMIN;
GRANT ROLE HOL_ANALYST     TO ROLE SYSADMIN;
GRANT ROLE AGENT_USER_ROLE TO ROLE SYSADMIN;

-- Grant the roles to yourself (works for any attendee, no hard-coded user name)
SET me = CURRENT_USER();
GRANT ROLE HOL_ENGINEER    TO USER IDENTIFIER($me);
GRANT ROLE HOL_ANALYST     TO USER IDENTIFIER($me);
GRANT ROLE AGENT_USER_ROLE TO USER IDENTIFIER($me);
```

> **Not granting roles to SYSADMIN**  creates "orphan" roles: objects they own are invisible to SYSADMIN and only ACCOUNTADMIN can clean them up. Always close the hierarchy.

### 6.3 Access roles as database roles, with FUTURE grants

We also turn GOLD into a **managed access schema**: object owners can no longer grant privileges on their own objects; only the schema owner (or a role with MANAGE GRANTS) can. That centralizes control of the layer that analysts and agents consume.

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE CLOSEUP_HOL2;

ALTER SCHEMA CLOSEUP_HOL2.GOLD ENABLE MANAGED ACCESS;

CREATE DATABASE ROLE IF NOT EXISTS CLOSEUP_HOL2.DR_GOLD_READ  COMMENT = 'Access role: read GOLD';
CREATE DATABASE ROLE IF NOT EXISTS CLOSEUP_HOL2.DR_SILVER_RW  COMMENT = 'Access role: read RAW, read/write/create in SILVER';

-- DR_GOLD_READ
GRANT USAGE ON DATABASE CLOSEUP_HOL2                                   TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;
GRANT USAGE ON SCHEMA   CLOSEUP_HOL2.GOLD                              TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;
GRANT SELECT ON ALL TABLES          IN SCHEMA CLOSEUP_HOL2.GOLD        TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;
GRANT SELECT ON ALL DYNAMIC TABLES  IN SCHEMA CLOSEUP_HOL2.GOLD        TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;
GRANT SELECT ON FUTURE TABLES         IN SCHEMA CLOSEUP_HOL2.GOLD      TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CLOSEUP_HOL2.GOLD      TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;
-- NOTE: no grant on VIEWS yet. We will "discover" that gap in error (c).

-- DR_SILVER_RW
GRANT USAGE ON DATABASE CLOSEUP_HOL2                                   TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT USAGE ON SCHEMA   CLOSEUP_HOL2.RAW                               TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT USAGE ON SCHEMA   CLOSEUP_HOL2.SILVER                            TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT SELECT ON ALL TABLES    IN SCHEMA CLOSEUP_HOL2.RAW               TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CLOSEUP_HOL2.RAW               TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES    IN SCHEMA CLOSEUP_HOL2.SILVER TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA CLOSEUP_HOL2.SILVER TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT SELECT ON ALL DYNAMIC TABLES    IN SCHEMA CLOSEUP_HOL2.SILVER    TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CLOSEUP_HOL2.SILVER    TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE ON SCHEMA CLOSEUP_HOL2.SILVER TO DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW;

-- Wire access roles into functional roles
GRANT DATABASE ROLE CLOSEUP_HOL2.DR_SILVER_RW TO ROLE HOL_ENGINEER;
GRANT DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ TO ROLE HOL_ENGINEER;
GRANT DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ TO ROLE HOL_ANALYST;
GRANT DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ TO ROLE AGENT_USER_ROLE;

-- Compute: engineers can also resize/resume the warehouse. HOL_ANALYST is deliberately
-- left WITHOUT warehouse usage for now (error b).
GRANT USAGE, OPERATE ON WAREHOUSE CLOSEUP_HOL2_WH TO ROLE HOL_ENGINEER;
GRANT USAGE          ON WAREHOUSE CLOSEUP_HOL2_WH TO ROLE AGENT_USER_ROLE;
```

> **Future grants: two rules that bite.**  (1) Schema-level future grants *override* database-level future grants for the same object type: if a schema has any future grant for TABLES, database-level future grants on TABLES are ignored in that schema. (2) Future grants are per object type: TABLES does not cover VIEWS or DYNAMIC TABLES. Grant each type you create.

### 6.4 Reproduce (and fix) the three classic errors

> **Secondary roles hide errors.**  Many accounts default to `USE SECONDARY ROLES ALL`: your session silently uses the privileges of *every* role you have, including ACCOUNTADMIN. That is why "it works for me but not for the analyst". To test like a real user, always run `USE SECONDARY ROLES NONE` after `USE ROLE`.

#### Error (a): object created by the wrong role

An engineer creates a silver helper table while still in ACCOUNTADMIN. Later, as HOL\_ENGINEER, they try to change it.

```sql
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE TABLE CLOSEUP_HOL2.SILVER.PROMO_CALENDAR (PROMO_MONTH DATE, PROMO_NAME VARCHAR);

USE ROLE HOL_ENGINEER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE CLOSEUP_HOL2_WH;

-- Works: DML comes from the FUTURE grant on DR_SILVER_RW
INSERT INTO CLOSEUP_HOL2.SILVER.PROMO_CALENDAR VALUES (DATE_TRUNC('month', CURRENT_DATE()), 'Winter campaign');

-- Fails: DDL requires OWNERSHIP, and the owner is ACCOUNTADMIN
ALTER TABLE CLOSEUP_HOL2.SILVER.PROMO_CALENDAR ADD COLUMN DISCOUNT_PCT NUMBER(5,2);
```

Expected error: `SQL access control error: Insufficient privileges to operate on table 'PROMO_CALENDAR'.`

**Diagnose:** `SHOW GRANTS ON TABLE ...` shows `OWNERSHIP` held by ACCOUNTADMIN. **Fix:** transfer ownership to the functional role that maintains it, keeping existing grants. **Prevent:** run builds (CI/CD, dbt, DCM) under the owning role, never under ACCOUNTADMIN.

```sql
USE ROLE ACCOUNTADMIN;
SHOW GRANTS ON TABLE CLOSEUP_HOL2.SILVER.PROMO_CALENDAR;

GRANT OWNERSHIP ON TABLE CLOSEUP_HOL2.SILVER.PROMO_CALENDAR TO ROLE HOL_ENGINEER COPY CURRENT GRANTS;

USE ROLE HOL_ENGINEER;
USE SECONDARY ROLES NONE;
ALTER TABLE CLOSEUP_HOL2.SILVER.PROMO_CALENDAR ADD COLUMN DISCOUNT_PCT NUMBER(5,2);   -- now succeeds
```

#### Error (b): missing USAGE on the warehouse (or database / schema)

HOL\_ANALYST can SELECT from GOLD, but nobody gave it compute.

```sql
USE ROLE HOL_ANALYST;
USE SECONDARY ROLES NONE;

-- Fails: the role cannot see the warehouse at all
USE WAREHOUSE CLOSEUP_HOL2_WH;

-- Also fails: HOL_ANALYST has no USAGE on schema SILVER
SELECT COUNT(*) FROM CLOSEUP_HOL2.SILVER.DT_SELL_OUT_CLEAN;
```

Expected error: `SQL compilation error: Object does not exist, or operation cannot be performed.` (If the warehouse was already selected in your session before you switched roles, the USE may be a no-op; open a fresh worksheet to reproduce it cleanly.) The same family of messages appears when USAGE on the *database* or *schema* is missing, for example HOL\_ANALYST querying SILVER: `Schema 'CLOSEUP_HOL2.SILVER' does not exist or not authorized. Your primary role HOL_ANALYST must have USAGE or any other privilege granted on SCHEMA CLOSEUP_HOL2.SILVER.` Snowflake deliberately does not reveal whether an object exists to a role that cannot use it, but it does tell you which *primary role* it checked: Snowflake deliberately does not reveal whether an object exists to a role that cannot use it. **Rule of thumb:** "does not exist" when you are sure it exists = check USAGE on the whole path: warehouse, database, schema, object.

```sql
USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE CLOSEUP_HOL2_WH TO ROLE HOL_ANALYST;

USE ROLE HOL_ANALYST;
USE SECONDARY ROLES NONE;
USE WAREHOUSE CLOSEUP_HOL2_WH;
SELECT COUNT(*) AS GOLD_ROWS FROM CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY;   -- now succeeds
```

#### Error (c): a new object type with no future grant

A new gold *view* is created. DR\_GOLD\_READ has future grants on TABLES and DYNAMIC TABLES, but not on VIEWS.

```sql
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE VIEW CLOSEUP_HOL2.GOLD.V_TOP_LABS AS
SELECT COUNTRY, LAB_NAME, SUM(SALES_USD) AS SALES_USD
FROM CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY
GROUP BY 1, 2;

USE ROLE HOL_ANALYST;
USE SECONDARY ROLES NONE;
USE WAREHOUSE CLOSEUP_HOL2_WH;

-- Fails: no SELECT on this view
SELECT * FROM CLOSEUP_HOL2.GOLD.V_TOP_LABS LIMIT 5;
```

Expected error: `SQL compilation error: Object 'CLOSEUP_HOL2.GOLD.V_TOP_LABS' does not exist or not authorized. Your primary role HOL_ANALYST must have at least one privilege granted on TABLE CLOSEUP_HOL2.GOLD.V_TOP_LABS.` **Fix:** grant on the existing views *and* add the future grant so it never happens again. Because GOLD is a managed access schema, only the schema owner or a role with MANAGE GRANTS can do this, which is exactly the control we want.

```sql
USE ROLE ACCOUNTADMIN;
SHOW FUTURE GRANTS IN SCHEMA CLOSEUP_HOL2.GOLD;     -- TABLE and DYNAMIC_TABLE only: VIEW is missing

GRANT SELECT ON ALL VIEWS    IN SCHEMA CLOSEUP_HOL2.GOLD TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CLOSEUP_HOL2.GOLD TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;

USE ROLE HOL_ANALYST;
USE SECONDARY ROLES NONE;
SELECT * FROM CLOSEUP_HOL2.GOLD.V_TOP_LABS ORDER BY SALES_USD DESC LIMIT 5;   -- now succeeds
```

### 6.5 Diagnosis toolkit

| Question | Command |
| --- | --- |
| Who am I right now? | `SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_SECONDARY_ROLES()` |
| Is role X active in my session (incl. inherited)? | `SELECT IS_ROLE_IN_SESSION('HOL_ANALYST')` |
| What can a role do? | `SHOW GRANTS TO ROLE HOL_ANALYST` / `SHOW GRANTS TO DATABASE ROLE ...` |
| Who can touch an object, and who owns it? | `SHOW GRANTS ON TABLE ...` |
| What will new objects get? | `SHOW FUTURE GRANTS IN SCHEMA ...` |
| Which roles does a user have? | `SHOW GRANTS TO USER ...` |
| Who inherits a role? | `SHOW GRANTS OF ROLE ...` |

```sql
USE ROLE ACCOUNTADMIN;
USE SECONDARY ROLES ALL;
SET me = CURRENT_USER();

SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_SECONDARY_ROLES(),
       IS_ROLE_IN_SESSION('HOL_ANALYST') AS ANALYST_IN_SESSION;

SHOW GRANTS TO ROLE HOL_ANALYST;
SHOW GRANTS TO DATABASE ROLE CLOSEUP_HOL2.DR_GOLD_READ;
SHOW GRANTS ON TABLE CLOSEUP_HOL2.SILVER.PROMO_CALENDAR;
SHOW FUTURE GRANTS IN SCHEMA CLOSEUP_HOL2.GOLD;
SHOW GRANTS OF ROLE HOL_ENGINEER;
SHOW GRANTS TO USER IDENTIFIER($me);
```

### 6.6 Grants for the agent end-user role

Cortex Analyst (and therefore the agent in Step 8) generates SQL and runs it **with the caller's role**. Whatever AGENT\_USER\_ROLE cannot read, the agent cannot answer, and whatever it can read can end up in an answer. It needs: USAGE on the warehouse, database and schemas; SELECT on every table the semantic view uses; SELECT on the semantic view itself (granted in Step 7); USAGE on the Cortex Search service and the agent (Step 8); and the `SNOWFLAKE.CORTEX_USER` database role.

```sql
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.RAW      TO ROLE AGENT_USER_ROLE;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.SILVER   TO ROLE AGENT_USER_ROLE;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.SEMANTIC TO ROLE AGENT_USER_ROLE;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.APPS     TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON TABLE CLOSEUP_HOL2.RAW.PRODUCTS     TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON TABLE CLOSEUP_HOL2.RAW.LABORATORIES TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON TABLE CLOSEUP_HOL2.RAW.PHARMACIES   TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON DYNAMIC TABLE CLOSEUP_HOL2.SILVER.DT_SELL_OUT_CLEAN TO ROLE AGENT_USER_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AGENT_USER_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE HOL_ANALYST;
```

### 6.7 Column masking on doctor names Enterprise+

A masking policy is a schema-level object attached to a column; it is evaluated at query time for every role, view, DT and agent that reads the column. Engineers see real names; analysts see initials; any query issued by an AI agent sees a redacted value, using the `IS_AGENT_ACTIVATED` context function.

```sql
USE ROLE ACCOUNTADMIN;
CREATE SCHEMA IF NOT EXISTS CLOSEUP_HOL2.GOVERNANCE;

CREATE OR REPLACE MASKING POLICY CLOSEUP_HOL2.GOVERNANCE.MP_PERSON_NAME
  AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN SYS_CONTEXT('SNOWFLAKE$CURRENT', 'IS_AGENT_ACTIVATED')::BOOLEAN THEN '*** REDACTED ***'
    WHEN IS_ROLE_IN_SESSION('HOL_ENGINEER') OR CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
    WHEN IS_ROLE_IN_SESSION('HOL_ANALYST') THEN REGEXP_REPLACE(val, '(\\w)\\w*', '\\1.')
    ELSE '*** MASKED ***'
  END
  COMMENT = 'PII: full name for engineers, initials for analysts, redacted for agents/others';

ALTER TABLE CLOSEUP_HOL2.RAW.DOCTORS MODIFY COLUMN DOCTOR_NAME
  SET MASKING POLICY CLOSEUP_HOL2.GOVERNANCE.MP_PERSON_NAME;

GRANT SELECT ON TABLE CLOSEUP_HOL2.RAW.DOCTORS TO ROLE HOL_ANALYST;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.RAW TO ROLE HOL_ANALYST;

-- Compare what each role sees
USE ROLE HOL_ENGINEER; USE SECONDARY ROLES NONE;
SELECT DOCTOR_ID, DOCTOR_NAME, SPECIALTY FROM CLOSEUP_HOL2.RAW.DOCTORS ORDER BY DOCTOR_ID LIMIT 3;

USE ROLE HOL_ANALYST; USE SECONDARY ROLES NONE;
SELECT DOCTOR_ID, DOCTOR_NAME, SPECIALTY FROM CLOSEUP_HOL2.RAW.DOCTORS ORDER BY DOCTOR_ID LIMIT 3;
```

> **Running this from Cortex Code?**  Then every role sees `*** REDACTED ***`: Cortex Code is an agent, so `IS_AGENT_ACTIVATED` is TRUE for its queries. Run the comparison in a Snowsight worksheet to see full names (engineer) vs initials (analyst). That is the policy doing exactly its job.

> **IS\_ROLE\_IN\_SESSION vs CURRENT\_ROLE:**  `CURRENT_ROLE()` only matches the primary role name; `IS_ROLE_IN_SESSION()` honors the role hierarchy and secondary roles, so a future HOL\_SENIOR\_ENGINEER that inherits HOL\_ENGINEER keeps working without touching the policy. Prefer it in policies.

### 6.8 Row access policy by country Enterprise+

A mapping table says which role sees which countries. HOL\_ANALYST sees Argentina only. The policy body reads the mapping table with the policy owner's rights, so analysts do not need SELECT on it.

> **Real-world trap we hit while building this lab:**  the first version matched the map with `IS_ROLE_IN_SESSION(m.ROLE_NAME)`. Later, AGENT\_USER\_ROLE was granted to HOL\_ANALYST (so analysts can use the app in Step 10). Through inheritance the analyst suddenly "was" AGENT\_USER\_ROLE too, and saw all four countries. With row-level security, inheritance *adds* visibility. Matching on `CURRENT_ROLE()` (the primary role) makes the mapping explicit and predictable; use `IS_ROLE_IN_SESSION` only when you want inheritance to widen access. Check with `SHOW GRANTS OF ROLE AGENT_USER_ROLE`.

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE TABLE CLOSEUP_HOL2.SEMANTIC.ROLE_COUNTRY_MAP (ROLE_NAME VARCHAR, COUNTRY VARCHAR);
INSERT INTO CLOSEUP_HOL2.SEMANTIC.ROLE_COUNTRY_MAP VALUES
  ('HOL_ANALYST',     'Argentina'),
  ('AGENT_USER_ROLE', 'Argentina'), ('AGENT_USER_ROLE', 'Brazil'),
  ('AGENT_USER_ROLE', 'Uruguay'),   ('AGENT_USER_ROLE', 'Chile');

CREATE OR REPLACE ROW ACCESS POLICY CLOSEUP_HOL2.GOVERNANCE.RAP_COUNTRY
  AS (country_val VARCHAR) RETURNS BOOLEAN ->
  IS_ROLE_IN_SESSION('HOL_ENGINEER')
  OR CURRENT_ROLE() = 'ACCOUNTADMIN'
  OR EXISTS (
      SELECT 1 FROM CLOSEUP_HOL2.SEMANTIC.ROLE_COUNTRY_MAP m
      WHERE m.COUNTRY   = country_val
        AND m.ROLE_NAME = CURRENT_ROLE()          -- primary role: explicit, no surprise inheritance
  )
  COMMENT = 'Row-level security by COUNTRY using SEMANTIC.ROLE_COUNTRY_MAP (primary role)';

ALTER DYNAMIC TABLE CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY
  ADD ROW ACCESS POLICY CLOSEUP_HOL2.GOVERNANCE.RAP_COUNTRY ON (COUNTRY);

USE ROLE HOL_ANALYST; USE SECONDARY ROLES NONE; USE WAREHOUSE CLOSEUP_HOL2_WH;
SELECT COUNTRY, COUNT(*) AS ROWS_ FROM CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY GROUP BY 1;   -- Argentina only

USE ROLE HOL_ENGINEER; USE SECONDARY ROLES NONE;
SELECT COUNTRY, COUNT(*) AS ROWS_ FROM CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY GROUP BY 1;   -- all 4 countries
```

> **Fail closed.**  A role not in the mapping table sees zero rows, not all rows. Policies apply everywhere the data is read: views on top of the DT, semantic views, Cortex Analyst and the agent all inherit the filter automatically. Note that a Cortex Search service runs with its *owner's* rights, so row policies do not filter search results; use search filters or per-tenant services for that.

### 6.9 Standard Edition alternative: secure view

Masking and row access policies require Enterprise Edition. On Standard Edition you get the same row filter with a **secure view** (its definition is hidden from consumers and the optimizer will not push predicates that could leak filtered rows) and you grant the view instead of the table.

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE SECURE VIEW CLOSEUP_HOL2.GOLD.V_MARKET_SHARE_SECURE
  COMMENT = 'Standard-Edition pattern: row filter via secure view + ROLE_COUNTRY_MAP'
AS
SELECT ms.*
FROM CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY ms
WHERE IS_ROLE_IN_SESSION('HOL_ENGINEER')
   OR EXISTS (SELECT 1 FROM CLOSEUP_HOL2.SEMANTIC.ROLE_COUNTRY_MAP m
              WHERE m.COUNTRY = ms.COUNTRY AND m.ROLE_NAME = CURRENT_ROLE());
-- (The view is covered automatically by the FUTURE VIEWS grant added in error (c).)

USE ROLE HOL_ANALYST; USE SECONDARY ROLES NONE;
SELECT COUNTRY, COUNT(*) AS ROWS_ FROM CLOSEUP_HOL2.GOLD.V_MARKET_SHARE_SECURE GROUP BY 1;
```

In a real Standard Edition design you would *not* grant SELECT on the underlying DT to analysts, only on the secure view. Here the analyst also has the DT through DR\_GOLD\_READ, which is fine because the row access policy already protects it.

### Verification

```sql
USE ROLE ACCOUNTADMIN;
USE SECONDARY ROLES ALL;

SELECT POLICY_NAME, POLICY_KIND, REF_ENTITY_NAME, REF_COLUMN_NAME
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.POLICY_REFERENCES(POLICY_NAME => 'CLOSEUP_HOL2.GOVERNANCE.MP_PERSON_NAME'))
UNION ALL
SELECT POLICY_NAME, POLICY_KIND, REF_ENTITY_NAME, REF_COLUMN_NAME
FROM TABLE(CLOSEUP_HOL2.INFORMATION_SCHEMA.POLICY_REFERENCES(POLICY_NAME => 'CLOSEUP_HOL2.GOVERNANCE.RAP_COUNTRY'));

SHOW GRANTS TO ROLE AGENT_USER_ROLE;
SELECT CURRENT_ROLE();   -- back to ACCOUNTADMIN for the next steps
```

### Recap: what you built in Step 6

- **You built:** the functional roles `HOL_ENGINEER`, `HOL_ANALYST` and `AGENT_USER_ROLE` under `SYSADMIN`; the access roles `DR_GOLD_READ` and `DR_SILVER_RW` as database roles with future grants; the masking policy `GOVERNANCE.MP_PERSON_NAME` on doctor names; the row access policy `GOVERNANCE.RAP_COUNTRY` with `SEMANTIC.ROLE_COUNTRY_MAP`; and the Standard Edition alternative `GOLD.V_MARKET_SHARE_SECURE`.
- **The model:** users get functional roles; functional roles get access roles; access roles hold the privileges on objects. Future grants cover tables that do not exist yet.
- **You reproduced and fixed** the three classic privilege errors and used the diagnosis toolkit (`SHOW GRANTS`, current and secondary roles).
- **Take it to your projects:** never grant privileges directly to people-facing roles, always create objects with the owning role, and put masking and row access policies on the data so they apply to BI, APIs and agents alike.

## Challenge Block 3 — AI (30 min · steps 7–8)

**Mission:** Expose the GOLD layer to business users: semantic view, search service and a Cortex Agent with a dedicated role, all in SQL.

> **Checkpoint ✓**  The agent answers one quantitative question (sales/share) and one qualitative question (field notes) correctly. When your team reaches the checkpoint, raise your hand (or post in the chat)
> — the first team gets the point for this block.

---

## Step 7: Semantic View via SQL (~10 min)

**What this step does:** defines the business metrics once, in a semantic view, so the agent and SQL users get the same answer.

```text
GOLD + RAW tables (the data) ──► SEMANTIC.SV_PHARMA_MARKET (tables · joins · metrics · synonyms) ──► SEMANTIC_VIEW() in SQL (7.3)
SEMANTIC.SV_PHARMA_MARKET ──► Agent (Step 8) (text-to-SQL)
```

### Why a semantic view?

A **semantic view** is a schema-level object that captures business meaning on top of physical tables: logical tables, the relationships between them, *facts* (row-level numeric values), *dimensions* (attributes to group and filter by) and *metrics* (aggregations). Cortex Analyst uses it to translate natural-language questions into correct SQL, and any SQL client can query it with the `SEMANTIC_VIEW()` table function. Synonyms and comments are the context the LLM reads, so treat them as product documentation.

> **Model scope** 
>
> We expose the GOLD monthly market-share dynamic table (pre-aggregated, fast) plus the SILVER sell-out grain joined to products, laboratories and pharmacies for drill-downs by brand, molecule, chain or channel.

### 7.1 Create the semantic view

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE CLOSEUP_HOL2_WH;
USE SCHEMA CLOSEUP_HOL2.SEMANTIC;

CREATE OR REPLACE SEMANTIC VIEW CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET
  TABLES (
    market_share AS CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY
      WITH SYNONYMS = ('monthly market share', 'market audit')
      COMMENT = 'Monthly pharma market share by country, therapeutic area and laboratory (pre-aggregated, GOLD layer)',
    sales AS CLOSEUP_HOL2.SILVER.DT_SELL_OUT_CLEAN
      PRIMARY KEY (SALE_ID)
      WITH SYNONYMS = ('sell out', 'pharmacy sales', 'transactions')
      COMMENT = 'Cleaned, deduplicated pharmacy sell-out transactions (one row per sale)',
    products AS CLOSEUP_HOL2.RAW.PRODUCTS
      PRIMARY KEY (PRODUCT_ID)
      WITH SYNONYMS = ('brands', 'drugs', 'SKUs')
      COMMENT = 'Product catalog: brand, molecule, ATC class, therapeutic area',
    laboratories AS CLOSEUP_HOL2.RAW.LABORATORIES
      PRIMARY KEY (LAB_ID)
      WITH SYNONYMS = ('labs', 'manufacturers', 'pharma companies')
      COMMENT = 'Pharmaceutical laboratories (manufacturers)',
    pharmacies AS CLOSEUP_HOL2.RAW.PHARMACIES
      PRIMARY KEY (PHARMACY_ID)
      WITH SYNONYMS = ('drugstores', 'points of sale', 'outlets')
      COMMENT = 'Pharmacies with chain, city, region, country and channel'
  )
  RELATIONSHIPS (
    sales_to_products     AS sales (PRODUCT_ID)        REFERENCES products,
    sales_to_pharmacies   AS sales (PHARMACY_ID)       REFERENCES pharmacies,
    products_to_labs      AS products (LAB_ID)         REFERENCES laboratories,
    market_share_to_labs  AS market_share (LAB_ID)     REFERENCES laboratories
  )
  FACTS (
    market_share.ms_units      AS UNITS            COMMENT = 'Units sold in the month for the lab / country / therapeutic area',
    market_share.ms_sales_usd  AS SALES_USD        COMMENT = 'Sales in USD in the month for the lab / country / therapeutic area',
    market_share.ms_share_pct  AS MARKET_SHARE_PCT COMMENT = 'Lab share (%) of the therapeutic-area sales in that country and month',
    sales.sale_units           AS UNITS            COMMENT = 'Units in a single sell-out transaction',
    sales.sale_amount_usd      AS NET_AMOUNT_USD   COMMENT = 'Net amount in USD of a single sell-out transaction',
    products.list_price_usd    AS LIST_PRICE_USD   COMMENT = 'Current list price in USD'
  )
  DIMENSIONS (
    market_share.month AS MONTH
      WITH SYNONYMS = ('period', 'month of sale')
      COMMENT = 'First day of the month',
    market_share.country AS COUNTRY
      WITH SYNONYMS = ('market', 'nation')
      COMMENT = 'Argentina, Brazil, Uruguay or Chile',
    market_share.therapeutic_area AS THERAPEUTIC_AREA
      WITH SYNONYMS = ('TA', 'therapy area', 'category')
      COMMENT = 'Cardiology, Diabetes, Oncology, Respiratory, CNS, Gastroenterology, Dermatology, Analgesics',
    sales.sale_date AS SALE_DATE
      WITH SYNONYMS = ('transaction date', 'date')
      COMMENT = 'Date of the sell-out transaction',
    sales.sale_month AS SALE_MONTH
      COMMENT = 'Month of the sell-out transaction',
    sales.promo_flag AS PROMO_FLAG
      WITH SYNONYMS = ('promotion', 'on promo')
      COMMENT = 'TRUE when the sale was under a promotion (only populated for newer batches)',
    products.brand_name AS BRAND_NAME
      WITH SYNONYMS = ('brand', 'product name')
      COMMENT = 'Commercial brand name',
    products.molecule AS MOLECULE
      WITH SYNONYMS = ('active ingredient', 'API', 'generic name')
      COMMENT = 'Active molecule',
    products.atc_class AS ATC_CLASS
      COMMENT = 'Anatomical Therapeutic Chemical classification code',
    products.product_therapeutic_area AS THERAPEUTIC_AREA
      COMMENT = 'Therapeutic area of the product',
    laboratories.lab_name AS LAB_NAME
      WITH SYNONYMS = ('laboratory', 'manufacturer', 'company')
      COMMENT = 'Laboratory name',
    laboratories.hq_country AS HQ_COUNTRY
      COMMENT = 'Country where the laboratory is headquartered',
    laboratories.is_closeup_client AS IS_CLOSEUP_CLIENT
      WITH SYNONYMS = ('client', 'customer lab')
      COMMENT = 'TRUE if the laboratory is a Close-Up client',
    pharmacies.chain AS CHAIN
      WITH SYNONYMS = ('pharmacy chain', 'banner')
      COMMENT = 'Pharmacy chain name',
    pharmacies.channel AS CHANNEL
      COMMENT = 'Retail Chain, Independent or Hospital',
    pharmacies.city AS CITY
      COMMENT = 'Pharmacy city',
    pharmacies.region AS REGION
      COMMENT = 'Pharmacy region / state / province',
    pharmacies.pharmacy_country AS COUNTRY
      COMMENT = 'Country of the pharmacy'
  )
  METRICS (
    market_share.total_market_sales_usd AS SUM(market_share.ms_sales_usd)
      WITH SYNONYMS = ('market value', 'revenue', 'sales')
      COMMENT = 'Total market sales in USD (from the monthly market-share table)',
    market_share.total_market_units AS SUM(market_share.ms_units)
      WITH SYNONYMS = ('volume', 'units')
      COMMENT = 'Total market units (from the monthly market-share table)',
    market_share.avg_market_share_pct AS AVG(market_share.ms_share_pct)
      WITH SYNONYMS = ('share', 'market share')
      COMMENT = 'Average monthly market share %. For one lab/country/TA/month it equals the share; across months it is the average share',
    sales.sellout_amount_usd AS SUM(sales.sale_amount_usd)
      WITH SYNONYMS = ('sell-out revenue', 'net sales')
      COMMENT = 'Net sell-out amount in USD at transaction grain',
    sales.sellout_units AS SUM(sales.sale_units)
      COMMENT = 'Sell-out units at transaction grain',
    sales.transaction_count AS COUNT(sales.SALE_ID)
      WITH SYNONYMS = ('number of sales', 'tickets')
      COMMENT = 'Number of sell-out transactions',
    sales.avg_ticket_usd AS AVG(sales.sale_amount_usd)
      COMMENT = 'Average net amount per transaction in USD'
  )
  COMMENT = 'Close-Up pharma market intelligence (LATAM): market share, sell-out, products, labs and pharmacies. Synthetic data.';
```

> **Reading the DDL** 
>
> `TABLES` declares logical tables and primary keys, `RELATIONSHIPS` declares the many-to-one joins (foreign key on the left, referenced table on the right), `FACTS` and `DIMENSIONS` map columns or expressions to business names, and `METRICS` define aggregations over facts.

### 7.2 Inspect it

```sql
SHOW SEMANTIC VIEWS IN SCHEMA CLOSEUP_HOL2.SEMANTIC;
DESCRIBE SEMANTIC VIEW CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET;
SHOW SEMANTIC METRICS IN CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET;
SELECT GET_DDL('SEMANTIC_VIEW', 'CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET');
```

### 7.3 Query it with SEMANTIC\_VIEW()

You ask for metrics and dimensions; Snowflake generates the joins and the GROUP BY for you. Dimensions and metrics must come from logical tables that are related.

```sql
-- Market value and average share by country and therapeutic area
SELECT * FROM SEMANTIC_VIEW(
    CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET
    DIMENSIONS market_share.country, market_share.therapeutic_area
    METRICS market_share.total_market_sales_usd, market_share.avg_market_share_pct
)
ORDER BY total_market_sales_usd DESC
LIMIT 10;

-- Top laboratories in Cardiology (relationship market_share -> laboratories)
SELECT * FROM SEMANTIC_VIEW(
    CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET
    DIMENSIONS laboratories.lab_name, laboratories.is_closeup_client
    METRICS market_share.total_market_sales_usd
    WHERE market_share.therapeutic_area = 'Cardiology'
)
ORDER BY total_market_sales_usd DESC
LIMIT 5;

-- Transaction grain: sell-out by channel and brand (sales -> pharmacies, products)
SELECT * FROM SEMANTIC_VIEW(
    CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET
    DIMENSIONS pharmacies.channel, products.brand_name
    METRICS sales.sellout_amount_usd, sales.transaction_count
)
ORDER BY sellout_amount_usd DESC
LIMIT 10;
```

### 7.4 Grants for consumers

Querying a semantic view requires `SELECT` on the semantic view **and** on the underlying tables and dynamic tables, plus USAGE on the database, schemas and warehouse. Cortex Analyst executes the generated SQL with the caller's role, so missing grants on base objects surface later as agent errors.

```sql
USE ROLE ACCOUNTADMIN;
-- Semantic view
GRANT USAGE ON DATABASE CLOSEUP_HOL2 TO ROLE AGENT_USER_ROLE;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.SEMANTIC TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON SEMANTIC VIEW CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON SEMANTIC VIEW CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET TO ROLE HOL_ANALYST;
GRANT USAGE ON WAREHOUSE CLOSEUP_HOL2_WH TO ROLE AGENT_USER_ROLE;

-- Underlying objects (queries run with the caller's role)
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.GOLD   TO ROLE AGENT_USER_ROLE;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.SILVER TO ROLE AGENT_USER_ROLE;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.RAW    TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON DYNAMIC TABLE CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON DYNAMIC TABLE CLOSEUP_HOL2.SILVER.DT_SELL_OUT_CLEAN     TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON TABLE CLOSEUP_HOL2.RAW.PRODUCTS     TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON TABLE CLOSEUP_HOL2.RAW.LABORATORIES TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON TABLE CLOSEUP_HOL2.RAW.PHARMACIES   TO ROLE AGENT_USER_ROLE;

-- HOL_ANALYST inherits AGENT_USER_ROLE's access (hierarchy) plus its own grants from step 6
GRANT ROLE AGENT_USER_ROLE TO ROLE HOL_ANALYST;
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.SEMANTIC TO ROLE HOL_ANALYST;
```

### Best practices

- **Naming:** business-friendly logical names (`market_share`, `sales`), an `SV_` prefix for the object, snake\_case for dimensions and metrics.
- **Descriptions everywhere:** every table, dimension, fact and metric gets a `COMMENT`; add `WITH SYNONYMS` for the vocabulary your users really use ("lab", "manufacturer", "revenue").
- **Keep it narrow:** one semantic view per business domain. A few well-described columns beat hundreds of raw ones for accuracy and latency.
- **Pre-aggregate:** point heavy questions at GOLD tables; use fine-grain tables only for drill-downs.
- **Verified queries:** in Snowsight (*AI & ML > Cortex Analyst > SV\_PHARMA\_MARKET*) add verified question/SQL pairs for your most frequent questions and mark some as onboarding questions; Analyst reuses them as examples.
- **Version as code:** keep the DDL in Git, retrieve it with `GET_DDL('SEMANTIC_VIEW', ...)` and review changes with `DESCRIBE SEMANTIC VIEW`.

### Verification

```sql
USE ROLE AGENT_USER_ROLE;
USE WAREHOUSE CLOSEUP_HOL2_WH;
SELECT * FROM SEMANTIC_VIEW(
    CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET
    DIMENSIONS market_share.country
    METRICS market_share.total_market_sales_usd, market_share.total_market_units
)
ORDER BY total_market_sales_usd DESC;
USE ROLE ACCOUNTADMIN;
```

---

### Recap: what you built in Step 7

- **You built:** the semantic view `SEMANTIC.SV_PHARMA_MARKET` in SQL, queried it with `SEMANTIC_VIEW()`, and granted it to the consumer roles.
- **Key idea:** one governed definition of the business metrics, shared by the agent and by SQL or BI consumers. Without a grant on the semantic view, a role cannot ask the agent about it.
- **Take it to your projects:** keep the semantic view in version control next to your pipeline code and review metric changes like code changes.

## Step 8: Cortex Search + Cortex Agent via SQL (~20 min)

**What this step does:** enriches the field notes with AI, makes them searchable, and creates the agent as code (YAML), granted to the end-user role.

```text
SV_PHARMA_MARKET (semantic view) ──► market_analytics (tool: numbers)
FIELD_NOTES_ENRICHED (AI functions) ──► CS_FIELD_NOTES (search service) ──► field_intelligence (tool: field notes)
AGENT_USER_ROLE (end-user role) ──► APPS.PHARMA_MARKET_AGENT (YAML spec · picks the tool(s)) ──► Answer (numbers + context)
```

### 8.1 Enrich field notes with AI functions

We classify each note into a category and extract its sentiment so the search service can filter on them. Remember: `AI_SENTIMENT` returns an OBJECT (read `categories[0]:sentiment`) and `AI_CLASSIFY` returns `labels`.

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE CLOSEUP_HOL2_WH;
USE SCHEMA CLOSEUP_HOL2.SEMANTIC;

CREATE OR REPLACE TABLE CLOSEUP_HOL2.SEMANTIC.FIELD_NOTES_ENRICHED AS
SELECT
    n.NOTE_ID,
    n.NOTE_DATE,
    n.PRODUCT_ID,
    p.BRAND_NAME,
    p.THERAPEUTIC_AREA,
    l.LAB_NAME,
    n.COUNTRY,
    n.AUTHOR_ROLE,
    n.NOTE_TEXT,
    AI_CLASSIFY(
        COALESCE(n.NOTE_TEXT, ''),
        ['Competitor Activity', 'Supply / Stock-out', 'Pricing', 'Doctor Feedback', 'Regulatory']
    ):labels[0]::VARCHAR                              AS CATEGORY,
    AI_SENTIMENT(COALESCE(n.NOTE_TEXT, '')):categories[0]:sentiment::VARCHAR AS SENTIMENT
FROM CLOSEUP_HOL2.RAW.FIELD_NOTES n
LEFT JOIN CLOSEUP_HOL2.RAW.PRODUCTS p     ON p.PRODUCT_ID = n.PRODUCT_ID
LEFT JOIN CLOSEUP_HOL2.RAW.LABORATORIES l ON l.LAB_ID = p.LAB_ID;

SELECT CATEGORY, SENTIMENT, COUNT(*) AS NOTES
FROM CLOSEUP_HOL2.SEMANTIC.FIELD_NOTES_ENRICHED
GROUP BY 1, 2 ORDER BY 1, 2;
```

### 8.2 Cortex Search service

Cortex Search provides hybrid (vector + keyword) retrieval over text with managed embeddings and incremental refresh. Attributes enable filters such as `COUNTRY = 'Brazil'` or `CATEGORY = 'Pricing'`.

```sql
ALTER TABLE CLOSEUP_HOL2.SEMANTIC.FIELD_NOTES_ENRICHED SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE CORTEX SEARCH SERVICE CLOSEUP_HOL2.SEMANTIC.CS_FIELD_NOTES
  ON NOTE_TEXT
  ATTRIBUTES COUNTRY, CATEGORY, SENTIMENT, THERAPEUTIC_AREA, BRAND_NAME, LAB_NAME
  WAREHOUSE = CLOSEUP_HOL2_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Field-force notes from LATAM reps and KAMs, enriched with category and sentiment'
AS
SELECT NOTE_ID, NOTE_DATE::VARCHAR AS NOTE_DATE, NOTE_TEXT, COUNTRY, CATEGORY, SENTIMENT,
       THERAPEUTIC_AREA, BRAND_NAME, LAB_NAME, AUTHOR_ROLE
FROM CLOSEUP_HOL2.SEMANTIC.FIELD_NOTES_ENRICHED;

-- Test retrieval with a filter
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'CLOSEUP_HOL2.SEMANTIC.CS_FIELD_NOTES',
    '{"query": "stock-out shortage pharmacies", "columns": ["NOTE_ID","NOTE_TEXT","COUNTRY","CATEGORY"], "filter": {"@eq": {"COUNTRY": "Argentina"}}, "limit": 3}'
  )
):results AS RESULTS;
```

### 8.3 Create the agent from a YAML specification

The agent is a first-class schema object. The specification defines the orchestration model, instructions, tools and tool resources. Tool **descriptions** are the most important routing signal for the orchestrator: say what data the tool covers, when to use it and when *not* to.

```sql
CREATE OR REPLACE AGENT CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT
  COMMENT = 'Close-Up pharma market intelligence agent for LATAM (synthetic data)'
  PROFILE = '{"display_name": "Pharma Market Agent", "color": "blue"}'
  FROM SPECIFICATION
$$
models:
  orchestration: auto

orchestration:
  budget:
    seconds: 60
    tokens: 16000

instructions:
  system: >
    You are a pharma market intelligence analyst for Close-Up International covering
    Argentina, Brazil, Uruguay and Chile. You answer questions about market share,
    sell-out sales, laboratories, brands, pharmacies and qualitative field reports.
    All data is synthetic.
  orchestration: >
    Use market_analytics for any quantitative question (sales, units, market share,
    rankings, trends, comparisons by month, country, therapeutic area, laboratory,
    brand, molecule, chain or channel).
    Use field_intelligence for qualitative questions about what field reps observe
    (competitor launches, stock-outs, pricing changes, doctor feedback, regulatory news),
    filtering by COUNTRY, CATEGORY or THERAPEUTIC_AREA when the user mentions them.
    For "why" questions, first quantify the change with market_analytics, then look for
    explanations with field_intelligence and combine both.
    If no time window is given, use the last 12 months and say so.
    If a question is outside pharma market intelligence for these four countries,
    politely say it is out of scope instead of guessing.
  response: >
    Be concise and business-oriented. Start with the direct answer in one or two sentences,
    then supporting details. Format money in USD with thousands separators and shares as
    percentages with one decimal. Use a table for rankings or comparisons of more than three
    items and a line chart for trends over time. When citing field notes, reference their
    NOTE_ID. State the time window and filters used. Never invent numbers.
  sample_questions:
    - question: "Who are the top 5 laboratories by market share in Cardiology in Brazil over the last 6 months?"
      answer: "I will rank laboratories by average market share in Cardiology in Brazil for the last 6 months."
    - question: "How did Diabetes sales evolve month by month in Argentina?"
      answer: "I will show monthly Diabetes sales in USD for Argentina as a trend."
    - question: "What are field reps saying about stock-outs in Chile?"
      answer: "I will search the field notes about supply and stock-outs in Chile and summarize them."
    - question: "Why did Oncology market share change in Uruguay, and what do field notes say?"
      answer: "I will quantify the change and then look for explanations in field notes."

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: market_analytics
      description: >
        Generates and runs SQL over the SV_PHARMA_MARKET semantic view. Covers monthly market
        share, sales in USD and units by country (Argentina, Brazil, Uruguay, Chile),
        therapeutic area (Cardiology, Diabetes, Oncology, Respiratory, CNS, Gastroenterology,
        Dermatology, Analgesics) and laboratory, plus transaction-level pharmacy sell-out by
        date, brand, molecule, ATC class, pharmacy chain, city, region and channel
        (Retail Chain, Independent, Hospital), and promotion flag. Use it for numbers, rankings,
        trends and comparisons. Do not use it for qualitative opinions or field observations.
  - tool_spec:
      type: cortex_search
      name: field_intelligence
      description: >
        Searches unstructured field notes written by sales reps and key account managers.
        Each note has NOTE_ID, NOTE_DATE, COUNTRY, CATEGORY (Competitor Activity,
        Supply / Stock-out, Pricing, Doctor Feedback, Regulatory), SENTIMENT, THERAPEUTIC_AREA,
        BRAND_NAME and LAB_NAME. Use it to explain the why behind market movements and to
        report qualitative signals. Do not use it to compute sales or market share.

tool_resources:
  market_analytics:
    semantic_view: CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET
    execution_environment:
      type: warehouse
      warehouse: CLOSEUP_HOL2_WH
      query_timeout: 60
  field_intelligence:
    search_service: CLOSEUP_HOL2.SEMANTIC.CS_FIELD_NOTES
    id_column: NOTE_ID
    title_column: BRAND_NAME
    max_results: 5
$$;

DESCRIBE AGENT CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT;
```

> **Inspect and version** 
>
> Use `DESCRIBE AGENT` and `SHOW AGENTS` to inspect it. Keep the YAML in Git and redeploy with `CREATE OR REPLACE AGENT ... COPY GRANTS` so existing grants survive.

### 8.4 Grant access and smoke test

```sql
GRANT USAGE ON SCHEMA CLOSEUP_HOL2.APPS TO ROLE AGENT_USER_ROLE;
GRANT USAGE ON AGENT CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT TO ROLE AGENT_USER_ROLE;
GRANT USAGE ON CORTEX SEARCH SERVICE CLOSEUP_HOL2.SEMANTIC.CS_FIELD_NOTES TO ROLE AGENT_USER_ROLE;
GRANT SELECT ON TABLE CLOSEUP_HOL2.SEMANTIC.FIELD_NOTES_ENRICHED TO ROLE AGENT_USER_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AGENT_USER_ROLE;

-- Smoke test from SQL (non-streaming); returns the agent response as JSON
USE ROLE AGENT_USER_ROLE;
USE WAREHOUSE CLOSEUP_HOL2_WH;
SELECT TRY_PARSE_JSON(
  SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT',
    $${"messages":[{"role":"user","content":[{"type":"text","text":"Top 3 laboratories by sales in Cardiology in Brazil"}]}],"stream":false}$$
  )
) AS RESPONSE;
USE ROLE ACCOUNTADMIN;
```

### Best-practices recap (from "Best Practices for Building Cortex Agents")

| Area | Recommendation | In this lab |
| --- | --- | --- |
| Purpose | Narrow scope: one persona, one job, a known set of questions. | LATAM pharma market analysts; market share plus field intelligence only. |
| Tools | Few, well-separated tools with precise descriptions and explicit boundaries. | `market_analytics` (numbers) vs `field_intelligence` (text). |
| Orchestration instructions | Routing rules, multi-step plans, defaults for ambiguity, out-of-scope handling. | Quantitative to Analyst, qualitative to Search, "why" to both; default 12-month window. |
| Response instructions | Tone, format, units, length, citations, charts. | Concise, USD and %, cite NOTE\_ID, state filters and time window. |
| Sample questions | Representative questions that onboard users and seed evaluation sets. | Four sample questions in the spec. |
| Versioning | Dev, test and prod stages; spec in source control; promote the same YAML. | Spec as DDL you can deploy to CLOSEUP\_HOL2\_DEV first, then to CLOSEUP\_HOL2. |
| Evaluation and monitoring | Ground-truth question/answer set evaluated before every release; monitor traces and user feedback. | Start with the sample questions as an eval set; review traces in Snowsight agent monitoring. |

### Verification

```sql
SHOW AGENTS IN SCHEMA CLOSEUP_HOL2.APPS;
SHOW CORTEX SEARCH SERVICES IN SCHEMA CLOSEUP_HOL2.SEMANTIC;
SHOW GRANTS ON AGENT CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT;
```

### Recap: what you built in Step 8

- **You built:** `SEMANTIC.FIELD_NOTES_ENRICHED` (AI functions), the Cortex Search service `SEMANTIC.CS_FIELD_NOTES`, and the agent `APPS.PHARMA_MARKET_AGENT` defined from a YAML specification, granted to `AGENT_USER_ROLE` and smoke-tested with that role.
- **The flow so far:** GOLD tables → semantic view + search service → agent, all protected by the roles and policies from step 6.
- **Key idea:** the agent is code: instructions, tools and model live in a specification you can version and promote from dev to production.
- **Take it to your projects:** always test the agent with the end-user role, not `ACCOUNTADMIN`; if it works as admin and fails as the user, the problem is a grant.

## Block 4 — Integrations (15 min · steps 9–10 · instructor demo)

The instructor shows how applications consume the platform: the SQL API and the Cortex Agents REST API with a role-restricted PAT,
and a React chat app deployed on Snowflake App Runtime. All code is in this repository (`app/`), so you can reproduce it after the session.

---

## Step 9: Consumption APIs (SQL API & Agents REST API) (~7 min · Instructor demo)

**What this step does:** lets any back end use the platform over HTTP, with the same role-based security as a user in Snowsight.

```text
Your app / script (any language) ──► PAT (HOL2_API_USER · network policy) ──► SQL API (run SQL) ──► Data (JSON)
Your app / script ──► PAT ──► Agents REST API (ask the agent) ──► Answer (streamed (SSE))
```

> **Instructor demo.**  During the session the instructor runs this step live on screen.
> Everything you need to reproduce it later is below.

### 9.1 Programmatic Access Token (PAT)

A PAT is a revocable secret bound to a user. With `ROLE_RESTRICTION` every request made with it runs under that role only, so a leaked token can never act as ACCOUNTADMIN. The secret is returned **only once** in the `token_secret` column of the output.

Best practice for applications: issue the PAT to a dedicated **service user** (`TYPE = SERVICE`), not to your own login. Snowflake requires the user that holds a PAT to be subject to a **network policy**, so we create one that only allows your current public IP. Your own user and your Snowsight session are not affected.

**a) Find your public IP** (in a terminal) and the account URL (in a worksheet):

```bash
curl https://api.ipify.org
```

```sql
SELECT 'https://' || LOWER(CURRENT_ORGANIZATION_NAME() || '-' || REPLACE(CURRENT_ACCOUNT_NAME(), '_', '-'))
       || '.snowflakecomputing.com' AS SNOWFLAKE_ACCOUNT_URL;
```

**b) Create the service user, its network policy and the PAT.** Replace `<your_public_ip>` with the IP from (a).

```sql
USE ROLE ACCOUNTADMIN;

-- Service user for API access (no password, no interactive login)
CREATE USER IF NOT EXISTS HOL2_API_USER
  TYPE = SERVICE
  DEFAULT_ROLE = AGENT_USER_ROLE
  DEFAULT_WAREHOUSE = CLOSEUP_HOL2_WH
  COMMENT = 'Close-Up HOL2 - service user for the SQL API and Agents REST API';
GRANT ROLE AGENT_USER_ROLE TO USER HOL2_API_USER;

-- Network policy: the PAT only works from your IP
CREATE NETWORK RULE IF NOT EXISTS CLOSEUP_HOL2.APPS.NR_HOL2_ALLOWED_IPS
  MODE = INGRESS TYPE = IPV4
  VALUE_LIST = ('<your_public_ip>/32');
CREATE NETWORK POLICY IF NOT EXISTS NP_HOL2_PAT
  ALLOWED_NETWORK_RULE_LIST = ('CLOSEUP_HOL2.APPS.NR_HOL2_ALLOWED_IPS');
ALTER USER HOL2_API_USER SET NETWORK_POLICY = NP_HOL2_PAT;

-- Create the token: copy token_secret from the output, it is shown only once
ALTER USER HOL2_API_USER ADD PROGRAMMATIC ACCESS TOKEN HOL2_AGENT_PAT
  ROLE_RESTRICTION = 'AGENT_USER_ROLE'
  DAYS_TO_EXPIRY = 7
  COMMENT = 'Close-Up HOL2 - Agent and SQL API access';
```

> **Lost the secret, or your IP changed?**  Rotate the token with
> `ALTER USER HOL2_API_USER ROTATE PROGRAMMATIC ACCESS TOKEN HOL2_AGENT_PAT;` (prints a new secret), or update the IP with
> `ALTER NETWORK RULE CLOSEUP_HOL2.APPS.NR_HOL2_ALLOWED_IPS SET VALUE_LIST = ('<new_ip>/32');`.
> A `Network policy is required` or `IP ... is not allowed` error means the request came from an IP outside the rule (VPN, Wi-Fi change).

> **Never commit the token.**  Keep it in an environment variable or a secret manager.

### 9.2 Call the SQL API

The SQL API v2 runs any statement over HTTPS. Use the PAT as a bearer token and declare its type in `X-Snowflake-Authorization-Token-Type`.

> **Account URL:**  use `<orgname>-<accountname>` with **hyphens** (run `SELECT CURRENT_ORGANIZATION_NAME() || '-' || REPLACE(CURRENT_ACCOUNT_NAME(), '_', '-');`). Underscores in the hostname break TLS certificate validation in Python/Node clients.

```bash
export SNOWFLAKE_ACCOUNT_URL="https://<account_identifier>.snowflakecomputing.com"
export SNOWFLAKE_PAT="<paste_token_secret>"

curl -s -X POST "$SNOWFLAKE_ACCOUNT_URL/api/v2/statements" \
  -H "Authorization: Bearer $SNOWFLAKE_PAT" \
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "statement": "SELECT COUNTRY, SUM(SALES_USD) AS SALES_USD FROM CLOSEUP_HOL2.GOLD.DT_MARKET_SHARE_MONTHLY GROUP BY 1 ORDER BY 2 DESC",
    "timeout": 60,
    "warehouse": "CLOSEUP_HOL2_WH",
    "role": "AGENT_USER_ROLE"
  }' | jq '.data'
# Long-running statements return 202 + statementHandle:
#   GET $SNOWFLAKE_ACCOUNT_URL/api/v2/statements/<statementHandle>
```

### 9.3 Call the Cortex Agents REST API

Because the agent is an object, the request only carries the conversation; models, tools and instructions come from the specification. To continue a conversation, send the previous user and assistant messages again in `messages`.

```bash
curl -N -X POST \
  "$SNOWFLAKE_ACCOUNT_URL/api/v2/databases/CLOSEUP_HOL2/schemas/APPS/agents/PHARMA_MARKET_AGENT:run" \
  -H "Authorization: Bearer $SNOWFLAKE_PAT" \
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "messages": [
      {
        "role": "user",
        "content": [
          { "type": "text", "text": "Top 5 laboratories by market share in Cardiology in Brazil, and what do field notes say about them?" }
        ]
      }
    ]
  }'

# Sample of the stream (abridged):
# event: response.status
# data: {"status":"planning","message":"Planning the next steps"}
# event: response.tool_use
# data: {"type":"cortex_analyst_text_to_sql","name":"market_analytics","input":{...}}
# event: response.tool_result
# data: {"name":"market_analytics","content":[{"json":{"sql":"SELECT ...","result_set":{...}}}]}
# event: response.text.delta
# data: {"text":"In Brazil Cardiology, the top laboratory is ..."}
# event: response
# data: {"role":"assistant","content":[...]}
```

### Understanding the SSE stream

The response is `text/event-stream`: a sequence of `event:` / `data:` pairs. The `-N` flag disables curl buffering so you see events as they arrive.

| Event | Meaning |
| --- | --- |
| `response.status` | Progress updates (planning, executing a tool, reviewing results). |
| `response.thinking.delta` | Incremental reasoning text from the orchestrator. |
| `response.tool_use` | The agent decided to call a tool: name, type and input. |
| `response.tool_result` | Tool output: generated SQL and result set, or search results. |
| `response.text.delta` | Incremental chunks of the final answer; append them to render live. |
| `response.chart` / `response.table` | Structured chart (Vega-Lite) or table payloads. |
| `response` | Final aggregated message with all content blocks; the stream ends after it. |
| `error` | Error payload with code and message. |

### 9.4 Python client with streaming

```python
import json, os, requests

URL = (f"{os.environ['SNOWFLAKE_ACCOUNT_URL']}/api/v2/databases/CLOSEUP_HOL2"
       "/schemas/APPS/agents/PHARMA_MARKET_AGENT:run")
HEADERS = {
    "Authorization": f"Bearer {os.environ['SNOWFLAKE_PAT']}",
    "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    "Content-Type": "application/json",
    "Accept": "text/event-stream",
}
body = {"messages": [{"role": "user", "content": [
    {"type": "text", "text": "How did Diabetes sales evolve month by month in Argentina?"}]}]}

with requests.post(URL, headers=HEADERS, json=body, stream=True, timeout=120) as r:
    r.raise_for_status()
    event = None
    for line in r.iter_lines(decode_unicode=True):
        if not line:
            continue
        if line.startswith("event:"):
            event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            payload = line[len("data:"):].strip()
            if payload == "[DONE]":  # end-of-stream marker, not JSON
                break
            data = json.loads(payload)
            if event == "response.text.delta":
                print(data.get("text", ""), end="", flush=True)
            elif event == "response.tool_use":
                print(f"\n[tool_use] {data.get('name')} -> {json.dumps(data.get('input'))[:200]}")
            elif event == "response.tool_result":
                print(f"\n[tool_result] {data.get('name')}")
            elif event == "error":
                print(f"\n[error] {data}")
print()
```

### Verification

```sql
SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER HOL2_API_USER;
```

---

### Recap: what you built in Step 9

- **You built:** the service user `HOL2_API_USER` with `AGENT_USER_ROLE`, a programmatic access token (PAT), and the network rule and policy that restrict where the token can be used from. Then you called the SQL API and the Cortex Agents REST API, including the streaming (SSE) response.
- **Key idea:** any back end that speaks HTTP can query data or ask the agent, with the same role-based security as a user in Snowsight.
- **Take it to your projects:** use a dedicated service user and role per integration, a PAT with a network policy, and never embed user passwords in code.

## Step 10: React App on Snowflake App Runtime (~8 min · Instructor demo)

> **Instructor demo.**  During the session the instructor runs this step live on screen.
> Everything you need to reproduce it later is below.

### Architecture

The browser never talks to Snowflake APIs directly and never sees a token. A **Next.js (React)** application runs inside a **Snowflake App Runtime** application service. Its server route `POST /api/chat` forwards questions to the Cortex Agents REST API and streams the SSE events back to the browser.

```text
Browser (React chat UI)
   |  HTTPS  (Snowflake login on the app endpoint)
   v
Application service on Snowflake App Runtime  (CLOSEUP_HOL2.APPS.CLOSEUP_AGENT_CHAT)
   +-- app/api/chat/route.ts  POST /api/chat ---> Cortex Agents REST API
   |                           /api/v2/databases/CLOSEUP_HOL2/schemas/APPS/agents/PHARMA_MARKET_AGENT:run
   |                           <--- SSE: response.text.delta, response.tool_use, response.tool_result, response
   +-- components/chat.tsx     renders streaming text + tool cards (SQL, tables, citations)
   v
PHARMA_MARKET_AGENT --> market_analytics (SV_PHARMA_MARKET) / field_intelligence (CS_FIELD_NOTES)
```

- **Auth inside the app service:** the platform provides an OAuth token at `/snowflake/session/token` and the host in `SNOWFLAKE_HOST`. `lib/agent-auth.ts` re-reads the token on every request and sends `Authorization: Bearer <token>` with `X-Snowflake-Authorization-Token-Type: OAUTH`. No secret is stored in the app.
- **Which role calls the agent:** inside the app service, calls run as the role that *owns* the app. That role needs the same grants as `AGENT_USER_ROLE` (agent, semantic view and its tables, search service, warehouse).
- **Auth for local development:** the same code falls back to a PAT (step 9) read from `.env.local`.

### 10.1 Get the code

The application code is published on GitHub, in the `app/` folder of the Lab 2 repository: <https://github.com/sfc-gh-gbertucci/closeup-snowday-lab2>. Choose one option:

**Option A: git clone**

```bash
git clone https://github.com/sfc-gh-gbertucci/closeup-snowday-lab2.git
cd closeup-snowday-lab2/app
```

**Option B: download the ZIP** (no git required)

1. Open [the repository page](https://github.com/sfc-gh-gbertucci/closeup-snowday-lab2).
2. Click the green **Code** button and choose **Download ZIP**.
3. Unzip the file (it creates a folder named `closeup-snowday-lab2-main`) and open a terminal in its `app` subfolder.

Key files: `app.yml` (App Runtime manifest), `app/api/chat/route.ts` (agent call and SSE), `lib/agent-auth.ts` (token handling), `components/chat.tsx` (UI) and `README.md`.

> **Prerequisites**  Node.js 20+, Snowflake CLI 3.26+ (`snow --version`) with a connection to your lab account (`snow connection add`), and steps 7 and 8 completed.

### 10.2 Let Cortex Code (CoCo) configure it for you

This is a good example of using an AI coding agent as a tool: instead of editing the manifest and running every command by hand, open the folder in **Cortex Code** (desktop or CLI) and ask it to prepare and deploy the app. CoCo reads the project, uses its Snowflake Apps skill, checks your connection and grants, and runs the commands, asking for your approval before each change.

Paste this prompt into CoCo:

```text
I cloned the Close-Up HOL React app in this folder. Please:
1. Check that my Snowflake CLI connection works and tell me which account and role it uses.
2. Review app.yml and set database CLOSEUP_HOL2, schema APPS and query_warehouse CLOSEUP_HOL2_WH.
3. Verify the owner role has access to agent CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT,
   the semantic view SV_PHARMA_MARKET, the search service CS_FIELD_NOTES and the warehouse.
   If something is missing, propose the GRANT statements before running them.
4. Run a dry run, then deploy the app to Snowflake App Runtime and give me the URL.
5. Grant USAGE on the application service to role HOL_ANALYST.
```

> **What to observe**  How CoCo plans the work, which commands it proposes, how it diagnoses a missing grant, and how you stay in control by approving each step. Try follow-ups such as *"Show me the app logs"* or *"Explain how route.ts parses the SSE events"*.

### 10.3 Or deploy manually

```bash
# from the project folder
snow connection test --connection <your_connection>
snow app setup --app-name=closeup_agent_chat --dry-run --connection <your_connection>
# edit app.yml if needed: query_warehouse: CLOSEUP_HOL2_WH
snow app deploy --verbose --connection <your_connection>
snow app open --print-only --connection <your_connection>
```

### 10.4 Grant access

```sql
USE ROLE ACCOUNTADMIN;
-- The role that owns the app calls the agent: give it the agent grants
-- (replace ACCOUNTADMIN with the app owner role if you deployed with another role)
GRANT ROLE AGENT_USER_ROLE TO ROLE ACCOUNTADMIN;

-- Let analysts open the app
GRANT USAGE ON APPLICATION SERVICE CLOSEUP_HOL2.APPS.CLOSEUP_AGENT_CHAT TO ROLE HOL_ANALYST;
SHOW APPLICATION SERVICES IN SCHEMA CLOSEUP_HOL2.APPS;
```

### 10.5 Open the endpoint and test

1. Open the URL printed by the deploy and sign in with your Snowflake user.
2. Ask *"Top 5 laboratories by market share in Cardiology in Brazil over the last 6 months"*: expect a `market_analytics` tool card with SQL and a table.
3. Ask *"What are field reps saying about stock-outs in Argentina?"*: expect a `field_intelligence` card with NOTE\_IDs.
4. Ask *"Why did Diabetes sales change in Chile? Combine numbers and field notes."*: expect both tools.
5. Expand the tool cards and compare them with the raw SSE events from step 9.

### 10.6 Local development alternative

```bash
npm install
cp .env.example .env.local
# edit .env.local:
#   SNOWFLAKE_ACCOUNT_URL=https://<org>-<account>.snowflakecomputing.com
#   SNOWFLAKE_PAT=<token of HOL2_API_USER from step 9>
npm run dev
# open http://localhost:3000
```

> **Same code, two auth modes**  Only the token source changes, so you can iterate locally with a PAT and deploy the identical build to App Runtime.

### Verification

```sql
SHOW APPLICATION SERVICES IN SCHEMA CLOSEUP_HOL2.APPS;
SHOW GRANTS ON AGENT CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT;
```

---

### Recap: what you built in Step 10

- **You built:** a React app deployed on Snowflake App Runtime that talks to the agent, with access granted through `AGENT_USER_ROLE`.
- **The complete platform:** files → RAW → SILVER → GOLD → semantic view + search → agent → REST API → React app, with security and cost controls at every layer, without the data leaving Snowflake.
- **Take it to your projects:** the code is on GitHub. Use it as a starting point for internal apps on top of your own agents.

## Step 11: Cleanup (After the event)

### Clean up everything

Run as ACCOUNTADMIN. Order matters: app first, then AI objects, the PAT, and finally databases, warehouse and roles.

> **Irreversible** 
>
> These statements remove all lab objects. Skip this step if you want to keep exploring.

```sql
USE ROLE ACCOUNTADMIN;

-- 1. App on Snowflake App Runtime: the CLOSEUP_AGENT_CHAT application service lives in
--    CLOSEUP_HOL2.APPS and is removed with the database in section 5.

-- 2. Agent and search service
DROP AGENT IF EXISTS CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT;
DROP CORTEX SEARCH SERVICE IF EXISTS CLOSEUP_HOL2.SEMANTIC.CS_FIELD_NOTES;

-- 3. Tasks (suspended in step 3, dropped with the DB; suspend root explicitly to be safe)
ALTER TASK IF EXISTS CLOSEUP_HOL2.RAW.T_ROOT SUSPEND;

-- 4. API service user, PAT and network policy
DROP USER IF EXISTS HOL2_API_USER;   -- also removes its PAT
DROP NETWORK POLICY IF EXISTS NP_HOL2_PAT;

-- 5. Databases, warehouse, roles
DROP DATABASE IF EXISTS CLOSEUP_HOL2_DEV;
DROP DATABASE IF EXISTS CLOSEUP_HOL2;
DROP WAREHOUSE IF EXISTS CLOSEUP_HOL2_WH;
DROP RESOURCE MONITOR IF EXISTS CLOSEUP_HOL2_RM;
DROP ROLE IF EXISTS AGENT_USER_ROLE;
DROP ROLE IF EXISTS HOL_ANALYST;
DROP ROLE IF EXISTS HOL_ENGINEER;
```

> **App removal** 
>
> The exact name and object type of the app depend on how `lab2-react-app/README.md` deployed it. Use the README teardown command or `SHOW SERVICES IN SCHEMA CLOSEUP_HOL2.APPS` and replace the placeholder above.

### What you built

- Resilient JSON ingestion with schema evolution, validation and idempotent loads.
- CDC with Streams + Tasks and declarative Dynamic Table pipelines (incremental, SCD2, custom REFRESH USING).
- A zero-copy dev lifecycle, RBAC with database roles, masking and row access policies.
- A governed semantic view, a Cortex Search service and a Cortex Agent defined as code.
- Programmatic consumption with a PAT, the SQL API and the Agent REST API with SSE, and a React app on Snowflake App Runtime.

### Congratulations!

You have completed the Production-Grade Data & AI Platform lab for Close-Up International. Every piece (pipelines, security, semantic layer, agent and app) can be versioned in Git and promoted across environments.

### Verification

```sql
SHOW DATABASES LIKE 'CLOSEUP_HOL2%';
SHOW ROLES LIKE '%HOL%';
SHOW WAREHOUSES LIKE 'CLOSEUP_HOL2_WH';
```

## Resources

- [Zero to Snowflake](https://www.snowflake.com/en/developers/guides/zero-to-snowflake/)
- [Comprehensive Guide to Dynamic Tables](https://www.snowflake.com/en/developers/guides/comprehensive-guide-to-dynamic-tables/)
- [Getting Started with Streams and Tasks](https://www.snowflake.com/en/developers/guides/getting-started-with-streams-and-tasks/)
- [Best Practices for Building Cortex Agents](https://www.snowflake.com/en/developers/guides/best-practices-to-building-cortex-agents/)
- [Cortex Agents REST API (docs)](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-rest-api)
- [Snowflake SQL API (docs)](https://docs.snowflake.com/en/developer-guide/sql-api/index)

© 2026 Snowflake Inc. All Rights Reserved.

---

© 2026 Snowflake Inc. All Rights Reserved.
