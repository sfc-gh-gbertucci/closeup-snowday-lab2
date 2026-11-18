-- Step 1: Setup & Environment
-- Source: lab2_production_platform.html

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

SELECT 'LABORATORIES' AS TABLE_NAME, COUNT(*) AS ROWS_ FROM SOURCE_SYSTEM.LABORATORIES UNION ALL
SELECT 'PRODUCTS',      COUNT(*) FROM SOURCE_SYSTEM.PRODUCTS      UNION ALL
SELECT 'PHARMACIES',    COUNT(*) FROM SOURCE_SYSTEM.PHARMACIES    UNION ALL
SELECT 'DOCTORS',       COUNT(*) FROM SOURCE_SYSTEM.DOCTORS       UNION ALL
SELECT 'SELL_OUT',      COUNT(*) FROM SOURCE_SYSTEM.SELL_OUT      UNION ALL
SELECT 'PRESCRIPTIONS', COUNT(*) FROM SOURCE_SYSTEM.PRESCRIPTIONS UNION ALL
SELECT 'FIELD_NOTES',   COUNT(*) FROM SOURCE_SYSTEM.FIELD_NOTES;
