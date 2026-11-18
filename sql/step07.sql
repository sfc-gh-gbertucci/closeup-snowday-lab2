-- Step 7: Semantic View via SQL
-- Source: lab2_production_platform.html

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

SHOW SEMANTIC VIEWS IN SCHEMA CLOSEUP_HOL2.SEMANTIC;
DESCRIBE SEMANTIC VIEW CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET;
SHOW SEMANTIC METRICS IN CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET;
SELECT GET_DDL('SEMANTIC_VIEW', 'CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET');

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

USE ROLE AGENT_USER_ROLE;
USE WAREHOUSE CLOSEUP_HOL2_WH;
SELECT * FROM SEMANTIC_VIEW(
    CLOSEUP_HOL2.SEMANTIC.SV_PHARMA_MARKET
    DIMENSIONS market_share.country
    METRICS market_share.total_market_sales_usd, market_share.total_market_units
)
ORDER BY total_market_sales_usd DESC;
USE ROLE ACCOUNTADMIN;
