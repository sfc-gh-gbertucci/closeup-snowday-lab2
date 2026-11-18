-- Step 8: Cortex Search + Cortex Agent via SQL
-- Source: lab2_production_platform.html

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

SHOW AGENTS IN SCHEMA CLOSEUP_HOL2.APPS;
SHOW CORTEX SEARCH SERVICES IN SCHEMA CLOSEUP_HOL2.SEMANTIC;
SHOW GRANTS ON AGENT CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT;
