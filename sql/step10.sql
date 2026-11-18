-- Step 10: React App on Snowflake App Runtime
-- Source: lab2_production_platform.html

USE ROLE ACCOUNTADMIN;
-- The role that owns the app calls the agent: give it the agent grants
-- (replace ACCOUNTADMIN with the app owner role if you deployed with another role)
GRANT ROLE AGENT_USER_ROLE TO ROLE ACCOUNTADMIN;

-- Let analysts open the app
GRANT USAGE ON APPLICATION SERVICE CLOSEUP_HOL2.APPS.CLOSEUP_AGENT_CHAT TO ROLE HOL_ANALYST;
SHOW APPLICATION SERVICES IN SCHEMA CLOSEUP_HOL2.APPS;

SHOW APPLICATION SERVICES IN SCHEMA CLOSEUP_HOL2.APPS;
SHOW GRANTS ON AGENT CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT;
