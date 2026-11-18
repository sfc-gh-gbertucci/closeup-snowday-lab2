# Close-Up SnowDay — Lab 2: Production-Grade Data & AI Platform

An advanced hands-on lab, run as a challenge (26/11, 14:00–17:00, ~115 min in 4 blocks). It uses **synthetic pharma market data**. None of it is real Close-Up or customer data.

**Guide:** [lab2_production_platform.md](lab2_production_platform.md) (read on GitHub) · [interactive HTML version](lab2_production_platform.html) (download and open in your browser for Copy buttons and progress tracking)

| Block | Steps | Topic |
|---|---|---|
| 1 — Land the data | 1–2 | Platform setup, cost guardrails, resilient ingestion with schema evolution |
| 2 — Make it flow | 3–4 | CDC with Streams + Tasks, Dynamic Tables (incremental, SCD2) |
| 3 — Lock it down | 6 | RBAC with database roles, masking and row access policies |
| 4 — Make it talk | 7–8 | Semantic View, Cortex Search and a Cortex Agent, all in SQL |
| Instructor demo | 9–10 | SQL API and Agents REST API with a PAT; React app on Snowflake App Runtime |

Step 5 (clone/swap/undrop) and step 1.5 (budgets) are optional.

## Repository contents

```
lab2_production_platform.md     step-by-step guide (renders on GitHub)
lab2_production_platform.html   same guide, interactive (Copy buttons, progress)
sql/stepNN.sql                  every SQL block, one file per step
app/                            React (Next.js) chat app for the Cortex Agent (step 10)
```

## How to use

1. `git clone https://github.com/sfc-gh-gbertucci/closeup-snowday-lab2.git`, or use **Code → Download ZIP** and unzip it.
2. Read `lab2_production_platform.md` on GitHub, or open `lab2_production_platform.html` in your browser, and follow the steps in a Snowsight SQL worksheet.
3. Step 9 creates a dedicated service user (`HOL2_API_USER`), a network policy for your IP and a role-restricted PAT. That is all a new account needs to call the APIs.
4. Step 10 deploys `app/` to Snowflake App Runtime:

```bash
cd closeup-snowday-lab2/app
snow app deploy --connection <your_connection>
snow app open --print-only --connection <your_connection>
```

**Requirements:** a Snowflake account with `ACCOUNTADMIN` (Enterprise for masking and row access; step 6 shows Standard Edition alternatives). For step 10 you also need Node 20+ and Snowflake CLI 3.26+.
**Cleanup:** step 11.
