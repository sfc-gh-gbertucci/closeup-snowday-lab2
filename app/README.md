# Close-Up Market Intelligence Assistant (Lab 2 — React app on Snowflake App Runtime)

A Next.js + React chat app that embeds the Cortex Agent
`CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT` and runs on Snowflake App Runtime
(an `APPLICATION SERVICE`). Scaffolded from the official `snowflake-apps`
Next.js template.

## Architecture

```
Browser (React, components/chat.tsx)
  │  POST /api/chat  { messages: [...full history...] }   (history lives client-side)
  ▼
Next.js route handler (app/api/chat/route.ts, Node runtime, inside SAR)
  │  POST https://{host}/api/v2/databases/CLOSEUP_HOL2/schemas/APPS/agents/PHARMA_MARKET_AGENT:run
  │       { messages, stream: true }   Accept: text/event-stream
  │  Auth (lib/agent-auth.ts):
  │    SAR  -> Bearer <contents of /snowflake/session/token>, token type OAUTH, host = $SNOWFLAKE_HOST
  │    local-> Bearer $SNOWFLAKE_PAT, token type PROGRAMMATIC_ACCESS_TOKEN, host = $SNOWFLAKE_ACCOUNT_URL
  ▼
Cortex Agent ── Cortex Analyst (semantic view) / Cortex Search ── warehouse
  │  SSE stream piped back unchanged
  ▼
Browser parses events (lib/sse.ts):
  response.text.delta      -> streaming answer text
  response.thinking.delta  -> "Reasoning" (inside the tool panel)
  response.status          -> progress line ("Planning the next steps"...)
  response.tool_use        -> tool name / type
  response.tool_result     -> generated SQL, analyst text, result_set table, search citations
  response.table           -> rendered data table
  response                 -> final aggregate (fallback text)
  error                    -> error banner
```

Tokens are read server-side only, never logged and never sent to the browser.
The SAR token file is re-read on every request (the platform rotates it).

Key files:

| File | Purpose |
| --- | --- |
| `app/api/chat/route.ts` | Validates history, calls `agent:run`, streams SSE to the browser |
| `lib/agent-auth.ts` | SAR OAuth token vs local PAT, agent URL from `AGENT_DB/AGENT_SCHEMA/AGENT_NAME` |
| `lib/sse.ts` | Fetch-stream SSE parser |
| `components/chat.tsx` | Chat UI, collapsible tool panel, SQL, citations, tables |
| `app.yml` | `version: 2` SAR manifest (deploy target + build/run phases + env vars) |

## Prerequisites

- Lab steps 1-8 completed: `CLOSEUP_HOL2` database, semantic view, Cortex Search service and the agent
  `CLOSEUP_HOL2.APPS.PHARMA_MARKET_AGENT` exist, and role `AGENT_USER_ROLE` has USAGE on the agent,
  the semantic view / underlying tables, the search service and the warehouse.
- Node.js 20+ and npm.
- Snowflake CLI 3.26+ (`snow --version`; `snow app deploy --help` must list `--target`).
- A CLI connection (`snow connection add`) whose role can create an application service in `CLOSEUP_HOL2.APPS`.
- The role that **owns** the application service needs the same grants as `AGENT_USER_ROLE`
  (the server calls the agent with the service's own OAuth token, i.e. owner's rights).
  Simplest: deploy with a role that has been granted `AGENT_USER_ROLE`.

## Local development

```bash
npm install
cp .env.example .env.local        # set SNOWFLAKE_ACCOUNT_URL and SNOWFLAKE_PAT
npm run dev                       # http://localhost:3000
npm run build                     # production build check
npm test                          # template unit tests
```

The PAT's user must have `AGENT_USER_ROLE` as default role (or a role with equivalent grants).
Use hyphens, not underscores, in the account URL host.

## Deploy to Snowflake App Runtime

```bash
cd lab2-react-app

# 1. Pick/verify the connection
snow connection list
snow connection test --connection <your_connection>

# 2. Resolve database/schema/warehouse/build EAI and reconcile app.yml (writes nothing)
snow app setup --app-name=closeup_agent_chat --dry-run --connection <your_connection>

# 3. Deploy (build in Snowflake, create/upgrade the APPLICATION SERVICE)
snow app deploy --verbose --connection <your_connection>

# 4. Get the app URL
snow app open --print-only --connection <your_connection>

# 5. Share with analysts
snow sql --connection <your_connection> -q "GRANT USAGE ON APPLICATION SERVICE CLOSEUP_HOL2.APPS.CLOSEUP_AGENT_CHAT TO ROLE HOL_ANALYST"

# Logs / status
snow app events --connection <your_connection>
snow sql --connection <your_connection> -q "DESCRIBE APPLICATION SERVICE CLOSEUP_HOL2.APPS.CLOSEUP_AGENT_CHAT"
```

Notes:
- `app.yml` was written without a live `snow app setup` run; set `query_warehouse` (and `build_eai`
  if your account needs one for `npm ci`) from the dry-run output before deploying.
- `snow app deploy` is not idempotent: re-running starts a new deployment. Use `--build-only`,
  `--upload-only` or `--promote-only` to retry one phase.
- Use `APPLICATION SERVICE` (not `SERVICE`) in GRANT/DESCRIBE statements.
- Teardown: `snow app teardown --force` (drops the service; cannot be undone).

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `Agent returned HTTP 403` / `404` / "does not exist or not authorized" | The calling role (service owner role in SAR, PAT user's role locally) lacks grants. Grant `USAGE` on database `CLOSEUP_HOL2`, schema `APPS`, the agent, the semantic view (`SELECT` / `REFERENCES` on the semantic view and its tables), the Cortex Search service, and the warehouse. Easiest: `GRANT ROLE AGENT_USER_ROLE TO ROLE <owner_role>`. Also confirm `SNOWFLAKE.CORTEX_USER` (or `CORTEX_AGENT_USER`) database role. |
| `HTTP 401` | Local: PAT expired/revoked or wrong token type. SAR: token file missing — confirm the app runs in SAR and `SNOWFLAKE_HOST` is set. |
| `No Snowflake credentials` | Running locally without `.env.local`. |
| `Network error reaching Snowflake` | Local: VPN/network policy blocks your IP, or wrong account URL. A network policy attached to the PAT user must allow your IP. |
| Analysts get 403 opening the app | `GRANT USAGE ON APPLICATION SERVICE ... TO ROLE HOL_ANALYST` missing. |
| Deploy build fails on `npm ci` | Builder has no egress: set `build_eai` in `app.yml` to an external access integration that allows the npm registry. |
| Tool result has no SQL / table | The agent's tool payload differs; open the tool panel and check the raw status, or inspect events in the browser Network tab. |
