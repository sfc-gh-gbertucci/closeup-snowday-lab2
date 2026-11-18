import { existsSync, readFileSync } from 'node:fs';

// Auth for the Cortex Agents REST API.
// - Inside Snowflake App Runtime (SPCS): the platform mounts a short-lived OAuth
//   token at /snowflake/session/token and sets SNOWFLAKE_HOST. The file is
//   refreshed by the platform, so it is re-read on every request.
// - Local dev: a programmatic access token (PAT) from .env.local / .env.
// Tokens are never logged or returned to the browser.

const SPCS_TOKEN_PATH = '/snowflake/session/token';

export type AgentAuth = {
  baseUrl: string;
  headers: Record<string, string>;
  mode: 'spcs-oauth' | 'pat';
};

export function getAgentAuth(): AgentAuth {
  if (existsSync(SPCS_TOKEN_PATH) && process.env.SNOWFLAKE_HOST) {
    const token = readFileSync(SPCS_TOKEN_PATH, 'utf8').trim();
    return {
      baseUrl: `https://${process.env.SNOWFLAKE_HOST}`,
      mode: 'spcs-oauth',
      headers: {
        Authorization: `Bearer ${token}`,
        'X-Snowflake-Authorization-Token-Type': 'OAUTH',
      },
    };
  }

  const accountUrl = process.env.SNOWFLAKE_ACCOUNT_URL;
  const pat = process.env.SNOWFLAKE_PAT;
  if (!accountUrl || !pat) {
    throw new Error(
      'No Snowflake credentials: not running in SAR (no /snowflake/session/token) and ' +
        'SNOWFLAKE_ACCOUNT_URL / SNOWFLAKE_PAT are not set in .env.local',
    );
  }
  return {
    baseUrl: accountUrl.replace(/\/+$/, ''),
    mode: 'pat',
    headers: {
      Authorization: `Bearer ${pat}`,
      'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
    },
  };
}

export function getAgentPath(): string {
  const db = process.env.AGENT_DB ?? 'CLOSEUP_HOL2';
  const schema = process.env.AGENT_SCHEMA ?? 'APPS';
  const name = process.env.AGENT_NAME ?? 'PHARMA_MARKET_AGENT';
  const enc = encodeURIComponent;
  return `/api/v2/databases/${enc(db)}/schemas/${enc(schema)}/agents/${enc(name)}:run`;
}
