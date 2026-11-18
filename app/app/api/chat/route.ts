import { getAgentAuth, getAgentPath } from '@/lib/agent-auth';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

type ContentItem = { type: 'text'; text: string };
type ChatMessage = { role: 'user' | 'assistant'; content: ContentItem[] };

function sseError(message: string, status = 200): Response {
  // Errors are sent as an SSE `error` event so the client has one code path.
  const body = `event: error\ndata: ${JSON.stringify({ message })}\n\n`;
  return new Response(body, {
    status,
    headers: { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache' },
  });
}

function sanitize(input: unknown): ChatMessage[] | null {
  if (!Array.isArray(input) || input.length === 0 || input.length > 50) return null;
  const out: ChatMessage[] = [];
  for (const m of input) {
    if (!m || (m.role !== 'user' && m.role !== 'assistant') || !Array.isArray(m.content)) return null;
    const content = m.content
      .filter((c: unknown): c is ContentItem =>
        !!c && (c as ContentItem).type === 'text' && typeof (c as ContentItem).text === 'string',
      )
      .map((c: ContentItem) => ({ type: 'text' as const, text: c.text.slice(0, 20000) }));
    if (content.length) out.push({ role: m.role, content });
  }
  return out.length && out[out.length - 1].role === 'user' ? out : null;
}

/**
 * POST /api/chat  { messages: [{role, content:[{type:'text', text}]}] }
 * Proxies to the Cortex Agents :run endpoint with stream:true and pipes the
 * server-sent events (response.text.delta, response.thinking.delta,
 * response.tool_use, response.tool_result, response.table, response, error, ...)
 * straight to the browser. History is kept client-side and sent on every turn.
 */
export async function POST(request: Request) {
  let messages: ChatMessage[] | null;
  try {
    messages = sanitize((await request.json())?.messages);
  } catch {
    messages = null;
  }
  if (!messages) return sseError('Invalid request: messages must end with a user message', 400);

  let auth;
  try {
    auth = getAgentAuth();
  } catch (e) {
    console.error(new Date().toISOString(), '[chat] auth config error:', (e as Error).message);
    return sseError((e as Error).message, 500);
  }

  let upstream: Response;
  try {
    upstream = await fetch(auth.baseUrl + getAgentPath(), {
      method: 'POST',
      headers: {
        ...auth.headers,
        'Content-Type': 'application/json',
        Accept: 'text/event-stream',
      },
      body: JSON.stringify({ messages, stream: true }),
      signal: request.signal,
    });
  } catch (e) {
    console.error(new Date().toISOString(), '[chat] network error calling agent:', (e as Error).message);
    return sseError(`Network error reaching Snowflake: ${(e as Error).message}`, 502);
  }

  if (!upstream.ok || !upstream.body) {
    const detail = (await upstream.text().catch(() => '')).slice(0, 1000);
    console.error(new Date().toISOString(), `[chat] agent HTTP ${upstream.status} (auth=${auth.mode})`, detail);
    const hint =
      upstream.status === 401
        ? ' (token invalid/expired)'
        : upstream.status === 403 || upstream.status === 404
          ? ' (check USAGE grants on the agent, semantic view, search service and warehouse for the app/PAT role)'
          : '';
    return sseError(`Agent returned HTTP ${upstream.status}${hint}: ${detail}`, upstream.status);
  }

  return new Response(upstream.body, {
    headers: {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      'X-Accel-Buffering': 'no',
      Connection: 'keep-alive',
    },
  });
}
