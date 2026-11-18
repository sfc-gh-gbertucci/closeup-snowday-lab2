'use client';

import { Button } from '@/components/ui/button';
import { SUGGESTED_QUESTIONS } from '@/lib/constants';
import { readSse } from '@/lib/sse';
import { cn } from '@/lib/utils';
import {
  ChevronDown,
  ChevronRight,
  Database,
  Loader2,
  RotateCcw,
  Search,
  Send,
  Sparkles,
  Square,
  Wrench,
} from 'lucide-react';
import { useEffect, useRef, useState } from 'react';

type Table = { title?: string; columns: string[]; rows: unknown[][] };
type Citation = { title?: string; text?: string; source?: string };
type ToolCall = {
  id: string;
  name: string;
  type: string;
  input?: unknown;
  status?: string;
  sql?: string;
  analystText?: string;
  citations: Citation[];
  tables: Table[];
};
type Msg = {
  role: 'user' | 'assistant';
  text: string;
  thinking?: string;
  status?: string;
  tools?: ToolCall[];
  tables?: Table[];
  error?: string;
  done?: boolean;
};

type ResultSet = {
  resultSetMetaData?: { rowType?: { name: string }[] };
  data?: unknown[][];
};

function toTable(rs: ResultSet | undefined, title?: string): Table | null {
  const cols = rs?.resultSetMetaData?.rowType?.map((c) => c.name);
  if (!cols?.length || !Array.isArray(rs?.data)) return null;
  return { title, columns: cols, rows: rs.data };
}

/** Pull SQL, citations and tabular results out of a response.tool_result payload. */
function parseToolResult(tc: ToolCall, content: unknown) {
  if (!Array.isArray(content)) return;
  for (const item of content) {
    const json = (item as { json?: Record<string, unknown> })?.json;
    if (!json) continue;
    if (typeof json.sql === 'string') tc.sql = json.sql;
    if (typeof json.text === 'string') tc.analystText = json.text;
    const t = toTable(json.result_set as ResultSet);
    if (t) tc.tables.push(t);
    const results = (json.searchResults ?? json.search_results ?? json.results) as unknown;
    if (Array.isArray(results)) {
      for (const r of results as Record<string, unknown>[]) {
        tc.citations.push({
          title: String(r.title ?? r.doc_title ?? r.source_id ?? r.doc_id ?? 'Result'),
          text: typeof r.text === 'string' ? r.text : undefined,
          source: typeof r.url === 'string' ? r.url : typeof r.doc_id === 'string' ? r.doc_id : undefined,
        });
      }
    }
  }
}

export function Chat() {
  const [messages, setMessages] = useState<Msg[]>([]);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => endRef.current?.scrollIntoView({ behavior: 'smooth' }), [messages]);

  const updateLast = (fn: (m: Msg) => Msg) =>
    setMessages((prev) => [...prev.slice(0, -1), fn(prev[prev.length - 1])]);

  async function send(question: string) {
    const text = question.trim();
    if (!text || busy) return;
    const history = [...messages.filter((m) => m.text), { role: 'user' as const, text }];
    setMessages([...messages, { role: 'user', text }, { role: 'assistant', text: '', tools: [], tables: [] }]);
    setInput('');
    setBusy(true);
    const ctrl = new AbortController();
    abortRef.current = ctrl;

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: history.map((m) => ({ role: m.role, content: [{ type: 'text', text: m.text }] })),
        }),
        signal: ctrl.signal,
      });
      if (!res.body) throw new Error(`HTTP ${res.status}`);

      for await (const ev of readSse(res.body)) {
        let d: Record<string, unknown>;
        try {
          d = JSON.parse(ev.data);
        } catch {
          continue;
        }
        switch (ev.event) {
          case 'response.text.delta':
            updateLast((m) => ({ ...m, text: m.text + String(d.text ?? '') }));
            break;
          case 'response.thinking.delta':
            updateLast((m) => ({ ...m, thinking: (m.thinking ?? '') + String(d.text ?? '') }));
            break;
          case 'response.status':
            updateLast((m) => ({ ...m, status: String(d.message ?? d.status ?? '') }));
            break;
          case 'response.tool_use':
            updateLast((m) => ({
              ...m,
              tools: [
                ...(m.tools ?? []),
                {
                  id: String(d.tool_use_id),
                  name: String(d.name ?? 'tool'),
                  type: String(d.type ?? ''),
                  input: d.input,
                  citations: [],
                  tables: [],
                },
              ],
            }));
            break;
          case 'response.tool_result':
            updateLast((m) => ({
              ...m,
              tools: (m.tools ?? []).map((t) => {
                if (t.id !== String(d.tool_use_id)) return t;
                const next = { ...t, status: String(d.status ?? 'done'), citations: [...t.citations], tables: [...t.tables] };
                parseToolResult(next, d.content);
                return next;
              }),
            }));
            break;
          case 'response.table': {
            const t = toTable(d.result_set as ResultSet, d.title as string | undefined);
            if (t) updateLast((m) => ({ ...m, tables: [...(m.tables ?? []), t] }));
            break;
          }
          case 'response':
            // Final aggregated response; deltas already built the text. Use it as fallback.
            updateLast((m) => {
              if (m.text) return m;
              const content = (d.content as { type: string; text?: string }[]) ?? [];
              return { ...m, text: content.filter((c) => c.type === 'text').map((c) => c.text).join('\n') };
            });
            break;
          case 'error':
            updateLast((m) => ({ ...m, error: String(d.message ?? 'Unknown error') }));
            break;
          default:
            break; // unknown events are ignored, per the API guidance
        }
      }
    } catch (e) {
      if ((e as Error).name !== 'AbortError') updateLast((m) => ({ ...m, error: (e as Error).message }));
    } finally {
      updateLast((m) => ({ ...m, done: true, status: undefined }));
      setBusy(false);
      abortRef.current = null;
    }
  }

  return (
    <div className="flex h-[calc(100vh-3.5rem)] flex-col">
      <div className="flex-1 overflow-y-auto">
        <div className="mx-auto w-full max-w-4xl space-y-6 px-4 py-6 sm:px-6">
          {messages.length === 0 && <Welcome onPick={send} />}
          {messages.map((m, i) =>
            m.role === 'user' ? (
              <div key={i} className="flex justify-end">
                <div className="max-w-[85%] whitespace-pre-wrap rounded-2xl rounded-br-sm bg-[#11567F] px-4 py-2.5 text-sm text-white">
                  {m.text}
                </div>
              </div>
            ) : (
              <AssistantMessage key={i} msg={m} />
            ),
          )}
          <div ref={endRef} />
        </div>
      </div>

      <form
        className="border-t border-border bg-background/80 backdrop-blur"
        onSubmit={(e) => {
          e.preventDefault();
          send(input);
        }}
      >
        <div className="mx-auto flex w-full max-w-4xl items-end gap-2 px-4 py-3 sm:px-6">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                send(input);
              }
            }}
            rows={1}
            placeholder="Ask about pharma market share, sales, laboratories..."
            className="max-h-40 min-h-11 flex-1 resize-none rounded-xl border border-input bg-background px-3 py-2.5 text-sm outline-none focus:ring-2 focus:ring-[#29B5E8]"
          />
          {busy ? (
            <Button type="button" variant="outline" onClick={() => abortRef.current?.abort()} aria-label="Stop">
              <Square className="size-4" />
            </Button>
          ) : (
            <Button type="submit" disabled={!input.trim()} className="bg-[#29B5E8] hover:bg-[#11567F]" aria-label="Send">
              <Send className="size-4" />
            </Button>
          )}
          {messages.length > 0 && !busy && (
            <Button type="button" variant="ghost" onClick={() => setMessages([])} aria-label="New conversation">
              <RotateCcw className="size-4" />
            </Button>
          )}
        </div>
      </form>
    </div>
  );
}

function Welcome({ onPick }: { onPick: (q: string) => void }) {
  return (
    <div className="space-y-6 py-10 text-center">
      <div className="mx-auto flex size-14 items-center justify-center rounded-2xl bg-gradient-to-br from-[#11567F] to-[#29B5E8] text-white">
        <Sparkles className="size-7" />
      </div>
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-[#11567F] dark:text-[#29B5E8]">
          Close-Up Market Intelligence Assistant
        </h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Powered by the Snowflake Cortex Agent PHARMA_MARKET_AGENT (Cortex Analyst + Cortex Search).
        </p>
      </div>
      <div className="mx-auto grid max-w-2xl gap-2">
        {SUGGESTED_QUESTIONS.map((q) => (
          <button
            key={q}
            onClick={() => onPick(q)}
            className="rounded-xl border border-border px-4 py-3 text-left text-sm transition hover:border-[#29B5E8] hover:bg-[#29B5E8]/5"
          >
            {q}
          </button>
        ))}
      </div>
    </div>
  );
}

function AssistantMessage({ msg }: { msg: Msg }) {
  const [open, setOpen] = useState(false);
  const tools = msg.tools ?? [];
  const working = !msg.done;
  return (
    <div className="space-y-2">
      {(tools.length > 0 || msg.thinking) && (
        <div className="rounded-xl border border-border bg-muted/30 text-sm">
          <button
            onClick={() => setOpen(!open)}
            className="flex w-full items-center gap-2 px-3 py-2 text-left text-muted-foreground"
          >
            {open ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
            <Wrench className="size-4" />
            <span>
              {tools.length} tool call{tools.length === 1 ? '' : 's'}
              {tools.length > 0 && `: ${tools.map((t) => t.name).join(', ')}`}
            </span>
          </button>
          {open && (
            <div className="space-y-3 border-t border-border px-3 py-3">
              {msg.thinking && (
                <details>
                  <summary className="cursor-pointer text-xs font-medium text-muted-foreground">Reasoning</summary>
                  <p className="mt-1 whitespace-pre-wrap text-xs text-muted-foreground">{msg.thinking}</p>
                </details>
              )}
              {tools.map((t) => (
                <ToolDetails key={t.id} tool={t} />
              ))}
            </div>
          )}
        </div>
      )}

      {working && msg.status && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="size-3 animate-spin" /> {msg.status}
        </div>
      )}

      {(msg.text || working) && (
        <div className="whitespace-pre-wrap rounded-2xl rounded-bl-sm border border-border bg-card px-4 py-3 text-sm leading-relaxed">
          {msg.text}
          {working && <span className="ml-0.5 inline-block h-4 w-1.5 animate-pulse bg-[#29B5E8] align-middle" />}
        </div>
      )}

      {(msg.tables ?? []).map((t, i) => (
        <DataTable key={i} table={t} />
      ))}

      {msg.error && (
        <div className="rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {msg.error}
        </div>
      )}
    </div>
  );
}

function ToolDetails({ tool }: { tool: ToolCall }) {
  const isSearch = tool.type.includes('search');
  return (
    <div className="space-y-2 rounded-lg border border-border bg-background p-3">
      <div className="flex items-center gap-2 text-xs">
        {isSearch ? <Search className="size-3.5 text-[#29B5E8]" /> : <Database className="size-3.5 text-[#29B5E8]" />}
        <span className="font-semibold">{tool.name}</span>
        <span className="text-muted-foreground">{tool.type}</span>
        <span
          className={cn(
            'ml-auto rounded px-1.5 py-0.5 text-[10px] uppercase',
            tool.status ? 'bg-[#29B5E8]/15 text-[#11567F] dark:text-[#29B5E8]' : 'bg-muted text-muted-foreground',
          )}
        >
          {tool.status ?? 'running'}
        </span>
      </div>
      {tool.analystText && <p className="text-xs text-muted-foreground">{tool.analystText}</p>}
      {tool.sql && (
        <pre className="max-h-64 overflow-auto rounded-md bg-[#0b2f45] p-2 text-[11px] leading-snug text-[#d6f1fb]">
          <code>{tool.sql}</code>
        </pre>
      )}
      {tool.tables.map((t, i) => (
        <DataTable key={i} table={t} compact />
      ))}
      {tool.citations.length > 0 && (
        <ol className="list-decimal space-y-1 pl-5 text-xs">
          {tool.citations.slice(0, 10).map((c, i) => (
            <li key={i}>
              <span className="font-medium">{c.title}</span>
              {c.source && c.source !== c.title && <span className="text-muted-foreground"> — {c.source}</span>}
              {c.text && <p className="line-clamp-2 text-muted-foreground">{c.text}</p>}
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}

function DataTable({ table, compact }: { table: Table; compact?: boolean }) {
  const rows = table.rows.slice(0, compact ? 10 : 50);
  return (
    <div className="overflow-hidden rounded-xl border border-border">
      {table.title && <div className="bg-muted/40 px-3 py-1.5 text-xs font-medium">{table.title}</div>}
      <div className="max-h-80 overflow-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-[#11567F] text-white">
            <tr>
              {table.columns.map((c) => (
                <th key={c} className="whitespace-nowrap px-3 py-1.5 text-left font-medium">
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="border-t border-border odd:bg-muted/20">
                {r.map((v, j) => (
                  <td key={j} className="whitespace-nowrap px-3 py-1 tabular-nums">
                    {v === null || v === undefined ? '—' : String(v)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {table.rows.length > rows.length && (
        <div className="px-3 py-1 text-[11px] text-muted-foreground">
          Showing {rows.length} of {table.rows.length} rows
        </div>
      )}
    </div>
  );
}
