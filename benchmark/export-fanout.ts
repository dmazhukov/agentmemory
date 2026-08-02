// Phase-0 measurement for the mem::export fan-out (see
// docs/superpowers/specs/2026-08-02-export-fanout-and-audit-retention-design.md).
//
// Answers one question before any fix is written: what actually stalls the
// event loop during a full export — the fan-out storm (50+ simultaneous
// state::list calls against one adapter) or a single oversized list whose
// WS frame blocks the loop while it is parsed?
//
// The distinction decides the fix. Bounding concurrency helps the first and
// does nothing for the second, and src/state/schema.ts:25-29 records that
// the second is real at scale ("37MB WS frame parse blocks heartbeat,
// worker is declared dead before any Promise.race timer can fire").
//
// The in-memory fake KV bypasses iii-sdk's WS layer entirely, so it cannot
// reproduce that parse cost on its own. SIMULATE_FRAME_PARSE re-introduces
// it by round-tripping every list result through JSON, which is the same
// synchronous work the real worker does on an incoming frame.
//
// Run: node --import tsx benchmark/export-fanout.ts
// Env: EXPORT_KV_CONCURRENCY, SIMULATE_FRAME_PARSE=0 to disable the parse.

import { registerExportImportFunction } from "../src/functions/export-import.js";
import { instrumentedKV } from "../test/helpers/instrumented-kv.js";

const SESSIONS = 40;
const OBS_PER_SESSION = 850;
const MEMORIES = 8000;
const GRAPH_NODES = 20000;
const GRAPH_EDGES = 20000;
const KV_LATENCY_MS = 5;
const LAG_PROBE_INTERVAL_MS = 20;

const simulateFrameParse = process.env.SIMULATE_FRAME_PARSE !== "0";

function mockSdk() {
  const functions = new Map<string, (payload: unknown) => Promise<unknown>>();
  return {
    registerFunction: (
      idOrOpts: string | { id: string },
      handler: (payload: unknown) => Promise<unknown>,
    ) => {
      const id = typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id;
      functions.set(id, handler);
    },
    registerTrigger: () => {},
    trigger: async (
      idOrInput: string | { function_id: string; payload: unknown },
      data?: unknown,
    ) => {
      const id =
        typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      const fn = functions.get(id);
      if (!fn) throw new Error(`No function: ${id}`);
      return fn(payload);
    },
  };
}

// Per-call synchronous block time, so the report can distinguish "many
// small stalls" (fan-out storm) from "one huge stall" (frame parse).
type BlockSample = { scope: string; ms: number; items: number };

function startLagProbe(intervalMs: number) {
  let maxLagMs = 0;
  let last = Date.now();
  const timer = setInterval(() => {
    const now = Date.now();
    maxLagMs = Math.max(maxLagMs, now - last - intervalMs);
    last = now;
  }, intervalMs);
  return {
    stop(): number {
      clearInterval(timer);
      return maxLagMs;
    },
  };
}

async function main(): Promise<void> {
  const kv = instrumentedKV({ latencyMs: KV_LATENCY_MS });
  const blocks: BlockSample[] = [];

  // Wrap list() so every result pays the JSON round-trip the real worker
  // pays when the engine's response frame arrives. Without this the
  // benchmark silently measures a workload the daemon never runs.
  const baseList = kv.list.bind(kv);
  const instrumented = Object.assign(kv, {
    list: async <T>(scope: string): Promise<T[]> => {
      const rows = await baseList<T>(scope);
      if (!simulateFrameParse) return rows;
      const startedAt = performance.now();
      const parsed = JSON.parse(JSON.stringify(rows)) as T[];
      blocks.push({
        scope,
        ms: performance.now() - startedAt,
        items: rows.length,
      });
      return parsed;
    },
  });

  const sdk = mockSdk();
  registerExportImportFunction(sdk as never, instrumented as never);

  for (let s = 0; s < SESSIONS; s++) {
    const sessionId = `ses_${s}`;
    kv.seed("mem:sessions", sessionId, {
      id: sessionId,
      project: `project_${s % 4}`,
      cwd: "/tmp",
      startedAt: "2026-02-01T00:00:00Z",
      status: "completed",
      observationCount: OBS_PER_SESSION,
    });
    for (let o = 0; o < OBS_PER_SESSION; o++) {
      kv.seed(`mem:obs:${sessionId}`, `obs_${s}_${o}`, {
        id: `obs_${s}_${o}`,
        sessionId,
        timestamp: "2026-02-01T10:00:00Z",
        type: "file_edit",
        title: `Edit ${o}`,
        facts: [`fact ${o} `.repeat(8)],
        narrative: `narrative ${o} `.repeat(20),
        concepts: ["auth"],
        files: [`src/file_${o}.ts`],
        importance: 5,
      });
    }
  }

  for (let m = 0; m < MEMORIES; m++) {
    kv.seed("mem:memories", `mem_${m}`, {
      id: `mem_${m}`,
      createdAt: "2026-02-01T00:00:00Z",
      updatedAt: "2026-02-01T00:00:00Z",
      type: "pattern",
      title: `Pattern ${m}`,
      content: `content ${m} `.repeat(30),
      concepts: ["auth"],
      files: [],
      sessionIds: ["ses_0"],
      strength: 5,
      version: 1,
      isLatest: true,
    });
  }

  for (let n = 0; n < GRAPH_NODES; n++) {
    kv.seed("mem:graph:nodes", `node_${n}`, {
      id: `node_${n}`,
      label: `Node ${n}`,
      type: "concept",
      mentions: 3,
      sessionIds: ["ses_0"],
    });
  }

  for (let e = 0; e < GRAPH_EDGES; e++) {
    kv.seed("mem:graph:edges", `edge_${e}`, {
      id: `edge_${e}`,
      source: `node_${e % GRAPH_NODES}`,
      target: `node_${(e + 1) % GRAPH_NODES}`,
      type: "relates_to",
      weight: 1,
    });
  }

  kv.resetInstrumentation();
  blocks.length = 0;

  const probe = startLagProbe(LAG_PROBE_INTERVAL_MS);
  const startedAt = Date.now();

  const result = await sdk.trigger("mem::export", {});

  const wallClockMs = Date.now() - startedAt;
  const maxEventLoopLagMs = probe.stop();
  const payloadBytes = Buffer.byteLength(JSON.stringify(result));

  const sorted = [...blocks].sort((a, b) => b.ms - a.ms);
  const totalBlockMs = blocks.reduce((sum, b) => sum + b.ms, 0);

  console.log(
    JSON.stringify(
      {
        config: {
          concurrency: process.env.EXPORT_KV_CONCURRENCY ?? "(unset)",
          simulateFrameParse,
          sessions: SESSIONS,
          obsPerSession: OBS_PER_SESSION,
          memories: MEMORIES,
          graphNodes: GRAPH_NODES,
          graphEdges: GRAPH_EDGES,
        },
        fanOut: {
          peakInFlight: kv.peakInFlight,
          kvCalls: kv.callCount,
        },
        eventLoop: {
          maxLagMs: maxEventLoopLagMs,
          probeIntervalMs: LAG_PROBE_INTERVAL_MS,
        },
        blocking: {
          totalSyncParseMs: +totalBlockMs.toFixed(1),
          worstSingleParse: sorted
            .slice(0, 5)
            .map((b) => ({ scope: b.scope, ms: +b.ms.toFixed(1), items: b.items })),
        },
        payloadMB: +(payloadBytes / 1024 / 1024).toFixed(2),
        wallClockMs,
      },
      null,
      2,
    ),
  );
}

void main();
