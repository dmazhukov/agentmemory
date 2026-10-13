import { afterEach, describe, expect, it, vi } from "vitest";
import { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import { resetGraphGeneration, setGraphGenerationListener } from "../src/state/graph-generation.js";
import { registerViewerStreamTriggers } from "../src/triggers/viewer-streams.js";
import { getViewerCounts, resetViewerCounts } from "../src/state/viewer-counts.js";
import { setAuditRecordedListener } from "../src/functions/audit.js";
import type { GraphNode, GraphSnapshot } from "../src/types.js";

function snapshot(count: number): GraphSnapshot {
  const topNodes: GraphNode[] = Array.from({ length: count }, (_, i) => ({
    id: `n${i}`, type: "concept", name: `node ${i}`, properties: {},
    sourceObservationIds: [], createdAt: "2026-10-12T12:00:00.000Z",
  }));
  return {
    version: 1, topNodes, topEdges: [], topDegrees: Object.fromEntries(topNodes.map((n) => [n.id, 0])),
    stats: { totalNodes: count, totalEdges: 0, nodesByType: { concept: count }, edgesByType: {} },
    updatedAt: "2026-10-12T12:00:00.000Z", dirty: false,
  };
}

async function setup() {
  const store = new Map<string, Map<string, unknown>>();
  const handlers = new Map<string, (data: any) => Promise<any>>();
  const sent: Array<{ type: string; data: { delta: any; stats: any; generation?: string; reset?: boolean } }> = [];
  let failSend = false;
  const sdk = {
    registerFunction: vi.fn((id: string, handler: (data: any) => Promise<any>) => handlers.set(id, handler)), registerTrigger: vi.fn(),
    trigger: async ({ function_id, payload: p }: { function_id: string; payload: any }): Promise<any> => {
      if (function_id === "stream::send") {
        if (failSend) { failSend = false; throw new Error("stream unavailable"); }
        sent.push(structuredClone(p)); return {};
      }
      const rows = store.get(p.scope) ?? new Map<string, unknown>();
      if (function_id === "state::get") return structuredClone(rows.get(p.key) ?? null);
      if (function_id === "state::list") return structuredClone([...rows.values()]);
      if (function_id === "state::set") {
        rows.set(p.key, structuredClone(p.value)); store.set(p.scope, rows); return p.value;
      }
      if (function_id === "state::delete") { rows.delete(p.key); return; }
      throw new Error(`Unexpected call ${function_id}`);
    },
  };
  const kv = new StateKV(sdk as never);
  await resetGraphGeneration(kv, snapshot(0));
  registerViewerStreamTriggers(sdk as never, kv);
  const write = (value: GraphSnapshot) => kv.set(KV.graphSnapshot, "current", value);
  const graphEvents = () => sent.filter((e) => e.type === "graph.changed");
  return { kv, sdk, write, graphEvents, handlers, sent, failNextSend: () => { failSend = true; } };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  setGraphGenerationListener(null);
  setAuditRecordedListener(null);
  resetViewerCounts();
});

describe("generation graph viewer deltas", () => {
  it("sends a bounded single-node patch for a 250-node graph after its initial invalidation", async () => {
    const { write, graphEvents } = await setup();
    const initial = snapshot(250);
    await write(initial);
    expect(graphEvents()[0].data.delta).toBeNull();
    const changed = structuredClone(initial);
    changed.topNodes[125].properties = { changed: true };
    await write(changed);
    expect(graphEvents()[1].data.delta).toEqual({
      full: false, nodes: [changed.topNodes[125]], edges: [],
      removedNodeIds: [], removedEdgeIds: [], degrees: {},
    });
    changed.topNodes.splice(10, 1);
    delete changed.topDegrees.n10;
    changed.stats.totalNodes--;
    await write(changed);
    expect(graphEvents()[2].data.delta).toMatchObject({ full: false, nodes: [], removedNodeIds: ["n10"] });
  });

  it("starts with a complete baseline after listener restart, then uses incremental patches", async () => {
    const { kv, sdk, write, graphEvents } = await setup();
    await write(snapshot(2));
    await write(snapshot(3));
    expect(graphEvents()[1].data.delta).toMatchObject({ full: false, nodes: [{ id: "n2" }] });
    registerViewerStreamTriggers(sdk as never, kv);
    await write(snapshot(4));
    expect(graphEvents()[2].data.delta).toMatchObject({ full: true, nodes: snapshot(4).topNodes });
    await write(snapshot(5));
    expect(graphEvents()[3].data.delta).toMatchObject({ full: false, nodes: [{ id: "n4" }] });
  });

  it("does not advance its baseline when a stream send fails", async () => {
    const { write, graphEvents, failNextSend } = await setup();
    await write(snapshot(2));
    failNextSend();
    await write(snapshot(3));
    expect(graphEvents()).toHaveLength(1);
    await write(snapshot(4));
    expect(graphEvents()[1].data.delta).toMatchObject({ full: false, nodes: [{ id: "n2" }, { id: "n3" }] });
  });

  it("invalidates the baseline across repeated same-timestamp resets, including a failed reset notification", async () => {
    const { kv, write, graphEvents, failNextSend } = await setup();
    await write(snapshot(2));
    for (const lostReset of [false, true, false]) {
      if (lostReset) failNextSend();
      const before = graphEvents().length;
      await resetGraphGeneration(kv, snapshot(0));
      if (!lostReset) expect(graphEvents()[before].data.delta).toMatchObject({ full: true, nodes: [] });
      await write(snapshot(1));
      const latest = graphEvents().at(-1)!.data.delta;
      expect(latest).toMatchObject({ full: lostReset, nodes: [{ id: "n0" }] });
      expect(graphEvents().at(-1)!.data.reset).toBe(lostReset);
      expect(graphEvents().at(-1)!.data.generation).toBe(await kv.get(KV.graphControl, "generation"));
      await write(snapshot(2));
      expect(graphEvents().at(-1)!.data.delta).toMatchObject({ full: false, nodes: [{ id: "n1" }] });
    }
  });

  it("keeps the last delivered baseline after a snapshot read failure", async () => {
    const { kv, write, graphEvents } = await setup();
    await write(snapshot(2));
    const original = kv.get.bind(kv);
    let failRead = true;
    vi.spyOn(kv, "get").mockImplementation(async (scope, key) => {
      if (scope === KV.graphSnapshot && failRead) { failRead = false; throw new Error("read failed"); }
      return original(scope, key);
    });
    await write(snapshot(3));
    expect(graphEvents()).toHaveLength(2);
    expect(graphEvents()[1].data).toMatchObject({ source: "items", coveredBySnapshot: false });
    await write(snapshot(4));
    expect(graphEvents()[2].data.delta).toMatchObject({ full: false, nodes: [{ id: "n2" }, { id: "n3" }] });
  });

  it("refreshes cached counts even if reading the reset snapshot fails", async () => {
    const { kv, write, graphEvents } = await setup();
    await write(snapshot(20));
    expect((await getViewerCounts(kv)).graphNodes).toBe(20);
    const original = kv.get.bind(kv);
    let failRead = true;
    vi.spyOn(kv, "get").mockImplementation(async (scope, key) => {
      if (scope === KV.graphSnapshot && failRead) { failRead = false; throw new Error("read failed"); }
      return original(scope, key);
    });
    await resetGraphGeneration(kv, snapshot(0));
    expect((await getViewerCounts(kv)).graphNodes).toBe(0);
    expect(graphEvents().at(-1)!.data).toMatchObject({ source: "items", reset: true, coveredBySnapshot: false });
    expect(graphEvents().at(-1)!.data).not.toHaveProperty("generation");
  });

  it("still pushes counts immediately after a successful reset", async () => {
    vi.useFakeTimers();
    const { kv, write, sent } = await setup();
    await write(snapshot(20));
    await getViewerCounts(kv);
    await resetGraphGeneration(kv, snapshot(0));
    await vi.advanceTimersByTimeAsync(500);
    expect(sent.filter((e) => e.type === "counts.changed").at(-1)?.data).toMatchObject({ counts: { graphNodes: 0 } });
  });

  it("does not mark coalesced item events covered by a failed snapshot send", async () => {
    vi.useFakeTimers();
    const { kv, write, graphEvents, failNextSend } = await setup();
    await write(snapshot(2));
    await kv.set(KV.graphNodes, "n2", snapshot(3).topNodes[2]);
    failNextSend();
    await write(snapshot(3));
    await vi.advanceTimersByTimeAsync(1000);
    expect(graphEvents().at(-1)?.data).toMatchObject({ source: "items", coveredBySnapshot: false });
  });

  it("tags the reconnect snapshot with its generation", async () => {
    const { kv, write, handlers } = await setup();
    await write(snapshot(2));
    const response = await handlers.get("api::viewer-snapshot")!({ headers: {}, query_params: {} });
    expect(response.body.graph).toMatchObject({
      generation: await kv.get(KV.graphControl, "generation"), stats: { totalNodes: 2 },
    });
  });

  it("orders queued snapshot deltas from the last delivered graph", async () => {
    const { kv, write, graphEvents } = await setup();
    await write(snapshot(250));
    const original = kv.get.bind(kv);
    let entered!: () => void;
    let release!: () => void;
    const ready = new Promise<void>((r) => { entered = r; });
    const gate = new Promise<void>((r) => { release = r; });
    let pause = true;
    vi.spyOn(kv, "get").mockImplementation(async (scope, key) => {
      const value = await original(scope, key);
      if (scope === KV.graphSnapshot && pause) { pause = false; entered(); await gate; }
      return value;
    });
    const first = write(snapshot(251));
    await ready;
    const second = write(snapshot(252));
    release();
    await Promise.all([first, second]);
    expect(graphEvents()[1].data.delta).toMatchObject({ full: false, nodes: [{ id: "n250" }] });
    expect(graphEvents()[2].data.delta).toMatchObject({ full: false, nodes: [{ id: "n251" }] });
  });

  it("does not associate a pre-reset snapshot with the new generation when reads overlap reset", async () => {
    const { kv, write, graphEvents } = await setup();
    await write(snapshot(250));
    const original = kv.get.bind(kv);
    let entered!: () => void;
    let release!: () => void;
    const ready = new Promise<void>((r) => { entered = r; });
    const gate = new Promise<void>((r) => { release = r; });
    let pause = true;
    vi.spyOn(kv, "get").mockImplementation(async (scope, key) => {
      const value = await original(scope, key);
      if (scope === KV.graphSnapshot && pause) { pause = false; entered(); await gate; }
      return value;
    });
    const writeBeforeReset = write(snapshot(251));
    await ready;
    const oldGeneration = await original(KV.graphControl, "generation");
    const reset = resetGraphGeneration(kv, snapshot(0));
    // Wait for publication, while its notification remains queued behind the old read.
    await vi.waitFor(async () => expect(await original(KV.graphControl, "generation")).not.toBe(oldGeneration));
    release();
    await Promise.all([writeBeforeReset, reset]);
    for (const event of graphEvents().slice(1)) {
      expect(event.data).toMatchObject({ reset: true, stats: { totalNodes: 0 }, delta: { full: true, nodes: [] } });
    }
  });
});
