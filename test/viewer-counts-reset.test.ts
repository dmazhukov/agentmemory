import { afterEach, describe, expect, it, vi } from "vitest";
import type { StateKV } from "../src/state/kv.js";
import {
  getViewerCounts, invalidateViewerCounts, noteGraphStats,
  resetViewerCounts, setViewerCountsEmitter,
} from "../src/state/viewer-counts.js";

const stats = (totalNodes: number) => ({ totalNodes, totalEdges: 0, nodesByType: {}, edgesByType: {} });

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => { resolve = r; });
  return { promise, resolve };
}

afterEach(() => { resetViewerCounts(); vi.useRealTimers(); });

describe("viewer count invalidation", () => {
  it("reloads reset counts and preserves the live emitter", async () => {
    vi.useFakeTimers();
    let total = 2;
    const kv = { list: async () => [], get: async () => ({ stats: stats(total) }) } as unknown as StateKV;
    const emit = vi.fn();
    setViewerCountsEmitter(emit);
    expect((await getViewerCounts(kv)).graphNodes).toBe(2);
    total = 0;
    invalidateViewerCounts();
    expect((await getViewerCounts(kv)).graphNodes).toBe(0);
    noteGraphStats(stats(1));
    await vi.advanceTimersByTimeAsync(500);
    expect(emit).toHaveBeenLastCalledWith(expect.objectContaining({ graphNodes: 1 }));
  });

  it("does not retain stale counts when invalidated during both load attempts", async () => {
    const entered = [deferred(), deferred()];
    const release = [deferred(), deferred()];
    let reads = 0;
    let total = 2;
    const kv = {
      list: async () => [],
      get: async () => {
        const result = { stats: stats(total) };
        const index = reads++;
        if (index < 2) { entered[index].resolve(); await release[index].promise; }
        return result;
      },
    } as unknown as StateKV;
    const pending = getViewerCounts(kv);
    await entered[0].promise;
    total = 1;
    invalidateViewerCounts();
    release[0].resolve();
    await entered[1].promise;
    total = 0;
    invalidateViewerCounts();
    release[1].resolve();
    await pending;
    expect((await getViewerCounts(kv)).graphNodes).toBe(0);
    expect(reads).toBe(3);
  });
});
