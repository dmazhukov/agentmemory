import { afterEach, describe, expect, it, vi } from "vitest";
import { HybridSearch } from "../src/state/hybrid-search.js";
import { SearchIndex } from "../src/state/search-index.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation } from "../src/types.js";

const observation: CompressedObservation = {
  id: "obs_ready", sessionId: "ses_ready", timestamp: "2026-01-01T00:00:00.000Z",
  type: "discovery", title: "Cobalt authentication", subtitle: "", facts: [],
  narrative: "Cobalt validates tokens", concepts: ["Cobalt"], files: [], importance: 5,
};

function fixture() {
  const index = new SearchIndex();
  index.add(observation);
  let release!: (rows: unknown[]) => void;
  const held = new Promise<unknown[]>((resolve) => { release = resolve; });
  const listed: string[] = [];
  const kv = {
    get: async (scope: string, key: string) =>
      scope === KV.observations(observation.sessionId) && key === observation.id ? observation : null,
    list: async (scope: string) => { listed.push(scope); return held; },
  };
  return { hybrid: new HybridSearch(index, null, null, kv as never), release, listed, kv, index };
}

afterEach(() => vi.useRealTimers());

describe("optional graph retrieval budget", () => {
  it("returns ready keyword results while graph storage is still unresponsive", async () => {
    vi.useFakeTimers();
    const { hybrid, release } = fixture();
    let results: Awaited<ReturnType<HybridSearch["search"]>> | undefined;
    const search = hybrid.search("Cobalt").then((value) => { results = value; });
    try {
      await vi.advanceTimersByTimeAsync(500);
      expect(results?.map((result) => result.observation.id)).toEqual([observation.id]);
      expect(results?.[0].graphScore).toBe(0);
    } finally {
      release([]);
      await search;
    }
  });

  it("does not start the edge read when a timed-out node read arrives late", async () => {
    vi.useFakeTimers();
    const { hybrid, release, listed } = fixture();
    const search = hybrid.search("Cobalt");
    await vi.advanceTimersByTimeAsync(500);
    expect((await search).map((result) => result.observation.id)).toEqual([observation.id]);
    release([]);
    await vi.advanceTimersByTimeAsync(0);
    expect(listed).toEqual([KV.graphNodes]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("keeps fast graph enrichment and clears its deadline", async () => {
    vi.useFakeTimers();
    const { kv, index } = fixture();
    kv.list = async (scope) => scope === KV.graphNodes ? [{
      id: "cobalt", name: "Cobalt", type: "concept", properties: {},
      sourceObservationIds: [observation.id], createdAt: observation.timestamp,
    }] : [];
    const hybrid = new HybridSearch(index, null, null, kv as never);
    const results = await hybrid.search("Cobalt");
    expect(results[0].graphScore).toBeGreaterThan(0);
    expect(results[0].graphContext).toContain("Cobalt");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds concurrent query reformulations and can recover on the next search", async () => {
    vi.useFakeTimers();
    const { hybrid, kv, release } = fixture();
    const search = hybrid.searchWithExpansion("Cobalt", 5, {
      reformulations: ["Cobalt tokens", "Cobalt authentication"],
      temporalConcretizations: [], entityExtractions: ["Cobalt"],
    });
    await vi.advanceTimersByTimeAsync(500);
    expect((await search).map((result) => result.observation.id)).toEqual([observation.id]);
    release([]);
    kv.list = async (scope) => scope === KV.graphNodes ? [{
      id: "cobalt", name: "Cobalt", type: "concept", properties: {},
      sourceObservationIds: [observation.id], createdAt: observation.timestamp,
    }] : [];
    expect((await hybrid.search("Cobalt"))[0].graphScore).toBeGreaterThan(0);
    expect(vi.getTimerCount()).toBe(0);
  });
});
