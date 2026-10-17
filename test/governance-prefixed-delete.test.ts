import { describe, it, expect, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { registerGovernanceFunction } from "../src/functions/governance.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  return {
    get: async <T>(scope: string, key: string): Promise<T | null> =>
      (store.get(scope)?.get(key) as T) ?? null,
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
      return data;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      store.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> =>
      Array.from(store.get(scope)?.values() ?? []) as T[],
  };
}

function mockSdk() {
  const fns = new Map<string, Function>();
  return {
    registerFunction: (idOrOpts: string | { id: string }, handler: Function) => {
      fns.set(typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id, handler);
    },
    registerTrigger: () => {},
    trigger: async (input: { function_id: string; payload: unknown }) =>
      fns.get(input.function_id)!(input.payload),
  };
}

describe("governance delete of non-memory records", () => {
  it("deletes insights, lessons, actions, crystals and checkpoints by id prefix", async () => {
    const kv = mockKV();
    const sdk = mockSdk();
    registerGovernanceFunction(sdk as never, kv as never);
    const records: Array<[string, string]> = [
      ["mem:insights", "ins_v1_abc"],
      ["mem:lessons", "lsn_1"],
      ["mem:actions", "act_1"],
      ["mem:crystals", "crys_1"],
      ["mem:checkpoints", "ckpt_1"],
    ];
    for (const [scope, id] of records) await kv.set(scope, id, { id });

    const result = (await sdk.trigger({
      function_id: "mem::governance-delete",
      payload: { memoryIds: [...records.map(([, id]) => id), "ins_missing"], reason: "cleanup" },
    })) as { deleted: number; notFound: string[] };

    expect(result.deleted).toBe(5);
    expect(result.notFound).toEqual(["ins_missing"]);
    for (const [scope, id] of records) expect(await kv.get(scope, id)).toBeNull();
  });

  it("leaves a record alone when its prefix names another collection", async () => {
    const kv = mockKV();
    const sdk = mockSdk();
    registerGovernanceFunction(sdk as never, kv as never);
    await kv.set("mem:lessons", "ins_lookalike", { id: "ins_lookalike" });

    const result = (await sdk.trigger({
      function_id: "mem::governance-delete",
      payload: { memoryIds: ["ins_lookalike"] },
    })) as { deleted: number };

    expect(result.deleted).toBe(0);
    expect(await kv.get("mem:lessons", "ins_lookalike")).not.toBeNull();
  });
});
