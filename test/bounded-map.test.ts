import { describe, it, expect } from "vitest";

import { mapBounded, yieldToEventLoop } from "../src/utils/bounded-map.js";

function concurrencyTracker() {
  let inFlight = 0;
  let peak = 0;
  return {
    get peak() {
      return peak;
    },
    async run<T>(value: T): Promise<T> {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 1));
      inFlight--;
      return value;
    },
  };
}

describe("mapBounded", () => {
  it("returns results in input order regardless of completion order", async () => {
    const out = await mapBounded([1, 2, 3, 4, 5], 2, async (n) => {
      await new Promise((resolve) => setTimeout(resolve, (6 - n) * 2));
      return n * 10;
    });

    expect(out).toEqual([10, 20, 30, 40, 50]);
  });

  it("passes the original index to the callback", async () => {
    const seen: Array<[string, number]> = [];

    await mapBounded(["a", "b", "c", "d", "e"], 2, async (item, index) => {
      seen.push([item, index]);
      return item;
    });

    expect(seen).toEqual([
      ["a", 0],
      ["b", 1],
      ["c", 2],
      ["d", 3],
      ["e", 4],
    ]);
  });

  it("never runs more calls at once than the limit allows", async () => {
    const tracker = concurrencyTracker();
    const items = Array.from({ length: 20 }, (_, i) => i);

    await mapBounded(items, 3, (n) => tracker.run(n));

    expect(tracker.peak).toBeLessThanOrEqual(3);
  });

  // The probe is setImmediate rather than setTimeout on purpose. A yield
  // is meant to let the worker's WebSocket heartbeat be serviced, and
  // socket callbacks run in the poll phase, which is the phase check
  // (setImmediate) follows. A pending setTimeout proves nothing here:
  // timers is a different phase and can be missed on the way to check.
  function immediatePump() {
    let ticks = 0;
    let running = true;
    const step = () => {
      if (!running) return;
      ticks++;
      setImmediate(step);
    };
    setImmediate(step);
    return {
      stop(): number {
        running = false;
        return ticks;
      },
    };
  }

  it("releases the event loop between windows", async () => {
    const pump = immediatePump();

    await mapBounded([1, 2, 3, 4, 5, 6], 2, async (n) => n);

    // 6 items at width 2 is 3 windows, so 2 yields — the pump must have
    // been able to run inside each of them.
    expect(pump.stop()).toBeGreaterThanOrEqual(2);
  });

  it("a bare Promise.all over the same work never releases the loop", async () => {
    const pump = immediatePump();

    await Promise.all([1, 2, 3, 4, 5, 6].map(async (n) => n));

    // Control for the test above: without windowing there is nothing to
    // yield between, so the pump gets no chance to run at all.
    expect(pump.stop()).toBe(0);
  });

  it("propagates a rejection to the caller", async () => {
    await expect(
      mapBounded([1, 2, 3], 2, async (n) => {
        if (n === 2) throw new Error("boom");
        return n;
      }),
    ).rejects.toThrow("boom");
  });

  it("returns an empty array for empty input", async () => {
    expect(await mapBounded([], 4, async () => 1)).toEqual([]);
  });

  it("falls back to serial execution when the limit is not a positive number", async () => {
    const tracker = concurrencyTracker();

    await mapBounded([1, 2, 3, 4], 0, (n) => tracker.run(n));

    expect(tracker.peak).toBe(1);
  });

  it("yieldToEventLoop lets an already-queued callback run first", async () => {
    let observerRan = false;
    setImmediate(() => {
      observerRan = true;
    });

    await yieldToEventLoop();

    expect(observerRan).toBe(true);
  });
});
