// Windowed fan-out for KV round-trips.
//
// A bare Promise.all over N keys puts N invocations in flight against a
// single state adapter at once. Their responses then land together and
// their frame parses run back-to-back with nothing between them, so a
// pile of individually small parses becomes one contiguous block of
// synchronous work — long enough for the engine to miss the worker
// heartbeat and kill the in-flight call (upstream #1124, #1100, #890;
// see also the note in src/state/schema.ts about a single oversized
// frame doing the same thing on its own).
//
// Windowing bounds peak in-flight work, and the yield between windows
// hands the event loop back so the heartbeat can actually be serviced.

export function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => {
    setImmediate(resolve);
  });
}

export async function mapBounded<T, R>(
  items: readonly T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const width = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 1;
  const results = new Array<R>(items.length);

  for (let start = 0; start < items.length; start += width) {
    const window = items.slice(start, start + width);
    const settled = await Promise.all(
      window.map((item, offset) => fn(item, start + offset)),
    );
    for (let offset = 0; offset < settled.length; offset++) {
      results[start + offset] = settled[offset];
    }
    if (start + width < items.length) await yieldToEventLoop();
  }

  return results;
}
