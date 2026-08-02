// Fake StateKV that records how many calls are in flight simultaneously,
// so a test can assert an operation bounds its fan-out instead of firing
// every key at the adapter at once. latencyMs must be non-zero for the
// watermark to mean anything — with instant resolution nothing overlaps.

export type InstrumentedKvOptions = {
  latencyMs?: number;
};

export function instrumentedKV(options: InstrumentedKvOptions = {}) {
  const latencyMs = options.latencyMs ?? 1;
  const store = new Map<string, Map<string, unknown>>();
  let inFlight = 0;
  let peakInFlight = 0;
  let callCount = 0;

  async function enter<T>(produce: () => T): Promise<T> {
    callCount++;
    inFlight++;
    peakInFlight = Math.max(peakInFlight, inFlight);
    try {
      await new Promise((resolve) => setTimeout(resolve, latencyMs));
      return produce();
    } finally {
      inFlight--;
    }
  }

  return {
    get peakInFlight() {
      return peakInFlight;
    },
    get callCount() {
      return callCount;
    },
    resetInstrumentation(): void {
      inFlight = 0;
      peakInFlight = 0;
      callCount = 0;
    },
    seed<T>(scope: string, key: string, data: T): void {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
    },
    get: <T>(scope: string, key: string): Promise<T | null> =>
      enter(() => (store.get(scope)?.get(key) as T) ?? null),
    set: <T>(scope: string, key: string, data: T): Promise<T> =>
      enter(() => {
        if (!store.has(scope)) store.set(scope, new Map());
        store.get(scope)!.set(key, data);
        return data;
      }),
    delete: (scope: string, key: string): Promise<void> =>
      enter(() => {
        store.get(scope)?.delete(key);
      }),
    list: <T>(scope: string): Promise<T[]> =>
      enter(() => {
        const entries = store.get(scope);
        return entries ? (Array.from(entries.values()) as T[]) : [];
      }),
  };
}
