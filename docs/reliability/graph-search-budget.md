# Graph enrichment budget

Hybrid search allows 500 ms for optional graph enrichment after keyword and vector retrieval. When that timer expires, it returns the available keyword/vector results without graph context and records a warning at most once per minute. A subsequent search can use graph enrichment again.

Cancellation prevents a late node response from starting the edge read and prevents late responses from starting graph construction. Construction and candidate/provenance scans yield periodically so timers and other requests can run. Context text is bounded, and direct entity hits are considered before neighbor provenance consumes the traversal budget. Each request retains its own pinned graph view; there is no persistent cache that can outlive an import, reset, mesh update or restore.

This is a latency mitigation, not a strict end-to-end response limit. The pinned engine's state list operation enumerates an entire scope and does not accept pagination. An already-started engine read cannot be cancelled by this timer, and response decoding can delay timers. Concurrent independent searches can still cause multiple reads. Large stores therefore still require bounded engine reads or a maintained retrieval index before the cold-read scalability problem is resolved.

`AGENTMEMORY_GRAPH_WEIGHT=0` skips graph enrichment entirely while preserving keyword and configured vector search. It reduces graph recall and is an operational workaround, not a replacement for the storage-level fix.
