import { pinGraphState } from "../state/graph-generation.js";
import { setImmediate } from "node:timers/promises";
import type {
  GraphNode,
  GraphEdge,
} from "../types.js";
import { matchesSearchLayer, type SearchLayer, type SearchLayerResolver } from "../state/search-layer.js";
import { KV } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";

export interface GraphRetrievalResult {
  obsId: string;
  sessionId: string;
  score: number;
  graphContext: string;
  pathLength: number;
}

function buildGraphContext(
  path: Array<{ node: GraphNode; edge?: GraphEdge }>,
): string {
  const parts: string[] = [];
  for (const step of path) {
    const properties: string[] = [];
    for (const key in step.node.properties) {
      if (!Object.hasOwn(step.node.properties, key)) continue;
      const value = step.node.properties[key];
      const text = typeof value === "string" ? value.slice(0, 256)
        : typeof value === "number" || typeof value === "boolean" ? String(value) : "[value]";
      properties.push(`${key.slice(0, 64)}=${text}`);
      if (properties.length === 3) break;
    }
    const props = properties.join(", ");
    let line = `[${step.node.type}] ${step.node.name.slice(0, 256)}`;
    if (props) line += ` (${props})`;
    if (step.edge) {
      line += ` --${step.edge.type}-->`;
      if (typeof step.edge.context?.reasoning === "string" && step.edge.context.reasoning) {
        line += ` [${step.edge.context.reasoning.slice(0, 512)}]`;
      }
      if (step.edge.tvalid) {
        line += ` @${step.edge.tvalid}`;
      }
    }
    parts.push(line);
  }
  return parts.join(" ").slice(0, 4096);
}

const MAX_START_NODES = 25;
const MAX_DISCOVERED_NODES = 2000;
const MAX_EXAMINED_EDGES = 10_000;
const MAX_EXAMINED_OBSERVATIONS = 10_000;

type GraphView = {
  nodes: GraphNode[];
  edges: GraphEdge[];
  byId: Map<string, GraphNode>;
  adjacency: Map<string, Array<{ neighborId: string; edge: GraphEdge }>>;
};
type TraversalBudget = { nodes: number; edges: number };

function compareText(a: string, b: string): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

async function readGraph(kv: StateKV, signal: AbortSignal): Promise<GraphView> {
  const pinned = await pinGraphState(kv);
  signal.throwIfAborted();
  const rawNodes = await pinned.list<GraphNode>(KV.graphNodes);
  signal.throwIfAborted();
  const rawEdges = await pinned.list<GraphEdge>(KV.graphEdges);
  signal.throwIfAborted();
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const byId = new Map<string, GraphNode>();
  let processed = 0;
  for (const node of rawNodes) {
    if (++processed % 256 === 0) await setImmediate();
    signal.throwIfAborted();
    if (node.stale) continue;
    nodes.push(node);
    byId.set(node.id, node);
  }
  const adjacency: GraphView["adjacency"] = new Map();
  for (const edge of rawEdges) {
    if (++processed % 256 === 0) await setImmediate();
    signal.throwIfAborted();
    if (edge.stale) continue;
    edges.push(edge);
    if (!byId.has(edge.sourceNodeId) || !byId.has(edge.targetNodeId)) continue;
    for (const [from, to] of [[edge.sourceNodeId, edge.targetNodeId], [edge.targetNodeId, edge.sourceNodeId]]) {
      let neighbors = adjacency.get(from);
      if (!neighbors) adjacency.set(from, neighbors = []);
      neighbors.push({ neighborId: to, edge });
    }
  }
  for (const neighbors of adjacency.values()) {
    if (++processed % 256 === 0) await setImmediate();
    signal.throwIfAborted();
    neighbors.sort((a, b) =>
      b.edge.weight - a.edge.weight ||
      compareText(a.neighborId, b.neighborId) ||
      compareText(a.edge.id, b.edge.id),
    );
  }
  return { nodes, edges, byId, adjacency };
}

export class GraphRetrieval {
  constructor(private kv: StateKV, private resolveLayer?: SearchLayerResolver) {}

  // The caller owns this query's lifetime; never retain its view across searches.
  createQuery(): GraphQuery {
    const controller = new AbortController();
    let view: Promise<GraphView> | undefined;
    return new GraphQuery(() => view ??= readGraph(this.kv, controller.signal), this.resolveLayer, controller);
  }

  searchByEntities(...args: Parameters<GraphQuery["searchByEntities"]>): Promise<GraphRetrievalResult[]> {
    return this.createQuery().searchByEntities(...args);
  }

  expandFromChunks(...args: Parameters<GraphQuery["expandFromChunks"]>): Promise<GraphRetrievalResult[]> {
    return this.createQuery().expandFromChunks(...args);
  }

  temporalQuery(...args: Parameters<GraphQuery["temporalQuery"]>): ReturnType<GraphQuery["temporalQuery"]> {
    return this.createQuery().temporalQuery(...args);
  }
}

export class GraphQuery {
  constructor(
    private loadGraph: () => Promise<GraphView>,
    private resolveLayer?: SearchLayerResolver,
    private controller = new AbortController(),
  ) {}

  cancel(): void {
    this.controller.abort();
  }

  async searchByEntities(
    entityNames: string[],
    maxDepth = 2,
    maxResults = 20,
    targetLayer: SearchLayer = "all",
  ): Promise<GraphRetrievalResult[]> {
    const exactNames = new Set(entityNames.map((name) => name.trim().toLowerCase()).filter(Boolean));
    const names = [...exactNames];
    if (names.length === 0 || maxResults <= 0) return [];
    const graph = await this.loadGraph();
    this.controller.signal.throwIfAborted();
    const candidates: Array<{ node: GraphNode; name: string }> = [];
    let examined = 0;
    for (const node of graph.nodes) {
      if (++examined % 256 === 0) await setImmediate();
      this.controller.signal.throwIfAborted();
      const name = node.name.toLowerCase();
      if (!name || !names.some((entity) => name.includes(entity) || entity.includes(name))) continue;
      candidates.push({ node, name });
      candidates.sort((a, b) =>
        Number(exactNames.has(b.name)) - Number(exactNames.has(a.name)) ||
        compareText(a.name, b.name) || compareText(a.node.id, b.node.id));
      if (candidates.length > MAX_START_NODES) candidates.pop();
    }
    const matchingNodes = candidates.map(({ node }) => node);

    if (matchingNodes.length === 0) return [];

    const results: GraphRetrievalResult[] = [];
    const visitedObs = new Set<string>();

    const budget = { nodes: MAX_DISCOVERED_NODES, edges: MAX_EXAMINED_EDGES };
    let examinedObservations = 0;
    for (const node of matchingNodes) {
      for (const obsId of node.sourceObservationIds) {
        if (++examinedObservations > MAX_EXAMINED_OBSERVATIONS) break;
        if (examinedObservations % 256 === 0) await setImmediate();
        this.controller.signal.throwIfAborted();
        if (visitedObs.has(obsId) || !matchesSearchLayer(obsId, "", targetLayer, this.resolveLayer?.(obsId, ""))) continue;
        visitedObs.add(obsId);
        results.push({ obsId, sessionId: "", score: 1.0,
          graphContext: `[${node.type}] ${node.name.slice(0, 256)}`, pathLength: 0 });
      }
    }
    for (const startNode of matchingNodes) {
      const paths = this.dijkstraTraversal(startNode, graph, maxDepth, budget);

      for (const path of paths) {
        const lastNode = path[path.length - 1].node;
        const graphContext = buildGraphContext(path);
        const pathLength = path.length;
        const edgeWeights = path.filter((step) => step.edge).map((step) => step.edge!.weight);
        const avgWeight = edgeWeights.length > 0
          ? edgeWeights.reduce((a, b) => a + b, 0) / edgeWeights.length : 0.5;
        const score = avgWeight / pathLength;
        for (const obsId of lastNode.sourceObservationIds) {
          if (++examinedObservations > MAX_EXAMINED_OBSERVATIONS) break;
          if (examinedObservations % 256 === 0) await setImmediate();
          this.controller.signal.throwIfAborted();
          if (visitedObs.has(obsId) || !matchesSearchLayer(obsId, "", targetLayer, this.resolveLayer?.(obsId, ""))) continue;
          visitedObs.add(obsId);

          results.push({
            obsId,
            sessionId: "",
            score,
            graphContext,
            pathLength,
          });
        }
      }

    }

    results.sort((a, b) => b.score - a.score);
    return results.slice(0, maxResults);
  }

  async expandFromChunks(
    obsIds: string[],
    maxDepth = 1,
    maxResults = 10,
    targetLayer: SearchLayer = "all",
  ): Promise<GraphRetrievalResult[]> {
    if (obsIds.length === 0 || maxResults <= 0) return [];
    const graph = await this.loadGraph();
    this.controller.signal.throwIfAborted();
    const wanted = new Set(obsIds);
    const linkedNodes: GraphNode[] = [];
    let examined = 0;
    for (const node of graph.nodes) {
      if (++examined % 256 === 0) await setImmediate();
      this.controller.signal.throwIfAborted();
      let linked = false;
      for (const id of node.sourceObservationIds) {
        if (++examined % 256 === 0) await setImmediate();
        this.controller.signal.throwIfAborted();
        if (wanted.has(id)) { linked = true; break; }
      }
      if (!linked) continue;
      linkedNodes.push(node);
      linkedNodes.sort((a, b) => compareText(a.id, b.id));
      if (linkedNodes.length > MAX_START_NODES) linkedNodes.pop();
    }

    const results: GraphRetrievalResult[] = [];
    const visitedObs = new Set<string>(obsIds);

    const budget = { nodes: MAX_DISCOVERED_NODES, edges: MAX_EXAMINED_EDGES };
    let examinedObservations = 0;
    for (const node of linkedNodes) {
      const paths = this.dijkstraTraversal(node, graph, maxDepth, budget);
      for (const path of paths) {
        const lastNode = path[path.length - 1].node;
        const graphContext = buildGraphContext(path);
        for (const obsId of lastNode.sourceObservationIds) {
          if (++examinedObservations > MAX_EXAMINED_OBSERVATIONS) break;
          if (examinedObservations % 256 === 0) await setImmediate();
          this.controller.signal.throwIfAborted();
          if (visitedObs.has(obsId) || !matchesSearchLayer(obsId, "", targetLayer, this.resolveLayer?.(obsId, ""))) continue;
          visitedObs.add(obsId);

          const pathLength = path.length;
          const score = 0.5 * (1 / (pathLength + 1));

          results.push({
            obsId,
            sessionId: "",
            score,
            graphContext,
            pathLength,
          });
        }
      }
    }

    results.sort((a, b) => b.score - a.score);
    return results.slice(0, maxResults);
  }

  async temporalQuery(
    entityName: string,
    asOf?: string,
  ): Promise<{
    entity: GraphNode | null;
    currentState: GraphEdge[];
    history: GraphEdge[];
  }> {
    const { nodes: allNodes, edges: allEdges } = await this.loadGraph();

    const entity = allNodes.find(
      (n) => n.name.toLowerCase() === entityName.toLowerCase(),
    );
    if (!entity) return { entity: null, currentState: [], history: [] };

    const relatedEdges = allEdges.filter(
      (e) => e.sourceNodeId === entity.id || e.targetNodeId === entity.id,
    );

    if (!asOf) {
      const latestEdges = this.getLatestEdges(relatedEdges);
      const historicalEdges = relatedEdges.filter(
        (e) => !latestEdges.some((le) => le.id === e.id),
      );
      return { entity, currentState: latestEdges, history: historicalEdges };
    }

    const asOfDate = new Date(asOf).getTime();
    const validEdges = relatedEdges.filter((e) => {
      const commitDate = new Date(e.tcommit || e.createdAt).getTime();
      if (commitDate > asOfDate) return false;
      if (e.tvalid) {
        const validDate = new Date(e.tvalid).getTime();
        if (validDate > asOfDate) return false;
      }
      if (e.tvalidEnd) {
        const endDate = new Date(e.tvalidEnd).getTime();
        if (endDate < asOfDate) return false;
      }
      return true;
    });

    return {
      entity,
      currentState: this.getLatestEdges(validEdges),
      history: validEdges,
    };
  }

  private getLatestEdges(edges: GraphEdge[]): GraphEdge[] {
    const byKey = new Map<string, GraphEdge[]>();
    for (const e of edges) {
      const key = `${e.sourceNodeId}|${e.targetNodeId}|${e.type}`;
      if (!byKey.has(key)) byKey.set(key, []);
      byKey.get(key)!.push(e);
    }

    const latest: GraphEdge[] = [];
    for (const group of byKey.values()) {
      if (group.length === 0) continue;
      group.sort(
        (a, b) =>
          new Date(b.tcommit || b.createdAt).getTime() -
          new Date(a.tcommit || a.createdAt).getTime(),
      );
      const newest = group.find((e) => e.isLatest !== false) || group[0];
      latest.push(newest);
    }
    return latest;
  }

  private dijkstraTraversal(
    startNode: GraphNode,
    graph: GraphView,
    maxDepth: number,
    budget: TraversalBudget,
  ): Array<Array<{ node: GraphNode; edge?: GraphEdge }>> {
    if (budget.nodes <= 0 || budget.edges <= 0 || maxDepth <= 0) return [];
    budget.nodes--;
    const { byId: nodeIndex, adjacency } = graph;

    const dist = new Map<string, number>();
    const pathTo = new Map<string, Array<{ node: GraphNode; edge?: GraphEdge }>>();
    dist.set(startNode.id, 0);
    pathTo.set(startNode.id, [{ node: startNode }]);

    const heap = new MinHeap<{ nodeId: string; depth: number; cost: number }>(
      (a, b) => a.cost - b.cost,
    );
    heap.push({ nodeId: startNode.id, depth: 0, cost: 0 });

    while (heap.size() > 0 && budget.edges > 0) {
      const { nodeId, depth, cost } = heap.pop()!;
      // Skip stale heap entries (cost beaten by a later push).
      if (cost > (dist.get(nodeId) ?? Infinity)) continue;
      if (depth >= maxDepth) continue;

      const neighbors = adjacency.get(nodeId) ?? [];
      for (const { neighborId, edge } of neighbors) {
        if (budget.edges <= 0) break;
        budget.edges--;
        const nextNode = nodeIndex.get(neighborId);
        if (!nextNode) continue;
        // Clamp weight to avoid division-by-zero on malformed edges;
        // 0.01 is below the documented 0.1 floor.
        const edgeCost = 1 / Math.max(edge.weight, 0.01);
        const newCost = cost + edgeCost;
        if (newCost < (dist.get(neighborId) ?? Infinity)) {
          if (!dist.has(neighborId)) {
            if (budget.nodes <= 0) continue;
            budget.nodes--;
          }
          dist.set(neighborId, newCost);
          pathTo.set(neighborId, [
            ...pathTo.get(nodeId)!,
            { node: nextNode, edge },
          ]);
          heap.push({ nodeId: neighborId, depth: depth + 1, cost: newCost });
        }
      }
    }

    pathTo.delete(startNode.id);
    return Array.from(pathTo.values());
  }
}

class MinHeap<T> {
  private heap: T[] = [];

  constructor(private compare: (a: T, b: T) => number) {}

  size(): number {
    return this.heap.length;
  }

  push(value: T): void {
    this.heap.push(value);
    this.bubbleUp(this.heap.length - 1);
  }

  pop(): T | undefined {
    if (this.heap.length === 0) return undefined;
    const top = this.heap[0];
    const last = this.heap.pop()!;
    if (this.heap.length > 0) {
      this.heap[0] = last;
      this.sinkDown(0);
    }
    return top;
  }

  private bubbleUp(i: number): void {
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.compare(this.heap[i], this.heap[parent]) < 0) {
        [this.heap[i], this.heap[parent]] = [this.heap[parent], this.heap[i]];
        i = parent;
      } else break;
    }
  }

  private sinkDown(i: number): void {
    const n = this.heap.length;
    while (true) {
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      let smallest = i;
      if (left < n && this.compare(this.heap[left], this.heap[smallest]) < 0) {
        smallest = left;
      }
      if (right < n && this.compare(this.heap[right], this.heap[smallest]) < 0) {
        smallest = right;
      }
      if (smallest === i) break;
      [this.heap[i], this.heap[smallest]] = [this.heap[smallest], this.heap[i]];
      i = smallest;
    }
  }
}
