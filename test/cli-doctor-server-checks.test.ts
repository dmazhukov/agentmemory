import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

const sandboxes: string[] = [];
const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map((server) => new Promise((resolve) => server.close(resolve))),
  );
  for (const path of sandboxes.splice(0)) {
    rmSync(path, { recursive: true, force: true });
  }
});

async function startFakeServer(embeddingProvider: string | undefined): Promise<{ base: string; paths: string[] }> {
  const paths: string[] = [];
  const server = createServer((req, res) => {
    const path = new URL(req.url ?? "/", "http://localhost").pathname;
    paths.push(path);
    const port = (server.address() as AddressInfo).port;
    const bodies: Record<string, unknown> = {
      "/": "ok",
      "/agentmemory/livez": { status: "ok", viewerPort: port },
      "/agentmemory/health": { status: "healthy" },
      "/agentmemory/config/flags": { provider: "noop", embeddingProvider, flags: [] },
      "/agentmemory/graph/stats": { totalNodes: 0 },
    };
    const body = bodies[path];
    res.writeHead(body === undefined ? 404 : 200, { "content-type": "application/json" });
    res.end(JSON.stringify(body ?? {}));
  });
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return { base: `http://127.0.0.1:${port}`, paths };
}

function runDoctor(base: string): Promise<{ code: number | null; output: string }> {
  const root = mkdtempSync(join(tmpdir(), "agentmemory-doctor-"));
  sandboxes.push(root);
  const home = join(root, "home");
  const inherited = Object.fromEntries(
    Object.entries(process.env).filter(([key]) => !/^(AGENTMEMORY_|III_)/.test(key)),
  );
  const child = spawn(
    process.execPath,
    ["--import", "tsx", "src/cli.ts", "doctor", "--dry-run", "--data-dir", join(root, "data")],
    {
      cwd: process.cwd(),
      env: { ...inherited, HOME: home, USERPROFILE: home, NO_COLOR: "1", AGENTMEMORY_URL: base },
    },
  );
  let output = "";
  child.stdout.on("data", (chunk) => (output += chunk));
  child.stderr.on("data", (chunk) => (output += chunk));
  return new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code) => resolve({ code, output: output.replace(/\x1b\[[0-9;]*m/g, "") }));
  });
}

describe("agentmemory doctor embedding check", () => {
  it("passes when the server reports an active embedding provider", async () => {
    const server = await startFakeServer("local (384 dims)");
    const { code, output } = await runDoctor(server.base);

    expect(server.paths).toContain("/agentmemory/config/flags");
    expect(code).toBe(0);
    expect(output).toMatch(/✓ Embedding provider/);
    expect(output).not.toMatch(/✗ Embedding provider/);
    expect(output).not.toContain("Running BM25-only");
  }, 60_000);

  it.each([["none"], [""], [undefined]])("fails with the BM25-only hint when the server reports %j", async (embeddingProvider) => {
    const server = await startFakeServer(embeddingProvider);
    const { code, output } = await runDoctor(server.base);

    expect(server.paths).toContain("/agentmemory/config/flags");
    expect(code).toBe(0);
    expect(output).toMatch(/✗ Embedding provider/);
    expect(output).toContain("Running BM25-only");
  }, 60_000);
});
