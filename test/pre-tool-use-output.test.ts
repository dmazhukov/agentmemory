import { describe, expect, it } from "vitest";
import { join, resolve } from "node:path";
import { createServer } from "node:http";
import { spawn } from "node:child_process";

const script = join(resolve(__dirname, ".."), "plugin", "scripts", "pre-tool-use.mjs");
const CONTEXT = '<agentmemory-file-context file="src/foo.ts">remembered "quoted"\nline</agentmemory-file-context>';

const HOST_ENV_KEYS = [
  "AGENTMEMORY_INJECT_CONTEXT",
  "AGENTMEMORY_SDK_CHILD",
  "DEVIN_PROJECT_DIR",
  "FACTORY_PROJECT_DIR",
  "DROID_PLUGIN_ROOT",
];

async function runPreToolUse(
  payload: Record<string, unknown>,
  env: Record<string, string> = {},
  context: string = CONTEXT,
): Promise<{ stdout: string; requests: number }> {
  let requests = 0;
  const server = createServer((req, res) => {
    requests += 1;
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ context }));
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("test server did not bind to a TCP port");
  }
  const baseEnv: Record<string, string | undefined> = { ...process.env };
  for (const k of HOST_ENV_KEYS) delete baseEnv[k];
  try {
    const child = spawn(process.execPath, [script], {
      env: {
        ...baseEnv,
        AGENTMEMORY_URL: `http://127.0.0.1:${address.port}`,
        AGENTMEMORY_SECRET: "",
        ...env,
      },
      stdio: ["pipe", "pipe", "ignore"],
    });
    let stdout = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stdin.end(JSON.stringify(payload));
    await new Promise<void>((resolveExit, reject) => {
      const timeout = setTimeout(() => {
        child.kill();
        reject(new Error("pre-tool-use timed out"));
      }, 5000);
      child.on("error", reject);
      child.on("close", () => {
        clearTimeout(timeout);
        resolveExit();
      });
    });
    return { stdout, requests };
  } finally {
    await new Promise<void>((r) => server.close(() => r()));
  }
}

const hookPayload = {
  session_id: "ses-1",
  cwd: "/repo",
  permission_mode: "default",
  hook_event_name: "PreToolUse",
  tool_name: "Read",
  tool_input: { file_path: "/repo/src/foo.ts" },
  tool_use_id: "toolu_1",
};

describe("pre-tool-use stdout shape", () => {
  it("wraps context in the structured PreToolUse envelope for hook_event_name payloads", async () => {
    const { stdout, requests } = await runPreToolUse(hookPayload, {
      AGENTMEMORY_INJECT_CONTEXT: "true",
    });
    expect(requests).toBe(1);
    expect(JSON.parse(stdout)).toEqual({
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        additionalContext: CONTEXT,
      },
    });
  });

  it("prints nothing when injection is off", async () => {
    const { stdout, requests } = await runPreToolUse(hookPayload);
    expect(requests).toBe(0);
    expect(stdout).toBe("");
  });

  it("prints nothing when the server returns empty context", async () => {
    const { stdout, requests } = await runPreToolUse(
      hookPayload,
      { AGENTMEMORY_INJECT_CONTEXT: "true" },
      "",
    );
    expect(requests).toBe(1);
    expect(stdout).toBe("");
  });

  it("keeps plain text for payloads without hook_event_name", async () => {
    const { stdout } = await runPreToolUse(
      { sessionId: "ses-2", toolName: "read", toolArgs: { path: "src/foo.ts" } },
      { AGENTMEMORY_INJECT_CONTEXT: "true" },
    );
    expect(stdout).toBe(CONTEXT);
  });

  it("keeps plain text for the camelCase preToolUse payload", async () => {
    const { stdout } = await runPreToolUse(
      {
        conversation_id: "c-1",
        hook_event_name: "preToolUse",
        cursor_version: "1.0",
        tool_name: "Read",
        tool_input: { file_path: "/repo/src/foo.ts" },
      },
      { AGENTMEMORY_INJECT_CONTEXT: "true" },
    );
    expect(stdout).toBe(CONTEXT);
  });

  it.each([
    ["FACTORY_PROJECT_DIR", "/repo"],
    ["DROID_PLUGIN_ROOT", "/plugin"],
    ["DEVIN_PROJECT_DIR", "/repo"],
  ])("keeps plain text when %s is set", async (key, value) => {
    const { stdout } = await runPreToolUse(hookPayload, {
      AGENTMEMORY_INJECT_CONTEXT: "true",
      [key]: value,
    });
    expect(stdout).toBe(CONTEXT);
  });
});
