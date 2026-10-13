#!/usr/bin/env node
import { execSync } from "node:child_process";
import { realpathSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { REST_URL, authHeaders, captureObservation, isDrainChild, runDrainChild, withEventId } from "./_capture.mjs";
//#region src/hooks/_project.ts
function resolveProject(cwd) {
	const explicit = process.env["AGENTMEMORY_PROJECT_NAME"];
	if (explicit && explicit.trim()) return explicit.trim();
	const dir = cwd && cwd.trim() ? cwd : process.cwd();
	try {
		const [top, gitDir, commonDir] = execSync("git rev-parse --show-toplevel --git-dir --git-common-dir", {
			cwd: dir,
			stdio: [
				"ignore",
				"pipe",
				"ignore"
			],
			timeout: 500
		}).toString().trim().split(/\r?\n/);
		if (top) return basename(repositoryRoot(dir, top, gitDir, commonDir));
	} catch {}
	return basename(dir);
}
function repositoryRoot(dir, top, gitDir, commonDir) {
	if (!gitDir || !commonDir) return top;
	const physical = realpathSync(dir);
	const common = resolve(physical, commonDir);
	if (resolve(physical, gitDir) === common || basename(common) !== ".git") return top;
	return dirname(common);
}
function hookCwd(data) {
	if (!data || typeof data !== "object") return void 0;
	if (typeof data.cwd === "string" && data.cwd.trim()) return data.cwd;
	const roots = data.workspace_roots;
	if (Array.isArray(roots)) {
		for (const root of roots) if (typeof root === "string" && root.trim()) return root;
	}
	const projectDir = process.env["DEVIN_PROJECT_DIR"] || process.env["CLAUDE_PROJECT_DIR"];
	if (projectDir && projectDir.trim()) return projectDir;
}
//#endregion
//#region src/functions/privacy.ts
const PRIVATE_TAG_RE = /<private>[\s\S]*?<\/private>/gi;
const SECRET_PATTERN_SOURCES = [
	/(?:api[_-]?key|secret|token|password|credential|auth)[\s]*[=:]\s*["']?[A-Za-z0-9_\-/.+]{20,}["']?/gi,
	/Bearer\s+[A-Za-z0-9._\-+/=]{20,}/gi,
	/(?<![\p{L}\p{N}\p{M}_])sk-proj-[A-Za-z0-9\-_]{20,}/gu,
	/(?<![\p{L}\p{N}\p{M}_])(?:sk|pk|rk|ak)-[A-Za-z0-9][A-Za-z0-9\-_]{19,}/gu,
	/(?<![\p{L}\p{N}\p{M}_])sk-ant-[A-Za-z0-9\-_]{20,}/gu,
	/gh[pus]_[A-Za-z0-9]{36,}/g,
	/github_pat_[A-Za-z0-9_]{22,}/g,
	/xoxb-[A-Za-z0-9\-]+/g,
	/AKIA[0-9A-Z]{16}/g,
	/AIza[A-Za-z0-9\-_]{35}/g,
	/eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}/g,
	/npm_[A-Za-z0-9]{36}/g,
	/glpat-[A-Za-z0-9\-_]{20,}/g,
	/dop_v1_[A-Za-z0-9]{64}/g
];
const PRIVATE_KEY_BLOCK_RE = /-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----[\s\S]*?(?:-----END [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----|$)/g;
const URL_CREDENTIALS_RE = /\b([a-z][a-z0-9+.-]*:\/\/)[^\s/?#@:"'<>]*:[^\s/?#@"'<>]+@/gi;
function stripPrivateData(input) {
	let result = input.replace(PRIVATE_TAG_RE, "[REDACTED]");
	result = result.replace(new RegExp(PRIVATE_KEY_BLOCK_RE.source, PRIVATE_KEY_BLOCK_RE.flags), "[REDACTED_SECRET]");
	result = result.replace(new RegExp(URL_CREDENTIALS_RE.source, URL_CREDENTIALS_RE.flags), "$1[REDACTED_SECRET]@");
	for (const source of SECRET_PATTERN_SOURCES) {
		const pattern = new RegExp(source.source, source.flags);
		result = result.replace(pattern, "[REDACTED_SECRET]");
	}
	return result;
}
//#endregion
//#region src/hooks/stop.ts
function isSdkChildContext(payload) {
	if (process.env["AGENTMEMORY_SDK_CHILD"] === "1") return true;
	if (!payload || typeof payload !== "object") return false;
	return payload.entrypoint === "sdk-ts";
}
const OBSERVE_TIMEOUT_MS = 2e3;
const EXIT_CAP_MS = 3500;
const END_CAP_MS = 1500;
const REPLY_MAX_CHARS = 4e3;
function boundReply(reply) {
	const safe = stripPrivateData(reply);
	if (safe.length <= REPLY_MAX_CHARS) return {
		text: safe,
		truncated: false
	};
	let head = Math.ceil((REPLY_MAX_CHARS - 1) / 2);
	let tail = safe.length - (REPLY_MAX_CHARS - 1 - head);
	if (/[\uD800-\uDBFF]/.test(safe[head - 1])) head--;
	if (/[\uDC00-\uDFFF]/.test(safe[tail])) tail++;
	return {
		text: `${safe.slice(0, head)}\u2026${safe.slice(tail)}`,
		truncated: true
	};
}
async function main() {
	if (isDrainChild()) return runDrainChild();
	let input = "";
	for await (const chunk of process.stdin) input += chunk;
	let data;
	try {
		data = JSON.parse(input);
	} catch {
		return;
	}
	if (!data || typeof data !== "object") return;
	if (isSdkChildContext(data)) return;
	const sessionId = data.session_id || data.sessionId || data.conversation_id || "unknown";
	const reply = data.last_assistant_message;
	if (typeof reply === "string" && reply.trim()) {
		const capture = setTimeout(() => process.exit(0), EXIT_CAP_MS).unref();
		const cwd = hookCwd(data) || process.cwd();
		const bounded = boundReply(reply);
		await captureObservation(withEventId({
			hookType: "stop",
			sessionId,
			project: resolveProject(cwd),
			cwd,
			timestamp: (/* @__PURE__ */ new Date()).toISOString(),
			data: {
				last_assistant_message: bounded.text,
				...bounded.truncated ? { last_assistant_message_truncated: true } : {}
			}
		}, {}), OBSERVE_TIMEOUT_MS).catch(() => {});
		clearTimeout(capture);
	}
	setTimeout(() => process.exit(0), END_CAP_MS).unref();
	fetch(`${REST_URL}/agentmemory/session/end`, {
		method: "POST",
		headers: authHeaders(),
		body: JSON.stringify({
			sessionId,
			final: false
		}),
		signal: AbortSignal.timeout(5e3)
	}).catch(() => {});
}
main().catch(() => process.exit(0));
//#endregion
export {};

//# sourceMappingURL=stop.mjs.map