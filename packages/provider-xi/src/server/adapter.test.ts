// @effect-diagnostics nodeBuiltinImport:off
import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";

import { XiSettings } from "../settings.ts";
import { XiAdapterV2Driver } from "./adapter.ts";

const fakeAgent = `
let buffer = "";
const send = (value) => process.stdout.write(JSON.stringify(value) + "\\n");
process.stdin.on("data", (chunk) => {
  buffer += chunk.toString();
  for (;;) {
    const newline = buffer.indexOf("\\n");
    if (newline < 0) return;
    const line = buffer.slice(0, newline).trim();
    buffer = buffer.slice(newline + 1);
    if (line === "") continue;
    const message = JSON.parse(line);
    if (message.method === "initialize") {
      send({ jsonrpc: "2.0", id: message.id, result: { protocolVersion: 1 } });
    } else if (message.method === "session/new") {
      send({ jsonrpc: "2.0", id: message.id, result: { sessionId: "s1", model: "default" } });
    } else if (message.method === "session/close") {
      send({ jsonrpc: "2.0", id: message.id, result: {} });
    } else if (message.method === "session/prompt") {
      send({ jsonrpc: "2.0", id: message.id, result: { accepted: true } });
      send({ jsonrpc: "2.0", method: "session/message", params: { sessionId: "s1", event: "message_end", message: { role: "assistant", content: [{ type: "text", text: "hello from xi" }] } } });
      send({ jsonrpc: "2.0", method: "session/plan", params: { sessionId: "s1", tasks: [{ id: 1, subject: "do it", status: "completed" }] } });
      send({ jsonrpc: "2.0", method: "session/usage", params: { sessionId: "s1", contextTokens: 10, contextOK: true, contextWindow: 100, usage: { Input: 5, Output: 2, CacheRead: 0 } } });
      send({ jsonrpc: "2.0", method: "session/run_done", params: { sessionId: "s1", error: "", errorKind: "", window: "", resetsAt: "" } });
    }
  }
});
`;

const instanceId = ProviderInstanceId.make("xi");
const threadId = ThreadId.make("thread-xi-test");
const providerSessionId = ProviderSessionId.make("provider-session-xi-test");
const runtimePolicy = {
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
} as never;
const modelSelection = { instanceId, model: "default" } as never;

const decodeXiSettings = Schema.decodeSync(XiSettings);

const layer = Layer.mergeAll(
  NodeServices.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);

describe("XiAdapterV2", () => {
  it.effect("opens a session, prompts, and streams the answer", () =>
    Effect.gen(function* () {
      const dir = yield* Effect.tryPromise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "xi-adapter-")),
      );
      const agentFile = NodePath.join(dir, "agent.mjs");
      const wrapper = NodePath.join(dir, "xi");
      yield* Effect.tryPromise(() => NodeFSP.writeFile(agentFile, fakeAgent));
      yield* Effect.tryPromise(() =>
        NodeFSP.writeFile(wrapper, `#!/bin/sh\nexec "${process.execPath}" "${agentFile}"\n`),
      );
      yield* Effect.tryPromise(() => NodeFSP.chmod(wrapper, 0o755));

      const config = decodeXiSettings({
        enabled: true,
        binaryPath: wrapper,
        launchArgs: "",
      });
      const adapter = yield* XiAdapterV2Driver.create({
        instanceId,
        displayName: undefined,
        environment: [],
        enabled: true,
        config,
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
        initialProviderItemIdentityVersion: 2,
      } as never);
      const thread = yield* runtime.ensureThread({
        threadId,
        title: "xi test",
        modelSelection,
        runtimePolicy,
      } as never);
      yield* runtime.startTurn({
        threadId,
        runId: "run-1",
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: "attempt-1",
        rootNodeId: "node-1",
        providerThread: thread,
        message: { messageId: "message-1", text: "hi", attachments: [] },
        modelSelection,
        runtimePolicy,
        appThread: {},
      } as never);
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
      );
      const list = Array.from(events);
      expect(
        list.some(
          (event) =>
            event.type === "message.updated" && event.message.text.includes("hello from xi"),
        ),
      ).toBe(true);
      expect(list.some((event) => event.type === "turn.terminal")).toBe(true);
      expect(
        list.some((event) => {
          const value = event as unknown as {
            type: string;
            turnItem?: { type?: string; steps?: ReadonlyArray<unknown> };
          };
          return (
            value.type === "turn_item.updated" &&
            value.turnItem?.type === "todo_list" &&
            (value.turnItem.steps?.length ?? 0) === 1
          );
        }),
      ).toBe(true);
      expect(
        list.some((event) => {
          const value = event as unknown as {
            type: string;
            providerTurn?: { tokenUsage?: { usedTokens?: number } };
          };
          return (
            value.type === "provider_turn.updated" &&
            value.providerTurn?.tokenUsage?.usedTokens === 10
          );
        }),
      ).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );
});
