import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

import { makeXiRpcConnection } from "./rpc.ts";

const fakeAgent = `
let buffer = "";
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
      process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: message.id, result: { protocolVersion: 1 } }) + "\\n");
    }
    if (message.method === "catalog/list") {
      process.stdout.write(JSON.stringify({ jsonrpc: "2.0", method: "session/event", params: { sessionId: "s", event: "notice" } }) + "\\n");
      process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: message.id, result: { models: [] } }) + "\\n");
    }
  }
});
process.stdin.on("end", () => process.exit(0));
`;

describe("makeXiRpcConnection", () => {
  it.effect("answers requests and delivers notifications", () =>
    Effect.gen(function* () {
      const connection = yield* makeXiRpcConnection({
        command: process.execPath,
        args: ["-e", fakeAgent],
        env: process.env,
      });
      const initialized = (yield* connection.request("initialize")) as { protocolVersion: number };
      expect(initialized.protocolVersion).toBe(1);
      const catalog = (yield* connection.request("catalog/list")) as { models: unknown[] };
      expect(catalog.models).toEqual([]);
      const notification = yield* Queue.take(connection.notifications);
      expect(notification.method).toBe("session/event");
      expect(notification.params["sessionId"]).toBe("s");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
