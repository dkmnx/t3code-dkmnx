import * as Effect from "effect/Effect";

import { XiRpcError, type XiRpcConnection } from "./rpc.ts";

export interface XiSessionConfig {
  readonly sessionId: string;
  readonly model: string;
  readonly thinkingLevel?: string;
}

function sessionConfig(value: unknown): XiSessionConfig | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const record = value as Record<string, unknown>;
  if (typeof record["sessionId"] !== "string" || record["sessionId"] === "") return undefined;
  if (typeof record["model"] !== "string") return undefined;
  return {
    sessionId: record["sessionId"],
    model: record["model"],
    ...(typeof record["thinkingLevel"] === "string"
      ? { thinkingLevel: record["thinkingLevel"] }
      : {}),
  };
}

const invalid = (operation: string, detail: string) => new XiRpcError({ operation, detail });

export const openXiSession = Effect.fn("XiSession.open")(function* (
  connection: XiRpcConnection,
  cwd: string,
) {
  yield* connection.request("initialize", { protocolVersion: 1 });
  const config = sessionConfig(yield* connection.request("session/new", { cwd }));
  if (config === undefined) return yield* invalid("session/new", "missing sessionId");
  return config;
});

export const promptXiSession = (
  connection: XiRpcConnection,
  sessionId: string,
  prompt: string,
  requestId?: string,
) =>
  connection.request("session/prompt", {
    sessionId,
    prompt,
    ...(requestId === undefined ? {} : { requestId }),
  });

export const cancelXiSession = (connection: XiRpcConnection, sessionId: string) =>
  connection.notify("session/cancel", { sessionId });

export const closeXiSession = (connection: XiRpcConnection, sessionId: string) =>
  connection.request("session/close", { sessionId });

export const setXiModel = (
  connection: XiRpcConnection,
  sessionId: string,
  model: string,
  thinking?: string,
) =>
  connection.request("session/set_model", {
    sessionId,
    model,
    ...(thinking === undefined ? {} : { thinking }),
  });

export const setXiThinking = (connection: XiRpcConnection, sessionId: string, level: string) =>
  connection.request("session/set_thinking", { sessionId, level });

export const replyXiApproval = (
  connection: XiRpcConnection,
  callId: string,
  decision: "allow-once" | "allow-always" | "reject" | "reject-always",
) => connection.request("session/approval_reply", { callId, decision });

export const replyXiQuestion = (
  connection: XiRpcConnection,
  callId: string,
  answers: ReadonlyArray<ReadonlyArray<string>> | undefined,
) =>
  connection.request("session/question_reply", {
    callId,
    ...(answers === undefined ? {} : { answers }),
  });
