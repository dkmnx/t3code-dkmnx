import {
  MessageId,
  ProviderDriverKind,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeRequestId,
  TurnItemId,
  type OrchestrationV2RuntimeRequest,
  type ProviderApprovalDecision,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type {
  ProviderAdapterDriver,
  ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";

import { XiSettings } from "../settings.ts";
import { makeXiRpcConnection } from "./rpc.ts";
import {
  cancelXiSession,
  closeXiSession,
  openXiSession,
  promptXiSession,
  replyXiApproval,
  replyXiQuestion,
  setXiModel,
  setXiThinking,
} from "./session.ts";

const XI_PROVIDER = ProviderDriverKind.make("xi");

export type XiAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | ProviderHost.ProviderHost;

const DEFAULT_XI_SETTINGS = Schema.decodeSync(XiSettings)({});

// Honest flags for the catalog milestone: the snapshot works, sessions land
// with the adapter milestone. Nothing claims a capability the transport does
// not implement yet.
const XiProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: false,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: false,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: false,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: false,
    emitsTurnCompleted: false,
    supportsInterrupt: false,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: false,
    terminalStatusQuality: "weak",
  },
  streaming: {
    streamsAssistantText: false,
    streamsReasoning: false,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: false,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: false,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: false,
    supportsDeltaHandoff: false,
    supportsFullThreadHandoff: false,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "weak",
    nativeTurnIds: "weak",
    nativeItemIds: "weak",
    nativeRequestIds: "weak",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export const XiAdapterV2Driver: ProviderAdapterDriver<XiSettings, XiAdapterV2DriverEnv> = {
  driverKind: XI_PROVIDER,
  configSchema: XiSettings,
  defaultConfig: (): XiSettings => DEFAULT_XI_SETTINGS,
  create: Effect.fn("XiAdapterV2Driver.create")(function* (
    input: ProviderAdapterDriverCreateInput<XiSettings>,
  ) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const host = yield* ProviderHost.ProviderHost;

    return ProviderAdapter.ProviderAdapterV2.of({
      instanceId: input.instanceId,
      driver: XI_PROVIDER,
      getCapabilities: Effect.fn("XiAdapterV2.getCapabilities")(() =>
        Effect.succeed(XiProviderCapabilitiesV2),
      ),
      planSelectionTransition: Effect.fn("XiAdapterV2.planSelectionTransition")(() =>
        Effect.succeed(turnScopedSelectionTransition()),
      ),
      openSession: Effect.fnUntraced(function* (
        sessionInput: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
      ) {
        const scope = yield* Effect.scope;
        const cwd = sessionInput.runtimePolicy.cwd ?? host.paths.cwd;
        const connection = yield* makeXiRpcConnection({
          command: input.config.binaryPath || "xi",
          args: ["rpc", ...input.config.launchArgs.split(/\s+/).filter((arg) => arg !== "")],
          cwd,
          env: process.env,
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: XI_PROVIDER,
                providerSessionId: sessionInput.providerSessionId,
                cause,
              }),
          ),
        );
        const opened = yield* openXiSession(connection, cwd).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: XI_PROVIDER,
                providerSessionId: sessionInput.providerSessionId,
                cause,
              }),
          ),
        );
        yield* Effect.addFinalizer(() =>
          closeXiSession(connection, opened.sessionId).pipe(Effect.ignore),
        );

        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        let thread: OrchestrationV2ProviderThread | null = null;
        let appliedThinking = opened.thinkingLevel ?? null;
        let activeTurn: {
          readonly id: OrchestrationV2ProviderTurn["id"];
          readonly runId: string;
          readonly threadId: string;
          readonly nodeId: string;
          readonly runOrdinal: number;
          readonly messageId: string;
        } | null = null;
        let sessionEntity: OrchestrationV2ProviderSession = {
          id: sessionInput.providerSessionId,
          driver: XI_PROVIDER,
          providerInstanceId: input.instanceId,
          status: "ready",
          cwd,
          model: opened.model,
          capabilities: XiProviderCapabilitiesV2,
          createdAt: yield* DateTime.now,
          updatedAt: yield* DateTime.now,
          lastError: null,
        };
        const approvals = new Map<
          string,
          {
            readonly callId: string;
            readonly threadId: string;
            readonly request: OrchestrationV2RuntimeRequest;
          }
        >();
        const itemOrdinals = new Map<string, number>();
        let nextItemOrdinal = 1;

        const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
          Queue.offer(events, event).pipe(Effect.asVoid);

        const finishTurn = Effect.fnUntraced(function* (runDone: Record<string, unknown>) {
          const turn = activeTurn;
          activeTurn = null;
          if (turn === null || thread === null) return;
          const errorText = typeof runDone["error"] === "string" ? runDone["error"] : "";
          if (errorText === "") {
            yield* emit({
              type: "turn.terminal",
              driver: XI_PROVIDER,
              providerThreadId: thread.id,
              providerTurnId: turn.id,
              runOrdinal: turn.runOrdinal,
              status: "completed",
              failure: null,
              threadDisposition: "reusable",
            });
            return;
          }
          if (/context canceled|request was aborted/i.test(errorText)) {
            yield* emit({
              type: "turn.terminal",
              driver: XI_PROVIDER,
              providerThreadId: thread.id,
              providerTurnId: turn.id,
              runOrdinal: turn.runOrdinal,
              status: "interrupted",
              failure: null,
              threadDisposition: "reusable",
            });
            return;
          }
          yield* emit({
            type: "turn.terminal",
            driver: XI_PROVIDER,
            providerThreadId: thread.id,
            providerTurnId: turn.id,
            runOrdinal: turn.runOrdinal,
            failureItemOrdinal: 0,
            status: "failed",
            failure: {
              class: runDone["errorKind"] === "usage_limit" ? "usage_limit" : "provider_error",
              message: errorText,
              code: null,
              retryable: null,
            },
            threadDisposition: "reusable",
          });
        });

        yield* Effect.gen(function* () {
          for (;;) {
            const notification = yield* Queue.take(connection.notifications);
            if (notification.method === "session/message") {
              const message = notification.params["message"];
              if (typeof message !== "object" || message === null) continue;
              const record = message as Record<string, unknown>;
              if (record["role"] !== "assistant") continue;
              const content = Array.isArray(record["content"]) ? record["content"] : [];
              let text = "";
              for (const block of content) {
                if (typeof block !== "object" || block === null) continue;
                const item = block as Record<string, unknown>;
                if (item["type"] === "text" && typeof item["text"] === "string") {
                  text += item["text"];
                }
              }
              const turn = activeTurn;
              if (turn === null || text === "") continue;
              const at = yield* DateTime.now;
              yield* emit({
                type: "message.updated",
                driver: XI_PROVIDER,
                message: {
                  id: MessageId.make(turn.messageId),
                  threadId: turn.threadId as never,
                  runId: turn.runId as never,
                  nodeId: turn.nodeId as never,
                  role: "assistant",
                  text,
                  attachments: [],
                  streaming: notification.params["event"] !== "message_end",
                  createdBy: "agent",
                  creationSource: "provider",
                  createdAt: at,
                  updatedAt: at,
                },
              });
              continue;
            }
            if (notification.method === "session/tool") {
              const turn = activeTurn;
              if (turn === null || thread === null) continue;
              const callId =
                typeof notification.params["callId"] === "string"
                  ? notification.params["callId"]
                  : "";
              if (callId === "") continue;
              const name =
                typeof notification.params["name"] === "string"
                  ? notification.params["name"]
                  : "tool";
              const event = notification.params["event"];
              const ordinal = itemOrdinals.get(callId) ?? nextItemOrdinal++;
              itemOrdinals.set(callId, ordinal);
              const at = yield* DateTime.now;
              const result = notification.params["result"];
              const failed =
                typeof result === "object" &&
                result !== null &&
                (result as Record<string, unknown>)["isError"] === true;
              const status =
                event === "tool_execution_end"
                  ? failed
                    ? "failed"
                    : "completed"
                  : event === "tool_execution_start"
                    ? "running"
                    : "pending";
              const item = {
                id: TurnItemId.make(`xi-${callId}`),
                threadId: turn.threadId as never,
                runId: turn.runId as never,
                nodeId: turn.nodeId as never,
                providerThreadId: thread.id,
                providerTurnId: turn.id,
                nativeItemRef: null,
                parentItemId: null,
                ordinal,
                status: status as never,
                title: name,
                startedAt: at,
                completedAt: status === "completed" || status === "failed" ? at : null,
                updatedAt: at,
                type: "dynamic_tool" as const,
                toolName: name,
                input: notification.params["arguments"] ?? {},
                ...(result === undefined ? {} : { output: result }),
              };
              yield* emit({
                type: "turn_item.updated",
                driver: XI_PROVIDER,
                turnItem: item as never,
              });
              continue;
            }
            if (notification.method === "session/approval") {
              const turn = activeTurn;
              if (turn === null || thread === null) continue;
              const callId =
                typeof notification.params["callId"] === "string"
                  ? notification.params["callId"]
                  : "";
              if (callId === "") continue;
              const requestId = RuntimeRequestId.make(`xi-${callId}`);
              const at = yield* DateTime.now;
              const request: OrchestrationV2RuntimeRequest = {
                id: requestId,
                nodeId: turn.nodeId as never,
                providerTurnId: turn.id,
                nativeRequestRef: null,
                kind: "permission",
                status: "pending",
                responseCapability: {
                  type: "live",
                  providerSessionId: sessionInput.providerSessionId,
                },
                createdAt: at,
                resolvedAt: null,
              };
              approvals.set(requestId, { callId, threadId: turn.threadId, request });
              yield* emit({
                type: "runtime_request.updated",
                driver: XI_PROVIDER,
                threadId: turn.threadId as never,
                runtimeRequest: request,
              });
              continue;
            }
            if (notification.method === "session/question") {
              const callId =
                typeof notification.params["callId"] === "string"
                  ? notification.params["callId"]
                  : "";
              if (callId !== "") yield* replyXiQuestion(connection, callId, undefined);
              continue;
            }
            if (notification.method === "session/run_done") {
              yield* finishTurn(notification.params);
            }
          }
        }).pipe(Effect.forkIn(scope), Effect.ignore);

        return {
          instanceId: input.instanceId,
          driver: XI_PROVIDER,
          providerSessionId: sessionInput.providerSessionId,
          get providerSession() {
            return sessionEntity;
          },
          events: Stream.fromQueue(events),
          hasPendingBackgroundWork: Effect.succeed(false),
          ensureThread: Effect.fn("XiAdapterV2.ensureThread")(function* (threadInput) {
            const at = yield* DateTime.now;
            const existing = threadInput.existingProviderThread;
            thread =
              existing === undefined
                ? {
                    id: ProviderThreadId.make(`xi-${threadInput.threadId}`),
                    driver: XI_PROVIDER,
                    providerInstanceId: input.instanceId,
                    providerSessionId: sessionInput.providerSessionId,
                    appThreadId: threadInput.threadId,
                    ownerNodeId: null,
                    nativeThreadRef: {
                      driver: XI_PROVIDER,
                      nativeId: opened.sessionId,
                      strength: "strong",
                    },
                    nativeConversationHeadRef: null,
                    status: "idle",
                    firstRunOrdinal: null,
                    lastRunOrdinal: null,
                    handoffIds: [],
                    forkedFrom: null,
                    pendingBackgroundTasks: [],
                    createdAt: at,
                    updatedAt: at,
                  }
                : { ...existing, status: "idle", updatedAt: at };
            yield* emit({
              type: "provider_thread.updated",
              driver: XI_PROVIDER,
              providerThread: thread,
            });
            return thread;
          }),
          resumeThread: Effect.fn("XiAdapterV2.resumeThread")(function* (threadInput) {
            const at = yield* DateTime.now;
            thread = { ...threadInput.providerThread, status: "idle", updatedAt: at };
            yield* emit({
              type: "provider_thread.updated",
              driver: XI_PROVIDER,
              providerThread: thread,
            });
            return thread;
          }),
          startTurn: Effect.fn("XiAdapterV2.startTurn")(function* (turnInput) {
            const currentThread = thread;
            if (currentThread === null) {
              return yield* new ProviderAdapter.ProviderAdapterTurnStartError({
                driver: XI_PROVIDER,
                threadId: turnInput.threadId,
                providerThreadId: ProviderThreadId.make("xi-missing-thread"),
                runId: turnInput.runId,
              });
            }
            if (activeTurn !== null) {
              return yield* new ProviderAdapter.ProviderAdapterProtocolError({
                driver: XI_PROVIDER,
                detail: "xi provider thread already has an active turn",
                reason: "previous_turn_active",
              });
            }
            if (turnInput.modelSelection.model !== sessionEntity.model) {
              yield* setXiModel(
                connection,
                opened.sessionId,
                turnInput.modelSelection.model,
                getModelSelectionStringOptionValue(turnInput.modelSelection, "thinking"),
              ).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterTurnStartError({
                      driver: XI_PROVIDER,
                      threadId: turnInput.threadId,
                      providerThreadId: currentThread.id,
                      runId: turnInput.runId,
                      cause,
                    }),
                ),
              );
              sessionEntity = {
                ...sessionEntity,
                model: turnInput.modelSelection.model,
                updatedAt: yield* DateTime.now,
              };
              appliedThinking =
                getModelSelectionStringOptionValue(turnInput.modelSelection, "thinking") ??
                appliedThinking;
            }
            const requestedThinking = getModelSelectionStringOptionValue(
              turnInput.modelSelection,
              "thinking",
            );
            if (requestedThinking !== undefined && requestedThinking !== appliedThinking) {
              yield* setXiThinking(connection, opened.sessionId, requestedThinking).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterTurnStartError({
                      driver: XI_PROVIDER,
                      threadId: turnInput.threadId,
                      providerThreadId: currentThread.id,
                      runId: turnInput.runId,
                      cause,
                    }),
                ),
              );
              appliedThinking = requestedThinking;
            }
            activeTurn = {
              id: ProviderTurnId.make(`xi-${turnInput.attemptId}`),
              runId: turnInput.runId,
              threadId: turnInput.threadId,
              nodeId: turnInput.rootNodeId,
              runOrdinal: turnInput.runOrdinal,
              messageId: `xi-${turnInput.attemptId}-assistant`,
            };
            const startedAt = yield* DateTime.now;
            yield* emit({
              type: "provider_turn.updated",
              driver: XI_PROVIDER,
              providerTurn: {
                id: activeTurn.id,
                providerThreadId: currentThread.id,
                nodeId: turnInput.rootNodeId,
                runAttemptId: turnInput.attemptId,
                nativeTurnRef: null,
                ordinal: turnInput.providerTurnOrdinal,
                status: "running",
                startedAt,
                completedAt: null,
              },
            });
            yield* promptXiSession(
              connection,
              opened.sessionId,
              turnInput.message.text,
              turnInput.attemptId,
            ).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver: XI_PROVIDER,
                    threadId: turnInput.threadId,
                    providerThreadId: currentThread.id,
                    runId: turnInput.runId,
                    cause,
                  }),
              ),
            );
          }),
          steerTurn: (steerInput) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterSteerRunUnsupportedError({
                driver: XI_PROVIDER,
                providerThreadId: steerInput.providerThread.id,
              }),
            ),
          interruptTurn: (interruptInput) =>
            cancelXiSession(connection, opened.sessionId).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: XI_PROVIDER,
                    providerThreadId: interruptInput.providerThread.id,
                    providerTurnId: interruptInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
          respondToRuntimeRequest: Effect.fn("XiAdapterV2.respondToRuntimeRequest")(
            function* (responseInput) {
              const pending = approvals.get(responseInput.requestId);
              if (pending === undefined) {
                return yield* new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                  driver: XI_PROVIDER,
                  requestId: responseInput.requestId,
                  cause: new Error("unknown xi approval request"),
                });
              }
              const decision: ProviderApprovalDecision = responseInput.decision ?? "cancel";
              const mapped =
                decision === "accept"
                  ? "allow-once"
                  : decision === "acceptForSession" || decision === "acceptAlways"
                    ? "allow-always"
                    : "reject";
              yield* replyXiApproval(connection, pending.callId, mapped).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                      driver: XI_PROVIDER,
                      requestId: responseInput.requestId,
                      cause,
                    }),
                ),
              );
              approvals.delete(responseInput.requestId);
              const resolvedAt = yield* DateTime.now;
              const request = {
                ...pending.request,
                status: "resolved" as const,
                decision,
                resolvedAt,
              };
              yield* emit({
                type: "runtime_request.updated",
                driver: XI_PROVIDER,
                threadId: pending.threadId as never,
                runtimeRequest: request,
              });
            },
          ),
          readThreadSnapshot: (snapshotInput) =>
            Effect.succeed({
              providerThread: snapshotInput.providerThread,
              providerTurns: [],
              messages: [],
              runtimeRequests: [],
            }),
          rollbackThread: (rollbackInput) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterRollbackThreadError({
                driver: XI_PROVIDER,
                providerThreadId: rollbackInput.providerThread.id,
              }),
            ),
          forkThread: (forkInput) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterForkThreadError({
                driver: XI_PROVIDER,
                providerThreadId: forkInput.sourceProviderThread.id,
              }),
            ),
        };
      }),
    });
  }),
};
