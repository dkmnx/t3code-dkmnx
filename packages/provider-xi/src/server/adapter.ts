import { ProviderDriverKind, type OrchestrationV2ProviderCapabilities } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type {
  ProviderAdapterDriver,
  ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";

import { XiSettings } from "../settings.ts";

const XI_PROVIDER = ProviderDriverKind.make("xi");

export type XiAdapterV2DriverEnv = never;

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
    exposesToolItemIds: false,
    emitsToolStarted: false,
    emitsToolCompleted: false,
    emitsToolOutput: false,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
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
  create: (input: ProviderAdapterDriverCreateInput<XiSettings>) =>
    Effect.succeed(
      ProviderAdapter.ProviderAdapterV2.of({
        instanceId: input.instanceId,
        driver: XI_PROVIDER,
        getCapabilities: Effect.fn("XiAdapterV2.getCapabilities")(() =>
          Effect.succeed(XiProviderCapabilitiesV2),
        ),
        planSelectionTransition: Effect.fn("XiAdapterV2.planSelectionTransition")(() =>
          Effect.succeed(turnScopedSelectionTransition()),
        ),
        openSession: Effect.fn("XiAdapterV2.openSession")(function* (
          session: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
        ) {
          return yield* new ProviderAdapter.ProviderAdapterOpenSessionError({
            driver: XI_PROVIDER,
            providerSessionId: session.providerSessionId,
            cause: new Error("xi rpc sessions are not wired yet"),
          });
        }),
      }),
    ),
};
