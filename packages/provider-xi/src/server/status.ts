import { type CustomModelSetting, type ServerProviderModel } from "@t3tools/contracts";
import type { XiSettings } from "../settings.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import {
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "@t3tools/provider-core/server/snapshotProbe";
import {
  EMPTY_XI_MODEL_CAPABILITIES,
  thinkingCapabilitiesForXiModel,
} from "./thinkingCapabilities.ts";

const XI_PRESENTATION = {
  displayName: "xi",
  showInteractionModeToggle: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
  reportsContextWindow: true,
  requiresNewThreadForModelChange: false,
} as const;

const CATALOG_TIMEOUT_MS = 20_000;

const XI_DEFAULT_MODEL: ServerProviderModel = {
  slug: "default",
  name: "xi default",
  isCustom: false,
  capabilities: EMPTY_XI_MODEL_CAPABILITIES,
};

function xiModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  discovered: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  const base = discovered.length > 0 ? discovered : [XI_DEFAULT_MODEL];
  return providerModelsFromSettings(base, customModels ?? [], EMPTY_XI_MODEL_CAPABILITIES);
}

function recordField(input: unknown, key: string): unknown {
  return typeof input === "object" && input !== null && !Array.isArray(input)
    ? (input as Record<string, unknown>)[key]
    : undefined;
}

function recordString(input: unknown, key: string): string | undefined {
  const value = recordField(input, key);
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

// parseXiCatalog maps `xi models --json` to provider models. Each row carries
// its own levels, so a model with no levels advertises no reasoning control.
export function parseXiCatalog(stdout: string): ReadonlyArray<ServerProviderModel> {
  const parsed: unknown = JSON.parse(stdout);
  const rows = recordField(parsed, "models");
  if (!Array.isArray(rows)) return [];
  const seen = new Set<string>();
  const models: Array<ServerProviderModel> = [];
  for (const row of rows) {
    const provider = recordString(row, "provider");
    const id = recordString(row, "id");
    const selector =
      recordString(row, "selector") ?? (provider && id ? `${provider}/${id}` : undefined);
    if (selector === undefined || seen.has(selector)) continue;
    seen.add(selector);
    const thinking = recordField(row, "thinking");
    const levels = Array.isArray(thinking)
      ? thinking.filter((level): level is string => typeof level === "string")
      : [];
    models.push({
      slug: selector,
      name: recordString(row, "name") ?? selector,
      ...(provider ? { subProvider: provider } : {}),
      isCustom: false,
      capabilities: thinkingCapabilitiesForXiModel(recordField(row, "reasoning") === true, levels),
    });
  }
  return models;
}

function parseXiCatalogSafe(stdout: string): ReadonlyArray<ServerProviderModel> | undefined {
  try {
    return parseXiCatalog(stdout);
  } catch {
    return undefined;
  }
}

export const buildInitialXiProviderSnapshot = Effect.fn("XiProviderStatus.buildInitialSnapshot")(
  function* (settings: XiSettings): Effect.fn.Return<ServerProviderDraft> {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    if (!settings.enabled) {
      return buildServerProvider({
        presentation: XI_PRESENTATION,
        enabled: false,
        checkedAt,
        models: xiModelsFromSettings(settings.customModels),
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "xi is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: XI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: xiModelsFromSettings(settings.customModels),
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Reading the xi model catalog...",
      },
    });
  },
);

export const checkXiProviderStatus = Effect.fn("XiProviderStatus.check")(function* (
  settings: XiSettings,
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = xiModelsFromSettings(settings.customModels);

  if (!settings.enabled) {
    return buildServerProvider({
      presentation: XI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "xi is disabled in T3 Code settings.",
      },
    });
  }

  const binary = settings.binaryPath || "xi";
  const result = yield* spawnAndCollect(
    binary,
    ChildProcess.make(binary, ["models", "--json"], { env: environment }),
  ).pipe(Effect.timeoutOption(CATALOG_TIMEOUT_MS), Effect.result);

  if (Result.isFailure(result)) {
    const error = result.failure;
    return buildServerProvider({
      presentation: XI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "xi is not installed or not on PATH."
          : "Failed to execute the xi model catalog.",
      },
    });
  }

  if (Option.isNone(result.success)) {
    return buildServerProvider({
      presentation: XI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "xi timed out while reading the model catalog.",
      },
    });
  }

  const output = result.success.value;
  if (output.code !== 0) {
    return buildServerProvider({
      presentation: XI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "xi failed to read the model catalog.",
      },
    });
  }

  const parsed = parseXiCatalogSafe(output.stdout);
  if (parsed === undefined) {
    return buildServerProvider({
      presentation: XI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "xi returned an unreadable model catalog.",
      },
    });
  }
  const models = xiModelsFromSettings(settings.customModels, parsed);
  const authenticated = parsed.length > 0;
  return buildServerProvider({
    presentation: XI_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: true,
      version: null,
      status: authenticated ? "ready" : "warning",
      auth: {
        status: authenticated ? "authenticated" : "unauthenticated",
        type: "xi",
      },
      ...(authenticated
        ? {}
        : { message: "xi has no usable models. Run `xi login <provider>` and retry." }),
    },
  });
});
