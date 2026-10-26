import { type ModelCapabilities, type ProviderOptionChoice } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";

export const EMPTY_XI_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const LABELS: Record<string, string> = {
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
};

const LEVEL_ORDER = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;

// clamp to the nearest supported level, walking up before down, matching
// xi's own clamp.
function clampDefault(levels: ReadonlyArray<string>): string {
  const start = LEVEL_ORDER.indexOf("medium");
  for (let index = start; index < LEVEL_ORDER.length; index += 1) {
    if (levels.includes(LEVEL_ORDER[index]!)) return LEVEL_ORDER[index]!;
  }
  for (let index = start - 1; index >= 0; index -= 1) {
    if (levels.includes(LEVEL_ORDER[index]!)) return LEVEL_ORDER[index]!;
  }
  return levels[0]!;
}

// xi reports the supported levels per model. A model that advertises none
// gets no descriptor, so the composer shows no reasoning control for it.
export function thinkingCapabilitiesForXiModel(
  reasoning: boolean,
  levels: ReadonlyArray<string>,
): ModelCapabilities {
  const supported = levels.filter((level) => level.trim() !== "");
  if (!reasoning || supported.length === 0) return EMPTY_XI_MODEL_CAPABILITIES;
  const defaultLevel = clampDefault(supported);
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinking",
        label: "Thinking",
        type: "select",
        options: supported.map((level): ProviderOptionChoice => ({
          id: level,
          label: LABELS[level] ?? level,
          ...(level === defaultLevel ? { isDefault: true } : {}),
        })),
      },
    ],
  });
}
