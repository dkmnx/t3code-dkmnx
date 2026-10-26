import { describe, expect, it } from "@effect/vitest";

import { thinkingCapabilitiesForXiModel } from "./thinkingCapabilities.ts";

describe("thinkingCapabilitiesForXiModel", () => {
  it("carries the model's own levels and marks the default", () => {
    const capabilities = thinkingCapabilitiesForXiModel(true, ["low", "high", "max"]);
    expect(capabilities.optionDescriptors).toHaveLength(1);
    const descriptor = capabilities.optionDescriptors?.[0];
    expect(descriptor?.type).toBe("select");
    if (descriptor?.type !== "select") return;
    expect(descriptor.options.map((option) => option.id)).toEqual(["low", "high", "max"]);
    expect(descriptor.options.find((option) => option.isDefault)?.id).toBe("high");
  });

  it("omits the control when the model offers no level", () => {
    expect(thinkingCapabilitiesForXiModel(true, []).optionDescriptors).toEqual([]);
    expect(thinkingCapabilitiesForXiModel(false, ["off"]).optionDescriptors).toEqual([]);
  });
});
