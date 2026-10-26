import { describe, expect, it } from "@effect/vitest";

import { parseXiCatalog } from "./status.ts";

describe("parseXiCatalog", () => {
  it("maps each model with its own levels", () => {
    const catalog = JSON.stringify({
      models: [
        {
          provider: "opencode-go",
          id: "mimo-v2.6-flash",
          selector: "opencode-go/mimo-v2.6-flash",
          name: "MiMo",
          reasoning: true,
          thinking: [],
        },
        {
          provider: "commandcode",
          id: "deepseek/deepseek-v4.1-flash",
          selector: "commandcode/deepseek/deepseek-v4.1-flash",
          name: "DeepSeek",
          reasoning: true,
          thinking: ["low", "high", "max"],
        },
      ],
    });
    const models = parseXiCatalog(catalog);
    expect(models).toHaveLength(2);
    expect(models[0]?.slug).toBe("opencode-go/mimo-v2.6-flash");
    expect(models[0]?.capabilities?.optionDescriptors).toEqual([]);
    const descriptor = models[1]?.capabilities?.optionDescriptors?.[0];
    expect(descriptor?.type).toBe("select");
    if (descriptor?.type !== "select") return;
    expect(descriptor.options.map((option) => option.id)).toEqual(["low", "high", "max"]);
  });

  it("drops duplicate selectors and invalid rows", () => {
    const models = parseXiCatalog(
      JSON.stringify({
        models: [
          { provider: "p", id: "a", selector: "p/a" },
          { provider: "p", id: "a", selector: "p/a" },
          { id: "missing-provider" },
        ],
      }),
    );
    expect(models).toHaveLength(1);
  });
});
