import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { XiSettings } from "./settings.ts";

export const xiClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("xi"),
  label: "xi",
  settingsSchema: XiSettings,
});
