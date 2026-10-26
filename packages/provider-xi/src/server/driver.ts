import { ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import type { ServerProviderDraft } from "@t3tools/provider-core/server/snapshotProbe";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";

import { XiSettings } from "../settings.ts";
import { XiAdapterV2Driver, type XiAdapterV2DriverEnv } from "./adapter.ts";
import { buildInitialXiProviderSnapshot, checkXiProviderStatus } from "./status.ts";
import { makeXiTextGeneration } from "./textGeneration.ts";

const DRIVER_KIND = ProviderDriverKind.make("xi");
const decodeXiSettings = Schema.decodeSync(XiSettings);

export type XiDriverEnv =
  | XiAdapterV2DriverEnv
  | ProviderHost.ProviderHost
  | ChildProcessSpawner.ChildProcessSpawner;

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export const XiDriver: ProviderDriver<XiSettings, XiDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "xi",
    supportsMultipleInstances: true,
  },
  configSchema: XiSettings,
  defaultConfig: (): XiSettings => decodeXiSettings({}),
  create: Effect.fn("XiDriver.create")(function* ({
    instanceId,
    displayName,
    accentColor,
    environment,
    enabled,
    config,
  }) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const host = yield* ProviderHost.ProviderHost;
    const processEnv = yield* mergeProviderInstanceEnvironment(environment);
    const continuationIdentity = defaultProviderContinuationIdentity({
      driverKind: DRIVER_KIND,
      instanceId,
    });
    const stampIdentity = withInstanceIdentity({
      instanceId,
      displayName,
      accentColor,
      continuationGroupKey: continuationIdentity.continuationKey,
    });
    const effectiveConfig = { ...config, enabled } satisfies XiSettings;

    const orchestrationAdapter = yield* XiAdapterV2Driver.create({
      instanceId,
      displayName,
      accentColor,
      environment,
      enabled,
      config,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build the xi orchestration adapter.",
            cause,
          }),
      ),
    );

    const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
    const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<XiSettings>>({
      resolveMaintenance: () =>
        Effect.succeed({ provider: DRIVER_KIND, packageName: null, update: null }),
      getSettings: snapshotSettings.getSettings,
      streamSettings: snapshotSettings.streamSettings,
      haveSettingsChanged: haveProviderSnapshotSettingsChanged,
      initialSnapshot: (settings) =>
        buildInitialXiProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
      checkProvider: checkXiProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      ),
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build the xi provider snapshot.",
            cause,
          }),
      ),
      Effect.provideService(ProviderHost.ProviderHost, host),
    );

    return {
      instanceId,
      driverKind: DRIVER_KIND,
      continuationIdentity,
      displayName,
      accentColor,
      enabled,
      snapshot,
      orchestrationAdapter,
      textGeneration: makeXiTextGeneration(),
    } satisfies ProviderInstance;
  }),
};
