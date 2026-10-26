import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

export class XiRpcError extends Schema.TaggedError<XiRpcError>()("XiRpcError", {
  operation: Schema.String,
  detail: Schema.optional(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `xi rpc ${this.operation} failed${this.detail === undefined ? "" : `: ${this.detail}`}.`;
  }
}

export interface XiRpcNotification {
  readonly method: string;
  readonly params: Record<string, unknown>;
}

export interface XiRpcSpawnOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string | undefined;
  readonly env: NodeJS.ProcessEnv;
}

export interface XiRpcConnection {
  readonly request: (
    method: string,
    params?: unknown,
    timeoutMs?: number,
  ) => Effect.Effect<unknown, XiRpcError>;
  readonly notify: (method: string, params?: unknown) => Effect.Effect<void, XiRpcError>;
  readonly notifications: Queue.Queue<XiRpcNotification>;
  readonly exited: Effect.Effect<number, XiRpcError>;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeJsonLine = Schema.decodeUnknownOption(UnknownFromJsonString);
const encodeJsonLine = Schema.encodeSync(UnknownFromJsonString);
const encodeText = (value: string) => new TextEncoder().encode(value);

const fail = (operation: string, detail?: string, cause?: unknown) =>
  new XiRpcError({
    operation,
    ...(detail === undefined ? {} : { detail }),
    ...(cause === undefined ? {} : { cause }),
  });

export const makeXiRpcConnection = Effect.fn("XiRpc.connect")(function* (
  options: XiRpcSpawnOptions,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const scope = yield* Effect.scope;
  const child = yield* spawner
    .spawn(
      ChildProcess.make(options.command, [...options.args], {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        env: options.env,
        extendEnv: false,
      }),
    )
    .pipe(Effect.mapError((cause) => fail("spawn", undefined, cause)));

  const notifications = yield* Queue.unbounded<XiRpcNotification>();
  const outgoing = yield* Queue.unbounded<Uint8Array>();
  const pending = yield* Ref.make(new Map<number, Deferred.Deferred<unknown, XiRpcError>>());
  const nextId = yield* Ref.make(0);

  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.forkIn(scope),
    Effect.ignore,
  );

  let buffer = "";
  yield* child.stdout.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Effect.gen(function* () {
        buffer += chunk;
        for (;;) {
          const newline = buffer.indexOf("\n");
          if (newline < 0) return;
          const line = buffer.slice(0, newline).trim();
          buffer = buffer.slice(newline + 1);
          if (line === "") continue;
          const decoded = decodeJsonLine(line);
          if (Option.isNone(decoded)) continue;
          const message = decoded.value;
          if (typeof message !== "object" || message === null) continue;
          const record = message as Record<string, unknown>;
          if (typeof record["id"] === "number") {
            const waiter = (yield* Ref.get(pending)).get(record["id"]);
            if (waiter === undefined) continue;
            yield* Ref.update(pending, (map) => {
              map.delete(record["id"] as number);
              return map;
            });
            const error = record["error"];
            if (typeof error === "object" && error !== null) {
              const detail = (error as Record<string, unknown>)["message"];
              yield* Deferred.fail(
                waiter,
                fail("request", typeof detail === "string" ? detail : undefined),
              );
            } else {
              yield* Deferred.succeed(waiter, record["result"]);
            }
            continue;
          }
          if (typeof record["method"] === "string") {
            const params = record["params"];
            yield* Queue.offer(notifications, {
              method: record["method"],
              params:
                typeof params === "object" && params !== null
                  ? (params as Record<string, unknown>)
                  : {},
            });
          }
        }
      }),
    ),
    Effect.forkIn(scope),
    Effect.ignore,
  );

  const request = Effect.fn("XiRpc.request")(function* (
    method: string,
    params?: unknown,
    timeoutMs?: number,
  ) {
    const id = yield* Ref.getAndUpdate(nextId, (value) => value + 1);
    const waiter = yield* Deferred.make<unknown, XiRpcError>();
    yield* Ref.update(pending, (map) => {
      map.set(id, waiter);
      return map;
    });
    yield* Queue.offer(
      outgoing,
      encodeText(
        `${encodeJsonLine({ jsonrpc: "2.0", id, method, ...(params === undefined ? {} : { params }) })}\n`,
      ),
    );
    return yield* Deferred.await(waiter).pipe(
      Effect.timeout(timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS),
      Effect.catchTags({ TimeoutError: () => Effect.fail(fail(method, "timed out")) }),
    );
  });

  const notify: XiRpcConnection["notify"] = (method, params) =>
    Queue.offer(
      outgoing,
      encodeText(
        `${encodeJsonLine({ jsonrpc: "2.0", method, ...(params === undefined ? {} : { params }) })}\n`,
      ),
    );

  return {
    request,
    notify,
    notifications,
    exited: child.exitCode.pipe(Effect.mapError((cause) => fail("exit", undefined, cause))),
  } satisfies XiRpcConnection;
});
