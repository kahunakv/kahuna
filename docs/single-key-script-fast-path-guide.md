# Kahuna single-key script fast path guide

This guide explains how Kahuna runs a script transaction over one ephemeral key with far fewer internal
messages than a general transaction, what it costs and gains, and the rules a maintainer must preserve. It is
written for two audiences:

- **Developers using Kahuna** who write counters, rate limiters, flags and similar single-key scripts and
  want to know which scripts take the fast path and how to turn it off.
- **Developers maintaining Kahuna** who need the mechanism and its invariants in one place.

The measured numbers are in [section 5](#5-measured-results).

---

## 1. The problem

A script such as the rate-limiting counter

```
LET current = EGET @counter_key
IF current = null THEN
  ESET @counter_key 1 EX to_int(@expires_ms)
  RETURN 1
END
LET count = to_int(current)
IF count >= to_int(@limit) THEN
  RETURN 0
END
ESET @counter_key count + 1 EX to_int(@expires_ms)
RETURN 1
```

is a pessimistic auto-commit transaction. On the general path an admitted request sends six messages to the
actor that owns the key: acquire the key's exclusive lock, `EGET`, `ESET`, prepare, the commit-time range-lock
probe, commit. Each one is routed by the locator (including a leadership confirmation), queued on the actor's
mailbox, and awaited. All six go to the **same single-threaded actor**, and most of what the protocol between
them protects against — another transaction interleaving between two steps — cannot happen inside one actor
turn.

## 2. The two mechanisms

Both apply only to the **ephemeral** key space, only when the key's partition is led by the node that
coordinates the transaction, and both fall back to the general path otherwise.

### 2.1 Fused finalize

A transaction whose whole write set is one ephemeral key finalizes with **one** actor message instead of three.
The message runs the existing prepare handler, the existing range-lock check, and the existing commit handler,
in that order, in a single actor turn. Six messages become four. This applies to scripts and to interactive
transactions alike.

Left out, and finalized the ordinary way: a transaction that validates its reads (optimistic locking, or
`readValidation=trackAndValidate`), a yielding transaction, a durable decision, a key led by another node.

### 2.2 Script actor turn

An auto-commit script whose lock analysis names **exactly one ephemeral key** runs start to finish inside one
turn of the actor that owns the key. The turn issues the same requests to the same handlers in the same order
as the general path — lock, the script's reads and writes, the fused finalize, release — but each request is a
direct call served by the actor that is already running, not a routed mailbox round trip. Six messages become
one.

A script takes a turn only when every node of its parsed tree is one a turn can run: expressions, `LET`, `IF`,
`RETURN`, `THROW`, and the ephemeral point operations `ESET`, `EGET`, `EEXISTS`, `EDELETE`, `EEXTEND`. A
script keeps the general path when it:

- uses `SLEEP` or `FOR` (a turn holds its actor for as long as it runs, so it must not wait or loop);
- reads a bucket or scans a prefix;
- touches the persistent key space;
- is an explicit `BEGIN … COMMIT` transaction, or sets any `BEGIN` option;
- names no key, or more than one key, in its lock analysis (for example a script that only calls `EEXISTS`,
  or one that writes only inside a `LET`);
- uses a key in a range-routed key space (its operations carry a routing generation that fences them against
  a range move, which a turn would bypass).

**The escape.** The shape check is a filter, not a proof. Inside a turn every statement checks that the key it
names is the turn's key; anything else — a different key, a batched write, a statement that cannot run in a
turn — ends the turn. The turn then releases the key (which also discards anything it staged), and the script
runs again from the start on the general path as a new transaction. Nothing the first attempt did is
observable, so a wrong guess costs one wasted attempt and nothing else.

## 3. What does not change

- **Answers.** Result type, value, revision, reason text, and the state left on the key are the same on either
  path. The test suites run every scenario with the mechanisms on and off and compare.
- **Isolation.** The turn takes the same exclusive lock the general path takes, so a turn and a general-path
  transaction on the same key exclude each other exactly as two general-path transactions do.
- **Foreign state.** A foreign exclusive lock, a foreign write intent, and a foreign prefix or range lock are
  honoured by the same handler checks as before. The commit-time range-lock probe still runs, in the same
  place in the order.
- **Admission and identity.** The transaction takes an admission slot and mints its HLC transaction id on the
  coordinating node before the turn starts.
- **Leadership.** Leadership of the key's partition is confirmed once, before the turn, with the same
  quorum-confirmed check every actor mutation uses.
- **Cluster behaviour.** A node that does not lead the key's partition cannot run a turn and sends the general
  path's messages to the leader. There is deliberately no inter-node form of either mechanism.

## 4. Configuration and metrics

| Setting | Default | Effect when off |
|---|---|---|
| `KahunaConfiguration.FusedEphemeralFinalize` / `--disable-fused-ephemeral-finalize` | on | Every transaction finalizes with prepare, probe, commit as three messages. |
| `KahunaConfiguration.ScriptActorTurns` / `--disable-script-actor-turns` | on | Every script takes the general path. |

Both are also on `EmbeddedKahunaOptions`. They are independent: a script that runs in a turn always finalizes
inside that turn.

| Metric | Meaning |
|---|---|
| `kahuna.transactions.fused_ephemeral_finalizes` | Single-ephemeral-key transactions finalized in one actor turn, whatever the outcome. |
| `kahuna.transactions.script_actor_turns` | Scripts that ran start to finish inside an actor turn. |
| `kahuna.transactions.script_actor_turn_escapes` | Scripts that left a turn and ran again on the general path. A sustained rate means the shape check admits scripts it should not; it costs the wasted attempt only. |

## 5. Measured results

### Setup

Same as the [gRPC request frames guide](grpc-request-frames-guide.md): one 8-core Apple Silicon Mac, node and
benchmark client on the same machine (CPU-saturated), standalone memory node, cleartext HTTP/2, request frames
on, `kahuna-bench --workload rate-limit --durability ephemeral --key-space 10000 --rate-limit-budget 1000000`,
concurrency 64, a fresh node per run. Measured 2026-09-21. Run-to-run noise on this machine is roughly ±15%.

| Configuration | req/s (each run) | p50 | p99 |
|---|---|---|---|
| Neither mechanism (`--disable-script-actor-turns --disable-fused-ephemeral-finalize`) | 134k, 127k, 136k | 410–420 µs | 2.4–3.8 ms |
| Fused finalize only (`--disable-script-actor-turns`) | 140k, 159k, 157k, 158k, 160k | 357–384 µs | 0.8–1.4 ms |
| Script actor turns (default) | 203k, 194k, 201k, 199k | 284–289 µs | 0.6–0.8 ms |

One further run with script actor turns on gave 90k req/s with a single 457 ms pause (p50 still 373 µs). It was
the first run directly after a compile on the same machine and did not recur in four more runs; the cause was
not established.

For reference, on the same machine Valkey ran the same counter as a Lua `EVAL` at 169k req/s, and the same
workload before request frames and before these two mechanisms ran at about 84k req/s.

## 6. Maintainer notes

- **Fused finalize.** `TryFinalizeMutationHandler` composes the existing prepare and commit handlers with
  `RangeLockChecks`; it adds no rule of its own. It answers which step produced the response
  (`KeyValueFinalizeStage`), because the coordinator reports a refused prepare, a range lock, and a failed
  commit differently. A range lock found there leaves the prepared intent in place; the coordinator rolls it
  back. `TransactionCoordinator.TryFinalizeInOneActorTurn` reproduces every state transition, result, reason,
  metric and exception of the three-message path.
- **Idempotency.** The fused handler answers `Committed` first when the transaction already committed on this
  actor, so a repeated message cannot report a failure for a commit that happened.
- **Actor turn.** `KeyValueRequestType.RunActorTurn` carries an `IKeyValueActorTurn`. The actor sets
  `turnRunning`, calls the turn, and serves the turn's requests through `DispatchInline`, which runs the same
  `RunHandler` switch the mailbox path runs.
- **How a request becomes inline.** `ScriptActorTurn` sets the thread-static `KeyValueInlineScope` around the
  synchronous start of a local operation; `KeyValueRequestPool.Rent` stamps the request; and
  `KeyValueActorRouters.AskKeyValueActor` serves a stamped request inline. The local operations — request
  construction, response mapping, retry loops — are reused unchanged.
- **Script side.** The five key-value commands send through `ScriptTransactionContext.ActorTurn` when it is
  set. `ScriptTransactionExecutor.HasActorTurnShape` is the allow-list, remembered on the cached tree's root.

Invariants to keep:

1. **A turn never sends the actor's own mailbox a request and waits for the answer.** That answer cannot come
   until the turn ends. Every request a turn issues must be served inline. This is why the finalize and the
   rollback have in-turn forms, and why batched writes, bucket reads and scans end the turn instead.
2. **A turn never waits.** No `SLEEP`, no unbounded loop, no network call. Every other key of the actor is
   stalled while a turn runs. Ephemeral handlers complete synchronously; persistent ones can defer to disk,
   which is one reason the persistent key space is excluded.
3. **The inline scope is thread static and is cleared before the first await.** An async local would flow
   into detached work that outlives the turn and would then touch actor state from outside the actor. A local
   operation that is called inside a turn must rent its request before its first await.
4. **`DispatchInline` answers only while a turn runs**, and a turn cannot start another turn.
5. **The turn releases in its `finally`.** The actor must never be left holding the transaction's lock or
   staged write when the turn ends, whatever the script threw.
6. **The allow-list names what is allowed.** A node type added to the language stays out of turns until
   someone decides it belongs.
7. **Same handlers, same order.** A change to the general path's sequence for a single ephemeral key must be
   mirrored in the turn, and the parity tests (`TestScriptActorTurns`, `TestFusedEphemeralFinalize`) are the
   place that proves it.
