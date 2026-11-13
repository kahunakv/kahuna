using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
using Kahuna.Utils;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The answer of a leader-local point get and which step produced it: <see cref="ServedFromPublishedHead"/> is
/// true when the owning actor's published head record answered it on the caller's thread, false when an actor
/// round trip did.
/// </summary>
internal readonly record struct LocalPointRead(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry, bool ServedFromPublishedHead);

/// <summary>
/// Executes key-value reads — get, exists, their many-key variants, and the write-intent probes — against
/// this node's actor rings.
///
/// Reads share the write path's retry loop, including the single off-mailbox resolution of a foreign prepared
/// intent: a read blocked behind a committed-but-unsettled intent resolves the decision once and re-issues,
/// so it observes the committed value rather than bouncing the caller.
/// </summary>
internal sealed class LocalKeyValueReadOperations
{
    private const int MaxRetries = 3;

    private readonly KeyValuesRuntime runtime;

    private readonly LocalKeyValueOperations localKeyValues;

    private readonly PublishedHeadReader publishedHeads;

    internal LocalKeyValueReadOperations(KeyValuesRuntime runtime, LocalKeyValueOperations localKeyValues)
    {
        this.runtime = runtime;
        this.localKeyValues = localKeyValues;

        publishedHeads = new PublishedHeadReader(
            runtime.Routers.Ephemeral,
            runtime.Routers.Persistent,
            (runtime.PersistenceBackend as UnflushedOverlayPersistenceBackend)?.UnflushedWrites,
            runtime.PreparedIntentStore,
            runtime.Raft,
            runtime.Configuration.PublishedHeadReads,
            runtime.SnapshotFloorStore?.InstalledHistory,
            runtime.RangeMapStore);
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KeyValueLocator locator => runtime.Locator;

    private KeyValueActorRing ephemeralKeyValuesRouter => runtime.Routers.Ephemeral;

    private KeyValueActorRing persistentKeyValuesRouter => runtime.Routers.Persistent;

    private Task<bool> TryRouteForeignDecision(KeyValueRequest request, string key, HLCTimestamp transactionId, KeyValueDurability durability, bool alreadyAttempted) =>
        localKeyValues.TryRouteForeignDecision(request, key, transactionId, durability, alreadyAttempted);

    private static ValueTask<KeyValueResponse?> AskKeyValueActor(
        KeyValueActorRing router,
        KeyValueRequest request) => KeyValueActorRouters.AskKeyValueActor(router, request);

    /// <summary>
    /// The serving gate on the non-locating read path. The inter-node batch reads land here without passing
    /// the locator: the sender grouped the keys by the leader it resolved, and a node whose projection of the
    /// key's partition is gated as incomplete (see <see cref="PartitionDivergenceContainment"/>) may still be
    /// that leader for the moment it takes to relinquish. Nothing is served from the incomplete state: the
    /// key answers MustRetry and the sender re-resolves. One flag read when nothing is gated.
    /// </summary>
    private bool IsGatedKey(string key) =>
        runtime.DivergenceContainment.AnyGated && runtime.DivergenceContainment.IsGated(locator.RouteKey(key));

    /// <summary>
    /// Passes a Get request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueReadMode readMode = KeyValueReadMode.Default,
        KeyValueIntentWait intentWait = default
    )
    {
        ValueTask<LocalPointRead> read = TryGetValueLocal(transactionId, key, revision, readTimestamp, durability, readMode, intentWait);

        if (!read.IsCompletedSuccessfully)
            return Awaited(read);

        LocalPointRead answer = read.Result;
        return Task.FromResult((answer.Type, answer.Entry));

        static async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> Awaited(ValueTask<LocalPointRead> pending)
        {
            LocalPointRead answer = await pending;
            return (answer.Type, answer.Entry);
        }
    }

    /// <summary>
    /// <see cref="TryGetValue"/>, reporting whether the answer came from the owning actor's published head
    /// record (no actor message) or from a mailbox round trip. A read with a timestamp is answered from the
    /// record only when the head was committed at or before that timestamp (see <see cref="PublishedHeadReader"/>).
    /// <para>
    /// A <see cref="ValueTask{TResult}"/>, and not an async method: the published record answers on the calling
    /// thread, and that answer must not cost a task object here and another in every caller that forwards it.
    /// Await the result exactly once. As with an async method, a failure is carried by the result and is never
    /// thrown from the call itself, so a caller that starts several reads before it awaits any of them cannot
    /// lose the ones it already started.
    /// </para>
    /// </summary>
    public ValueTask<LocalPointRead> TryGetValueLocal(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueReadMode readMode = KeyValueReadMode.Default,
        KeyValueIntentWait intentWait = default
    )
    {
        try
        {
            if (IsGatedKey(key))
                return new(new LocalPointRead(KeyValueResponseType.MustRetry, null, false));

            if (publishedHeads.TryRead(transactionId, key, revision, readTimestamp, durability, readMode, out ReadOnlyKeyValueEntry? published))
                return new(new LocalPointRead(KeyValueResponseType.Get, published, true));
        }
        catch (Exception ex)
        {
            return ValueTask.FromException<LocalPointRead>(ex);
        }

        return TryGetValueFromActor(transactionId, key, revision, readTimestamp, durability, readMode, intentWait);
    }

    private async ValueTask<LocalPointRead> TryGetValueFromActor(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueReadMode readMode,
        KeyValueIntentWait intentWait
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryGet,
            transactionId,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            revision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        request.ReadTimestamp = readTimestamp;
        request.ReadMode = readMode;
        request.IntentWait = intentWait;

        try
        {
            // Poll loop (ReadWaitBackoff), matching the LocateAndScanRange page-retry contract.
            // WaitingForReplication covers two cases:
            //   • Short replication lag (ReplicationIntent): typically resolves in < 10 ms.
            //   • Safe-time wait (pending write intent whose commit ts ≤ readTimestamp): can
            //     last up to the intent TTL (DefaultTxCompleteTimeout ≈ 15 s). The poll serves
            //     the read within one short sleep of either: the lag resolving, or the in-flight
            //     write committing, rolling back, or its intent expiring and the actor clearing it.
            // The deadline is DefaultTxCompleteTimeout + 1 500 ms buffer.
            int backoffMs = 1;
            long deadline = Environment.TickCount64 + 16_500;
            bool attemptedRoutedResolve = false;

            while (true)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return new(KeyValueResponseType.Errored, null, false);

                if (response.Type != KeyValueResponseType.WaitingForReplication)
                    return new(response.Type, response.Entry, false);

                // A wait caused by a foreign prepared intent whose canonical decision is not resolvable locally (a
                // remote anchor) would otherwise spin until settlement propagates the decision here. Route the
                // lookup to the anchor-partition leader once, off the mailbox, and re-issue the read with the
                // terminal decision so it resolves immediately (the committed value, or the prior value on abort).
                if (await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                {
                    attemptedRoutedResolve = true;
                    if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                        continue; // resolve now, no backoff
                }

                if (Environment.TickCount64 >= deadline)
                    return new(KeyValueResponseType.MustRetry, null, false);

                Transactions.DurableTransactionMetrics.AddKvRetryWait("TryGetValue_4018");
                await Task.Delay(ReadWaitBackoff.Next(ref backoffMs, deadline));
            }
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Off-mailbox resolution of a remote-anchor foreign intent's decision for a stalled read: when the read's
    /// <c>WaitingForReplication</c> wait is caused by a foreign prepared intent whose canonical record is not local,
    /// route the lookup to the anchor-partition leader once and, on a terminal decision, stamp a
    /// <see cref="ForeignDecisionHint"/> on the request so a re-issued read resolves it immediately instead of
    /// spinning until settlement. Returns true when a routed attempt was made this iteration (whether or not it
    /// produced a terminal decision), so the caller records it and stops re-routing. Ephemeral reads carry no
    /// durable intents, and a hint is only set when the intent's canonical record genuinely lives elsewhere.
    /// </summary>
    /// <summary>
    /// Passes a Exists request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability
    )
    {
        if (IsGatedKey(key))
            return (KeyValueResponseType.MustRetry, null);

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryExists,
            transactionId,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            revision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        request.ReadTimestamp = readTimestamp;

        try
        {
            int backoffMs = 1;
            long deadline = Environment.TickCount64 + 16_500;
            bool attemptedRoutedResolve = false;

            while (true)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, null);

                if (response.Type != KeyValueResponseType.WaitingForReplication)
                    return (response.Type, response.Entry);

                // Resolve a remote-anchor foreign intent's decision once, off the mailbox, and re-issue with the
                // hint so the read resolves immediately instead of spinning until settlement (see TryGetValue).
                if (await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                {
                    attemptedRoutedResolve = true;
                    if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                        continue;
                }

                if (Environment.TickCount64 >= deadline)
                    return (KeyValueResponseType.MustRetry, null);

                Transactions.DurableTransactionMetrics.AddKvRetryWait("TryExistsValue_4158");
                await Task.Delay(ReadWaitBackoff.Next(ref backoffMs, deadline));
            }
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Reads many locally owned keys, each answered as a single <see cref="TryGetValue"/> of it is. Returns one
    /// result per key, in input order.
    /// <para>
    /// Every read is started before any is awaited, so the keys that need their actor are asked together. A key
    /// the published head record answers is answered in the starting pass, on the calling thread, and costs
    /// nothing beyond its entry; a batch of such keys completes without suspending at all. Only the keys that
    /// went to an actor are awaited, by one state machine for the batch.
    /// </para>
    /// </summary>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results = new(keys.Count);
        List<(int Index, ValueTask<LocalPointRead> Read)>? pending = null;

        for (int i = 0; i < keys.Count; i++)
        {
            (string key, long revision, KeyValueDurability durability) = keys[i];

            ValueTask<LocalPointRead> read = TryGetValueLocal(transactionId, key, revision, readTimestamp, durability);

            if (read.IsCompletedSuccessfully)
            {
                LocalPointRead answer = read.Result;
                results.Add((answer.Type, key, durability, answer.Entry));
                continue;
            }

            // The slot is filled when the read completes; the placeholder keeps the results in input order.
            results.Add((KeyValueResponseType.Errored, key, durability, null));
            (pending ??= new(keys.Count - i)).Add((i, read));
        }

        return pending is null ? Task.FromResult(results) : AwaitPendingReads(results, pending);
    }

    /// <summary>
    /// Completes a batched read whose starting pass left reads in flight. Every pending read is awaited, also
    /// after one of them failed, so no started read is left unobserved; the first failure is thrown once all of
    /// them have finished, which is when a wait on all of them at once reports it too.
    /// </summary>
    internal static async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> AwaitPendingReads(
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results,
        List<(int Index, ValueTask<LocalPointRead> Read)> pending)
    {
        System.Runtime.ExceptionServices.ExceptionDispatchInfo? failure = null;

        for (int i = 0; i < pending.Count; i++)
        {
            (int index, ValueTask<LocalPointRead> read) = pending[i];

            try
            {
                LocalPointRead answer = await read;
                (_, string key, KeyValueDurability durability, _) = results[index];
                results[index] = (answer.Type, key, durability, answer.Entry);
            }
            catch (Exception ex)
            {
                failure ??= System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex);
            }
        }

        failure?.Throw();

        return results;
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>[] tasks = new Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            (string key, long revision, KeyValueDurability durability) item = keys[i];
            tasks[i] = TryExistsManyValue(item);
        }

        return [.. await Task.WhenAll(tasks)];

        async Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> TryExistsManyValue(
            (string key, long revision, KeyValueDurability durability) item
        )
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await TryExistsValue(
                transactionId,
                item.key,
                item.revision,
                readTimestamp,
                item.durability
            );

            return (type, item.key, item.durability, entry);
        }
    }

    /// <summary>
    /// How long a batched probe waits out a transient refusal of one of its Shared locks (an in-flight replication
    /// intent, or a durable intent whose decision is not known here) before it answers that key <c>MustRetry</c>.
    /// The same bound as a single range-lock acquire.
    /// </summary>
    private const int ProbeLockWaitMs = 3_000;

    /// <summary>
    /// How long a batched probe waits out a read that answers <c>WaitingForReplication</c> before it answers that
    /// key <c>MustRetry</c>. The same bound as a single read (<see cref="TryGetValueFromActor"/>).
    /// </summary>
    private const int ProbeReadWaitMs = 16_500;

    /// <summary>
    /// Reads many keys of one key space, owned by this node, with one actor message per attempt (see
    /// <see cref="Handlers.TryProbeManyHandler"/>). Returns one result per key, in input order.
    ///
    /// <para>Waits are handled per key with the loop a single read or a single range-lock acquire uses: a key that
    /// answers <c>WaitingForReplication</c> is asked again, alone with the other waiting keys, with exponential
    /// back-off, and a wait caused by a foreign prepared intent whose decision lives on another partition routes
    /// that decision once and re-asks with it at once. A Shared-lock wait stops the batch at that key, so it and
    /// every key after it are asked again. A read wait gives up after <see cref="ProbeReadWaitMs"/> and a lock wait
    /// after <see cref="ProbeLockWaitMs"/>, as <c>MustRetry</c>.</para>
    ///
    /// <para>A key's <see cref="KeyValueProbeResult.Locked"/> stays set once any attempt reported it: the lock is
    /// session-owned until the transaction finalizes, and reporting it is what lets the caller release it.</para>
    /// </summary>
    public async Task<KeyValueProbeResult[]> TryProbeMany(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        IReadOnlyList<string> keys,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability,
        KeyValueReadMode readMode = KeyValueReadMode.Default,
        KeyValueIntentWait intentWait = default
    )
    {
        if (keys.Count == 0)
            return [];

        // The first attempt usually answers every key, and the actor's array of answers is then the result as it
        // is. This array exists only for a probe that is refused as a whole or that asks some keys again, where
        // the answers of several attempts are merged.
        KeyValueProbeResult[]? results = null;

        for (int i = 0; i < keys.Count; i++)
        {
            if (!IsGatedKey(keys[i]))
                continue;

            results = new KeyValueProbeResult[keys.Count];
            Array.Fill(results, KeyValueProbeResult.MustRetry);
            return results;
        }

        KeyValueActorRing router = durability == KeyValueDurability.Ephemeral ? ephemeralKeyValuesRouter : persistentKeyValuesRouter;

        long start = Environment.TickCount64;
        int backoffMs = 1;

        // Indexes (into keys) still to ask, in input order. Null means every key.
        List<int>? pending = null;

        // A probe that takes no lock answers each key by the rules of a single read, so a key whose published
        // head record can answer a single read is answered from it here, and only the others go to the actor.
        // The keys are then read at slightly different instants instead of in one actor turn, as a sequence of
        // single reads would be; a probe that locks keeps the grant and the read in one turn and is not served
        // here.
        if (lockMode == KeyValueProbeLock.None)
        {
            for (int i = 0; i < keys.Count; i++)
            {
                if (publishedHeads.TryRead(transactionId, keys[i], -1, readTimestamp, durability, readMode, out ReadOnlyKeyValueEntry? published))
                {
                    if (results is null)
                    {
                        results = new KeyValueProbeResult[keys.Count];
                        for (int j = 0; j < i; j++)
                            (pending ??= []).Add(j);
                    }

                    results[i] = new KeyValueProbeResult(KeyValueResponseType.Get, published, HLCTimestamp.Zero, false);
                }
                else if (results is not null)
                    (pending ??= []).Add(i);
            }

            if (results is not null && pending is null)
                return results;
        }
        Transactions.Data.ForeignDecisionHint[]? hints = null;
        bool[]? routed = null;

        KeyValueRequest? scratch = null;

        try
        {
            while (true)
            {
                int count = pending?.Count ?? keys.Count;
                string[] batchKeys = new string[count];
                Transactions.Data.ForeignDecisionHint[]? batchHints = hints is null ? null : new Transactions.Data.ForeignDecisionHint[count];

                for (int j = 0; j < count; j++)
                {
                    int index = pending is null ? j : pending[j];
                    batchKeys[j] = keys[index];
                    if (batchHints is not null)
                        batchHints[j] = hints![index];
                }

                KeyValueResponse? response = await AskProbe(router, transactionId, readTimestamp, batchKeys, batchHints, lockMode, expiresMs, durability, readMode, intentWait);

                if (response?.ProbeResults is not { } answers || answers.Length != count)
                {
                    // Answered as a whole (refused, or no answer at all): every key still pending takes that answer
                    // and keeps the lock an earlier attempt reported.
                    KeyValueResponseType whole = response is null
                        ? KeyValueResponseType.Errored
                        : response.ProbeResults is null ? response.Type : KeyValueResponseType.Errored;

                    results ??= new KeyValueProbeResult[keys.Count];

                    for (int j = 0; j < count; j++)
                    {
                        int index = pending is null ? j : pending[j];
                        results[index] = new(whole, null, HLCTimestamp.Zero, results[index].Locked);
                    }

                    return results;
                }

                if (results is null)
                {
                    // First attempt, one answer per key in input order. The actor writes nothing to the array
                    // after it replied, so when no key waits it is handed to the caller without a copy.
                    bool anyWaiting = false;

                    for (int j = 0; j < count; j++)
                    {
                        if (answers[j].Type != KeyValueResponseType.WaitingForReplication)
                            continue;

                        anyWaiting = true;
                        break;
                    }

                    if (!anyWaiting)
                        return answers;

                    results = new KeyValueProbeResult[keys.Count];
                }

                List<int>? waiting = null;
                bool lockWait = false;
                bool retryNow = false;

                for (int j = 0; j < count; j++)
                {
                    int index = pending is null ? j : pending[j];
                    KeyValueProbeResult answer = answers[j];
                    results[index] = answer with { Locked = answer.Locked || results[index].Locked };

                    if (answer.Type != KeyValueResponseType.WaitingForReplication)
                        continue;

                    // A lock wait stopped the batch here: the keys after it were not asked and go round again too.
                    bool stoppedHere = lockMode == KeyValueProbeLock.Shared && !answer.Locked;

                    waiting ??= [];
                    waiting.Add(index);

                    if (await TryRouteProbeDecision(index, keys[index], transactionId, durability))
                        retryNow = true;

                    if (!stoppedHere)
                        continue;

                    lockWait = true;
                    for (int k = j + 1; k < count; k++)
                        waiting.Add(pending is null ? k : pending[k]);

                    break;
                }

                if (waiting is null)
                    return results;

                long elapsed = Environment.TickCount64 - start;
                if (elapsed >= (lockWait ? ProbeLockWaitMs : ProbeReadWaitMs))
                {
                    // Out of budget: the keys still waiting answer MustRetry, as a single read or acquire does.
                    foreach (int index in waiting)
                        results[index] = KeyValueProbeResult.MustRetry with { Locked = results[index].Locked };

                    return results;
                }

                pending = waiting;

                if (retryNow)
                    continue; // a decision was resolved: ask again at once

                Transactions.DurableTransactionMetrics.AddKvRetryWait("TryProbeMany");
                await Task.Delay(ReadWaitBackoff.Next(ref backoffMs, start + (lockWait ? ProbeLockWaitMs : ProbeReadWaitMs)));
            }
        }
        finally
        {
            if (scratch is not null)
                KeyValueRequestPool.Return(scratch);
        }

        // Routes the foreign decision behind a waiting key once per key, and keeps the hint for the next ask.
        async Task<bool> TryRouteProbeDecision(int index, string key, HLCTimestamp txId, KeyValueDurability keyDurability)
        {
            routed ??= new bool[keys.Count];
            if (routed[index])
                return false;

            scratch ??= KeyValueRequestPool.Rent(
                KeyValueRequestType.TryGet, txId, HLCTimestamp.Zero, key, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, keyDurability, 0, 0, default);

            scratch.ForeignDecisionHint = default;

            if (!await TryRouteForeignDecision(scratch, key, txId, keyDurability, alreadyAttempted: false))
                return false;

            routed[index] = true;

            if (scratch.ForeignDecisionHint.TransactionId == HLCTimestamp.Zero)
                return false;

            hints ??= new Transactions.Data.ForeignDecisionHint[keys.Count];
            hints[index] = scratch.ForeignDecisionHint;
            return true;
        }
    }

    private static async Task<KeyValueResponse?> AskProbe(
        KeyValueActorRing router,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string[] keys,
        Transactions.Data.ForeignDecisionHint[]? hints,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability,
        KeyValueReadMode readMode,
        KeyValueIntentWait intentWait)
    {
        // The first key routes the request: every key of a key space hashes to the same actor.
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryProbeMany, transactionId, HLCTimestamp.Zero, keys[0], null, null, -1,
            KeyValueFlags.None, expiresMs, HLCTimestamp.Zero, durability, 0, 0, default);

        request.ReadTimestamp = readTimestamp;
        request.ReadMode = readMode;
        request.IntentWait = intentWait;
        request.ProbeKeys = keys;
        request.ProbeHints = hints;
        request.ProbeLock = lockMode;

        try
        {
            return await AskKeyValueActor(router, request);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// The most probes one actor answers in one turn of a batched conflict probe. A probe of a key that is not
    /// resident reads the backend inside the turn, so an unbounded group would hold the actor's other requests
    /// behind all of those reads; a larger group is sent as several requests, which the actor takes in order.
    /// </summary>
    private const int MaxConflictProbesPerTurn = 64;

    /// <summary>Marks a probe that no actor request carries: it was answered here, or it is asked alone.</summary>
    private const int NotGrouped = -2;

    /// <summary>
    /// One request of a batched conflict probe that is waiting for its answer: the probes it carries, or the
    /// single probe that is answered without an actor.
    /// </summary>
    private struct PendingConflictProbe
    {
        /// <summary>Index of the first probe the request carries.</summary>
        public int First;

        /// <summary>How many probes the request carries, chained from <see cref="First"/>.</summary>
        public int Count;

        /// <summary>The request sent to the actor, returned to the pool once its reply was awaited.</summary>
        public KeyValueRequest? Request;

        /// <summary>The actor's reply. Awaited exactly once, by the loop that collects the answers.</summary>
        public ValueTask<KeyValueResponse?> Reply;

        /// <summary>The answer of a probe that is asked alone through <see cref="TryCheckWriteIntentValue"/>.</summary>
        public Task<KeyValueResponseType>? Alone;

        /// <summary>A request that was sent to its actor, with the reply it is waiting for.</summary>
        public static PendingConflictProbe Sent(int first, int count, KeyValueRequest request, ValueTask<KeyValueResponse?> reply) =>
            new() { First = first, Count = count, Request = request, Reply = reply };
    }

    /// <summary>
    /// Probes several locally owned keys for the conflict classes each probe names, returning one result per
    /// requested key, in request order. The probes are grouped by the actor that owns each key and one request
    /// per group carries them, so an actor answers its probes in one turn; a key that is alone on its actor is
    /// asked as a single probe. Every request is sent before any reply is awaited.
    /// </summary>
    public Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntentValues(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys
    )
    {
        return keys.Count == 1 ? CheckOne(transactionId, keys[0]) : CheckGrouped(transactionId, keys);

        async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> CheckOne(HLCTimestamp txId, KeyValueConflictProbe probe)
        {
            KeyValueResponseType type = await TryCheckWriteIntentValue(txId, probe.Key, probe.Durability, probe.Checks, probe.BaseRevision);

            return [(type, probe.Key, probe.Durability)];
        }
    }

    private async Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> CheckGrouped(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys
    )
    {
        int count = keys.Count;

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results = new(count);

        if (count == 0)
            return results;

        KeyValueActorRing persistentRing = persistentKeyValuesRouter;
        KeyValueActorRing ephemeralRing = ephemeralKeyValuesRouter;

        // An owner is one actor of one ring: the persistent actors first, then the ephemeral ones.
        int persistentOwners = persistentRing.ActorCount;
        int owners = persistentOwners + ephemeralRing.ActorCount;

        // heads[owner] is the first probe of that owner's chain and next[i] the probe after i in it; both end at -1.
        int[] heads = System.Buffers.ArrayPool<int>.Shared.Rent(owners);
        int[] next = System.Buffers.ArrayPool<int>.Shared.Rent(count);
        PendingConflictProbe[] pending = System.Buffers.ArrayPool<PendingConflictProbe>.Shared.Rent(count);
        int pendingCount = 0;

        Transactions.Data.ForeignDecisionHint[]? hints = null;
        KeyValueRequest? scratch = null;

        try
        {
            for (int i = 0; i < count; i++)
            {
                KeyValueConflictProbe probe = keys[i];
                results.Add((KeyValueResponseType.Errored, probe.Key, probe.Durability));

                // The leader-term check is answered by the locator and not by an actor: it is asked alone.
                if ((probe.Checks & KeyValueConflictChecks.LeaderTerm) != 0)
                {
                    next[i] = NotGrouped;
                    pending[pendingCount++] = new()
                    {
                        First = i,
                        Count = 1,
                        Alone = TryCheckWriteIntentValue(transactionId, probe.Key, probe.Durability, probe.Checks, probe.BaseRevision)
                    };
                    continue;
                }

                if (IsGatedKey(probe.Key))
                {
                    logger.LogCommitProbeGated(transactionId, probe.Key);
                    results[i] = (KeyValueResponseType.MustRetry, probe.Key, probe.Durability);
                    next[i] = NotGrouped;
                    continue;
                }

                next[i] = -1;

                // Resolve a remote-anchor concurrent writer's decision off the mailbox first, so a committed/aborted
                // intent whose record lives on another node is not mis-flagged as a live undecided conflict by the
                // actor. Only the write-intent check consults foreign intents.
                if ((probe.Checks & KeyValueConflictChecks.WriteIntent) == 0 || probe.Durability == KeyValueDurability.Ephemeral)
                    continue;

                scratch ??= KeyValueRequestPool.Rent(
                    KeyValueRequestType.TryCheckWriteIntent, transactionId, HLCTimestamp.Zero, probe.Key, null, null, -1,
                    KeyValueFlags.None, 0, HLCTimestamp.Zero, probe.Durability, 0, 0, default);

                scratch.ForeignDecisionHint = default;

                await TryRouteForeignDecision(scratch, probe.Key, transactionId, probe.Durability, alreadyAttempted: false);

                if (scratch.ForeignDecisionHint.TransactionId == HLCTimestamp.Zero)
                    continue;

                hints ??= new Transactions.Data.ForeignDecisionHint[count];
                hints[i] = scratch.ForeignDecisionHint;
            }

            heads.AsSpan(0, owners).Fill(-1);

            // Chain from the last probe back, so each owner's chain runs in request order.
            for (int i = count - 1; i >= 0; i--)
            {
                if (next[i] == NotGrouped)
                    continue;

                KeyValueConflictProbe probe = keys[i];

                int owner = probe.Durability == KeyValueDurability.Ephemeral
                    ? persistentOwners + ephemeralRing.ActorIndexOf(probe.Key)
                    : persistentRing.ActorIndexOf(probe.Key);

                next[i] = heads[owner];
                heads[owner] = i;
            }

            // Send every request before any reply is awaited: the actors answer side by side.
            for (int owner = 0; owner < owners; owner++)
            {
                KeyValueActorRing ring = owner < persistentOwners ? persistentRing : ephemeralRing;

                for (int first = heads[owner]; first >= 0;)
                {
                    int members = 0;
                    int after = first;

                    while (after >= 0 && members < MaxConflictProbesPerTurn)
                    {
                        after = next[after];
                        members++;
                    }

                    KeyValueRequest request = members == 1
                        ? RentWriteIntentProbe(transactionId, keys[first], hints is null ? default : hints[first])
                        : RentManyWriteIntentProbes(transactionId, keys, next, first, members, hints);

                    pending[pendingCount++] = PendingConflictProbe.Sent(first, members, request, AskKeyValueActor(ring, request));

                    first = after;
                }
            }

            for (int p = 0; p < pendingCount; p++)
            {
                PendingConflictProbe item = pending[p];

                if (item.Alone is { } alone)
                {
                    (_, string aloneKey, KeyValueDurability aloneDurability) = results[item.First];
                    results[item.First] = (await alone, aloneKey, aloneDurability);
                    continue;
                }

                KeyValueResponse? response;

                try
                {
                    response = await item.Reply;
                }
                finally
                {
                    // The actor is done with the request once its reply was awaited, also when the reply faulted.
                    pending[p].Request = null;
                    KeyValueRequestPool.Return(item.Request!);
                }

                KeyValueConflictProbeResult[]? answers = response?.ConflictProbeResults;
                int index = item.First;

                for (int j = 0; j < item.Count; j++, index = next[index])
                {
                    KeyValueConflictProbe probe = keys[index];

                    // A request the actor answered as a whole (refused, or not admitted) gives every probe that answer.
                    KeyValueResponseType type = response is null
                        ? KeyValueResponseType.Errored
                        : answers is not null ? answers[j].Type : response.Type;

                    HLCTimestamp commitFloor = response is null
                        ? HLCTimestamp.Zero
                        : answers is not null ? answers[j].CommitFloor : response.CommitFloor;

                    results[index] = (CompleteWriteIntentProbe(transactionId, probe.Key, probe.Checks, type, commitFloor), probe.Key, probe.Durability);
                }
            }

            return results;
        }
        finally
        {
            if (scratch is not null)
                KeyValueRequestPool.Return(scratch);

            // A request whose reply was never awaited (an earlier reply faulted) may still be in its actor's hands:
            // it is left to the collector and not handed back to the pool.
            System.Buffers.ArrayPool<int>.Shared.Return(heads);
            System.Buffers.ArrayPool<int>.Shared.Return(next);
            System.Buffers.ArrayPool<PendingConflictProbe>.Shared.Return(pending, clearArray: true);
        }
    }

    /// <summary>Rents the actor request of one conflict probe.</summary>
    private static KeyValueRequest RentWriteIntentProbe(HLCTimestamp transactionId, KeyValueConflictProbe probe, Transactions.Data.ForeignDecisionHint hint)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryCheckWriteIntent,
            transactionId,
            HLCTimestamp.Zero,
            probe.Key,
            null,
            null,
            // CompareRevision carries the staged-base check's validated base; -1 (also the pool's
            // idle value) doubles as "the base was a non-existent key", which only the StagedBase
            // check consults.
            probe.BaseRevision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            probe.Durability,
            0,
            0,
            default
        );

        request.ConflictChecks = probe.Checks;
        request.ForeignDecisionHint = hint;

        return request;
    }

    /// <summary>
    /// Rents the actor request that carries <paramref name="members"/> probes of one actor, chained from
    /// <paramref name="first"/> through <paramref name="next"/>. The first probe's key routes the request.
    /// </summary>
    private static KeyValueRequest RentManyWriteIntentProbes(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        int[] next,
        int first,
        int members,
        Transactions.Data.ForeignDecisionHint[]? hints)
    {
        KeyValueConflictProbe[] probes = new KeyValueConflictProbe[members];
        Transactions.Data.ForeignDecisionHint[]? probeHints = null;

        int index = first;

        for (int j = 0; j < members; j++, index = next[index])
        {
            probes[j] = keys[index];

            if (hints is null || hints[index].TransactionId == HLCTimestamp.Zero)
                continue;

            probeHints ??= new Transactions.Data.ForeignDecisionHint[members];
            probeHints[j] = hints[index];
        }

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryCheckManyWriteIntents, transactionId, HLCTimestamp.Zero, probes[0].Key, null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, probes[0].Durability, 0, 0, default);

        request.ConflictProbes = probes;
        request.ProbeHints = probeHints;

        return request;
    }

    /// <summary>
    /// What the caller does with an actor's answer to a conflict probe, whichever request carried the probe: a
    /// refusal is logged, and a commit floor is raised into the open capture.
    /// </summary>
    private KeyValueResponseType CompleteWriteIntentProbe(
        HLCTimestamp transactionId,
        string key,
        KeyValueConflictChecks checks,
        KeyValueResponseType type,
        HLCTimestamp commitFloor)
    {
        if (type == KeyValueResponseType.MustRetry)
            logger.LogCommitProbeActorRefused(transactionId, key, checks);

        // The commit floor a held own staged intent carries travels to the coordinator the way a lock grant's
        // leadership term does: into the capture the probing request opened, which a transport that crossed a
        // process boundary replays on the caller's side.
        if (!commitFloor.IsNull())
            LockGrantScope.RaiseCommitFloor(commitFloor);

        return type;
    }

    /// <summary>
    /// Checks the given key for the conflict classes named in <paramref name="checks"/>: a live write intent
    /// from another transaction (the write-skew guard optimistic transactions apply to their read set), a
    /// foreign range lock covering the key (the decide-time fence applied to a write set), and/or a moved
    /// staged base (the post-prepare fence for a read-modify-write key, compared against
    /// <paramref name="baseRevision"/>). Returns Aborted for an intent/range-lock conflict, NotSet for a
    /// staged-base mismatch; DoesNotExist otherwise. The leader-term check is answered alone, by
    /// <see cref="KeyValueLocator.CheckLeaderTerm"/>, against the term <paramref name="baseRevision"/> carries;
    /// under an unbroken leadership it answers Unlocked when this node stopped honoring a range lock of the
    /// transaction (<see cref="LapsedRangeLockRegistry"/>).
    /// </summary>
    public async Task<KeyValueResponseType> TryCheckWriteIntentValue(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        KeyValueConflictChecks checks = KeyValueConflictChecks.WriteIntent,
        long baseRevision = -1
    )
    {
        if (IsGatedKey(key))
        {
            logger.LogCommitProbeGated(transactionId, key);
            return KeyValueResponseType.MustRetry;
        }

        // The leader-term check asks about the partition, not the key: the key only routed the probe here. No
        // actor holds the answer, so the locator gives it from the Raft term, after it confirmed that this
        // node leads the partition.
        if ((checks & KeyValueConflictChecks.LeaderTerm) != 0)
        {
            KeyValueResponseType leadership = await locator.CheckLeaderTerm(key, baseRevision, CancellationToken.None);

            // An unbroken leadership proves that no leader change dropped the transaction's locks. It does not
            // prove that this leader still honors them: a range lock is a lease, and one whose lease ran out
            // was already stepped over by whoever asked next. The registry is read after the confirmation, so
            // an answer of "held" covers every lapse recorded up to the instant this node was confirmed.
            if (leadership == KeyValueResponseType.DoesNotExist && runtime.LapsedRangeLocks.Contains(transactionId))
                return KeyValueResponseType.Unlocked;

            return leadership;
        }

        KeyValueRequest request = RentWriteIntentProbe(transactionId, new(key, durability, checks, baseRevision), default);

        try
        {
            // Resolve a remote-anchor concurrent writer's decision off the mailbox first, so a committed/aborted
            // intent whose record lives on another node is not mis-flagged as a live undecided conflict here.
            // Only the write-intent check consults foreign intents; a range-lock-only probe needs no lookup.
            if ((checks & KeyValueConflictChecks.WriteIntent) != 0)
                await TryRouteForeignDecision(request, key, transactionId, durability, alreadyAttempted: false);

            KeyValueResponse? response = durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null)
                return KeyValueResponseType.Errored;

            return CompleteWriteIntentProbe(transactionId, key, checks, response.Type, response.CommitFloor);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }
}
