using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

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
            runtime.Configuration.PublishedHeadReads);
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
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueReadMode readMode = KeyValueReadMode.Default
    )
    {
        LocalPointRead read = await TryGetValueLocal(transactionId, key, revision, readTimestamp, durability, readMode);
        return (read.Type, read.Entry);
    }

    /// <summary>
    /// <see cref="TryGetValue"/>, reporting whether the answer came from the owning actor's published head
    /// record (no actor message) or from a mailbox round trip. A read with a timestamp, so every statement
    /// read, always takes the actor path: the published record answers latest reads only.
    /// </summary>
    public async Task<LocalPointRead> TryGetValueLocal(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueReadMode readMode = KeyValueReadMode.Default
    )
    {
        if (IsGatedKey(key))
            return new(KeyValueResponseType.MustRetry, null, false);

        if (publishedHeads.TryRead(transactionId, key, revision, readTimestamp, durability, out ReadOnlyKeyValueEntry? published))
            return new(KeyValueResponseType.Get, published, true);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await TryGetValueFromActor(transactionId, key, revision, readTimestamp, durability, readMode);
        return new(type, entry, false);
    }

    private async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValueFromActor(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueReadMode readMode
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

        try
        {
            // Exponential back-off loop, matching the LocateAndScanRange page-retry contract.
            // WaitingForReplication covers two cases:
            //   • Short replication lag (ReplicationIntent): typically resolves in < 10 ms.
            //   • Safe-time wait (pending write intent whose commit ts ≤ readTimestamp): can
            //     last up to the intent TTL (DefaultTxCompleteTimeout ≈ 15 s). The exponential
            //     back-off naturally amortises both: a replication lag resolves in the first 1-2
            //     iterations; a safe-time wait resolves once the in-flight write commits or the
            //     intent expires and the actor clears it.
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
                    return (KeyValueResponseType.Errored, null);

                if (response.Type != KeyValueResponseType.WaitingForReplication)
                    return (response.Type, response.Entry);

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
                    return (KeyValueResponseType.MustRetry, null);

                Transactions.DurableTransactionMetrics.AddKvRetryWait("TryGetValue_4018");
                await Task.Delay(backoffMs);
                backoffMs = Math.Min(backoffMs * 2, 1000);
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
                await Task.Delay(backoffMs);
                backoffMs = Math.Min(backoffMs * 2, 1000);
            }
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>[] tasks = new Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            (string key, long revision, KeyValueDurability durability) item = keys[i];
            tasks[i] = TryGetManyValue(item);
        }

        return [.. await Task.WhenAll(tasks)];

        async Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> TryGetManyValue(
            (string key, long revision, KeyValueDurability durability) item
        )
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await TryGetValue(
                transactionId,
                item.key,
                item.revision,
                readTimestamp,
                item.durability
            );

            return (type, item.key, item.durability, entry);
        }
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
        KeyValueReadMode readMode = KeyValueReadMode.Default
    )
    {
        KeyValueProbeResult[] results = new KeyValueProbeResult[keys.Count];

        if (keys.Count == 0)
            return results;

        for (int i = 0; i < keys.Count; i++)
        {
            if (!IsGatedKey(keys[i]))
                continue;

            Array.Fill(results, KeyValueProbeResult.MustRetry);
            return results;
        }

        KeyValueActorRing router = durability == KeyValueDurability.Ephemeral ? ephemeralKeyValuesRouter : persistentKeyValuesRouter;

        long start = Environment.TickCount64;
        int backoffMs = 1;

        // Indexes (into keys) still to ask, in input order. Null means every key.
        List<int>? pending = null;
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

                KeyValueResponse? response = await AskProbe(router, transactionId, readTimestamp, batchKeys, batchHints, lockMode, expiresMs, durability, readMode);

                if (response?.ProbeResults is not { } answers || answers.Length != count)
                {
                    // Answered as a whole (refused, or no answer at all): every key still pending takes that answer
                    // and keeps the lock an earlier attempt reported.
                    KeyValueResponseType whole = response is null
                        ? KeyValueResponseType.Errored
                        : response.ProbeResults is null ? response.Type : KeyValueResponseType.Errored;

                    for (int j = 0; j < count; j++)
                    {
                        int index = pending is null ? j : pending[j];
                        results[index] = new(whole, null, HLCTimestamp.Zero, results[index].Locked);
                    }

                    return results;
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
                await Task.Delay(backoffMs);
                backoffMs = Math.Min(backoffMs * 2, 1000);
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
        KeyValueReadMode readMode)
    {
        // The first key routes the request: every key of a key space hashes to the same actor.
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryProbeMany, transactionId, HLCTimestamp.Zero, keys[0], null, null, -1,
            KeyValueFlags.None, expiresMs, HLCTimestamp.Zero, durability, 0, 0, default);

        request.ReadTimestamp = readTimestamp;
        request.ReadMode = readMode;
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
    /// Probes several locally owned keys for concurrent write intents, returning one result per requested key.
    /// The keys are dispatched to their owning actors independently, as the many-key reads do: the grouping that
    /// pays is by node, upstream in the locator, not by actor here.
    /// </summary>
    public async Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntentValues(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys
    )
    {
        Task<(KeyValueResponseType, string, KeyValueDurability)>[] tasks = new Task<(KeyValueResponseType, string, KeyValueDurability)>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
            tasks[i] = CheckWriteIntent(keys[i]);

        return [.. await Task.WhenAll(tasks)];

        async Task<(KeyValueResponseType, string, KeyValueDurability)> CheckWriteIntent(KeyValueConflictProbe item)
        {
            KeyValueResponseType type = await TryCheckWriteIntentValue(transactionId, item.Key, item.Durability, item.Checks, item.BaseRevision);

            return (type, item.Key, item.Durability);
        }
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
            return KeyValueResponseType.MustRetry;

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

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryCheckWriteIntent,
            transactionId,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            // CompareRevision carries the staged-base check's validated base; -1 (also the pool's
            // idle value) doubles as "the base was a non-existent key", which only the StagedBase
            // check consults.
            baseRevision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        request.ConflictChecks = checks;

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

            return response?.Type ?? KeyValueResponseType.Errored;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }
}
