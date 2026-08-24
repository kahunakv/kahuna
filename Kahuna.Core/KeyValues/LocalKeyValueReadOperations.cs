using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

using Kahuna.Utils;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

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

    internal LocalKeyValueReadOperations(KeyValuesRuntime runtime, LocalKeyValueOperations localKeyValues)
    {
        this.runtime = runtime;
        this.localKeyValues = localKeyValues;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KeyValueLocator locator => runtime.Locator;

    private KeyValueActorRing ephemeralKeyValuesRouter => runtime.Routers.Ephemeral;

    private KeyValueActorRing persistentKeyValuesRouter => runtime.Routers.Persistent;

    private Task<bool> TryRouteForeignDecision(KeyValueRequest request, string key, HLCTimestamp transactionId, KeyValueDurability durability, bool alreadyAttempted) =>
        localKeyValues.TryRouteForeignDecision(request, key, transactionId, durability, alreadyAttempted);

    private static Task<KeyValueResponse?> AskKeyValueActor(
        KeyValueActorRing router,
        KeyValueRequest request) => KeyValueActorRouters.AskKeyValueActor(router, request);

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
        KeyValueDurability durability
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
            null
        );

        request.ReadTimestamp = readTimestamp;

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
            null
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
    /// staged-base mismatch; DoesNotExist otherwise.
    /// </summary>
    public async Task<KeyValueResponseType> TryCheckWriteIntentValue(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        KeyValueConflictChecks checks = KeyValueConflictChecks.WriteIntent,
        long baseRevision = -1
    )
    {
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
            null
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
