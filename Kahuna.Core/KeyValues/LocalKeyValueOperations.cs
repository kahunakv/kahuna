using System.Runtime.CompilerServices;

using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

using Kahuna.Utils;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Executes key-value reads and writes against this node's actor rings — the node-local half of every
/// operation, reached either directly or through the locator once a request has been routed to the
/// partition's leader.
///
/// Each entry point rents a pooled request, drives the same bounded retry loop, and returns the pooled
/// request in a <c>finally</c>. The loop absorbs two distinct transients: an in-flight replication
/// (<c>WaitingForReplication</c>) is simply waited out, while a <c>MustRetry</c> caused by a foreign
/// prepared intent triggers a single off-mailbox decision resolution before the request is re-issued —
/// bounded to one attempt so a stalled decision surfaces to the caller instead of looping.
/// </summary>
internal sealed class LocalKeyValueOperations
{
    private const int MaxRetries = 3;

    private readonly KeyValuesRuntime runtime;

    internal LocalKeyValueOperations(KeyValuesRuntime runtime) => this.runtime = runtime;

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private IInterNodeCommunication interNodeCommunication => runtime.InterNodeCommunication;

    private KeyValueLocator locator => runtime.Locator;

    private TransactionRecordStore transactionRecordStore => runtime.TransactionRecordStore;

    private PreparedIntentStore preparedIntentStore => runtime.PreparedIntentStore;

    private KeyValueActorRing ephemeralKeyValuesRouter => runtime.Routers.Ephemeral;

    private KeyValueActorRing persistentKeyValuesRouter => runtime.Routers.Persistent;

    private Task<TransactionRecord?> LookupDurableRecordRouted(HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken) =>
        runtime.DurableReplication.LookupDurableRecordRouted(transactionId, epoch, anchorKey, cancellationToken);

    private static ValueTask<KeyValueResponse?> AskKeyValueActor(
        KeyValueActorRing router,
        KeyValueRequest request) => KeyValueActorRouters.AskKeyValueActor(router, request);


    /// <summary>
    /// Passes a TrySet request to the key/value actor for the given key/value key.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        long routedGeneration = 0
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TrySet,
            transactionId,
            HLCTimestamp.Zero,
            key,
            value,
            compareValue,
            compareRevision,
            flags,
            expiresMs,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        request.RoutedGeneration = routedGeneration;

        try
        {
            bool attemptedRoutedResolve = false;
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TrySetKeyValue_3609");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                // A MustRetry caused by a foreign prepared intent whose canonical record is remote: resolve the
                // decision off the mailbox once and re-issue so a committed intent is materialized (write proceeds)
                // instead of bouncing the caller until settlement.
                if (response.Type == KeyValueResponseType.MustRetry
                    && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                {
                    attemptedRoutedResolve = true;
                    if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                        continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            return (KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Attempts to set multiple key-value pairs on the node.
    /// </summary>
    /// <param name="items">A list of key-value set requests to be processed.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a list of responses for each set request, indicating the outcome of the operation.</returns>
    public async Task<List<KahunaSetKeyValueResponseItem>> SetManyNodeKeyValue(List<KahunaSetKeyValueRequestItem> items)
    {
        // Fan every item out through the ordinary TrySet path, launched together. Persistent items meet in the
        // shared partition write aggregator — coalescing across this call, other concurrent many-key calls, and
        // single writes to the same partition — while ephemeral and transactional items keep their existing
        // per-key behavior. tasks[i] corresponds to items[i], so response order matches input order.
        Task<KahunaSetKeyValueResponseItem>[] tasks = new Task<KahunaSetKeyValueResponseItem>[items.Count];
        for (int i = 0; i < items.Count; i++)
            tasks[i] = SetOneNodeKeyValue(items[i]);

        return [.. await Task.WhenAll(tasks)];

        async Task<KahunaSetKeyValueResponseItem> SetOneNodeKeyValue(KahunaSetKeyValueRequestItem item)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
                KeyValueRequestType.TrySet,
                item.TransactionId,
                HLCTimestamp.Zero,
                item.Key ?? "",
                item.Value,
                item.CompareValue,
                item.CompareRevision,
                item.Flags,
                item.ExpiresMs,
                HLCTimestamp.Zero,
                item.Durability,
                0,
                0,
                default
            );

            request.RoutedGeneration = item.RoutedGeneration;

            try
            {
                KeyValueResponse? response;

                if (item.Durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return new() { Key = item.Key ?? "", Type = KeyValueResponseType.Errored, Durability = item.Durability };

                // A residual WaitingForReplication (a live replication intent the caller should retry against)
                // is retryable, not a terminal error.
                if (response.Type == KeyValueResponseType.WaitingForReplication)
                    return new() { Key = item.Key ?? "", Type = KeyValueResponseType.MustRetry, Durability = item.Durability };

                return new()
                {
                    Key = item.Key ?? "",
                    Type = response.Type,
                    Revision = response.Revision,
                    LastModified = response.Ticket,
                    Durability = item.Durability
                };
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }
    }


    public async Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items)
    {
        // Fan every item out through the ordinary TryDelete path, launched together. Persistent items meet in
        // the shared partition write aggregator (coalescing across this and other concurrent calls), while
        // ephemeral and transactional items keep their existing per-key behavior. Order matches input order.
        Task<KahunaDeleteKeyValueResponseItem>[] tasks = new Task<KahunaDeleteKeyValueResponseItem>[items.Count];
        for (int i = 0; i < items.Count; i++)
            tasks[i] = DeleteOneNodeKeyValue(items[i]);

        return [.. await Task.WhenAll(tasks)];

        async Task<KahunaDeleteKeyValueResponseItem> DeleteOneNodeKeyValue(KahunaDeleteKeyValueRequestItem item)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
                KeyValueRequestType.TryDelete,
                item.TransactionId,
                HLCTimestamp.Zero,
                item.Key ?? "",
                null,
                null,
                -1,
                KeyValueFlags.None,
                0,
                HLCTimestamp.Zero,
                item.Durability,
                0,
                0,
                default
            );

            try
            {
                LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
                for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
                {
                    KeyValueResponse? response;

                    if (item.Durability == KeyValueDurability.Ephemeral)
                        response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                    else
                        response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                    if (response is null)
                        return new()
                        {
                            Key = item.Key ?? "",
                            Type = KeyValueResponseType.Errored,
                            Revision = -1,
                            LastModified = HLCTimestamp.Zero,
                            Durability = item.Durability
                        };

                    if (response.Type == KeyValueResponseType.WaitingForReplication)
                    {
                        Transactions.DurableTransactionMetrics.AddKvRetryWait("DeleteManyNodeKeyValue_3760");
                        if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                        continue;
                    }

                    return new()
                    {
                        Key = item.Key ?? "",
                        Type = response.Type,
                        Revision = response.Revision,
                        LastModified = response.Ticket,
                        Durability = item.Durability
                    };
                }

                return new()
                {
                    Key = item.Key ?? "",
                    Type = KeyValueResponseType.MustRetry,
                    Revision = -1,
                    LastModified = HLCTimestamp.Zero,
                    Durability = item.Durability
                };
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }
    }


    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        HLCTimestamp transactionId,
        string key, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryExtend,
            transactionId,
            HLCTimestamp.Zero,
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            expiresMs, 
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        try
        {
            bool attemptedRoutedResolve = false;

            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryExtendKeyValue_3840");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                // A MustRetry caused by a foreign prepared intent whose canonical record is remote: resolve the
                // decision off the mailbox once and re-issue so a committed intent is materialized (write proceeds)
                // instead of bouncing the caller until settlement.
                if (response.Type == KeyValueResponseType.MustRetry
                    && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                {
                    attemptedRoutedResolve = true;
                    if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                        continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            return (KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryDelete, 
            transactionId,
            HLCTimestamp.Zero,
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        try
        {
            bool attemptedRoutedResolve = false;
            
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryDeleteKeyValue_3914");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                // A MustRetry caused by a foreign prepared intent whose canonical record is remote: resolve the
                // decision off the mailbox once and re-issue so a committed intent is materialized (write proceeds)
                // instead of bouncing the caller until settlement.
                if (response.Type == KeyValueResponseType.MustRetry
                    && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                {
                    attemptedRoutedResolve = true;
                    if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                        continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            return (KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }
    
    internal async Task<bool> TryRouteForeignDecision(KeyValueRequest request, string key, HLCTimestamp transactionId, KeyValueDurability durability, bool alreadyAttempted)
    {
        if (alreadyAttempted || durability == KeyValueDurability.Ephemeral)
            return false;

        if (preparedIntentStore.Get(key) is not { Resolution: PreparedIntentResolution.Pending } foreignIntent
            || foreignIntent.TransactionId == transactionId
            || transactionRecordStore.Get(foreignIntent.TransactionId, foreignIntent.Epoch) is not null)
            return false;

        TransactionRecord? record;
        try
        {
            record = await LookupDurableRecordRouted(
                foreignIntent.TransactionId, foreignIntent.Epoch, foreignIntent.RecordAnchorKey, CancellationToken.None).ConfigureAwait(false);
        }
        catch (PartitionNotHostedException)
        {
            // The anchor partition's leader could not be resolved from here right now (placement
            // view still warming). The resolution is best-effort: the read's retryable outcome
            // stands and a later attempt — or settlement — resolves the intent.
            return true;
        }

        if (record is { IsTerminal: true })
            request.ForeignDecisionHint = new ForeignDecisionHint(foreignIntent.TransactionId, foreignIntent.Epoch, record.Decision);

        return true;
    }

    /// <summary>
    /// Off-mailbox resolution of the still-pending foreign intents a scan page meets. A scan window can straddle
    /// many foreign intents anchored on different partitions, so — unlike a point read's single
    /// <see cref="ForeignDecisionHint"/> — this resolves the whole set at once: for every pending intent in the
    /// window whose canonical record is not co-located, it routes a lookup to that intent's anchor-partition leader
    /// and collects the terminal decisions into a map the re-issued scan applies through its overlay. Returns the
    /// map when at least one committed/aborted decision was resolved (the caller re-issues the scan once with it),
    /// or null when nothing terminal was found — in which case the scan's <c>MustRetry</c> stands (an intent still
    /// genuinely undecided, or the retry had another cause). Never routes an intent belonging to the scanning
    /// transaction itself.
    /// </summary>
    internal async Task<IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>?> TryRouteForeignScanDecisions(
        IReadOnlyList<PreparedIntent> windowIntents,
        HLCTimestamp scanTransactionId,
        CancellationToken cancellationToken)
    {
        Dictionary<(HLCTimestamp, long), TransactionDecision>? decisions = null;

        foreach (PreparedIntent intent in windowIntents)
        {
            if (intent.Resolution != PreparedIntentResolution.Pending
                || intent.TransactionId == scanTransactionId
                || transactionRecordStore.Get(intent.TransactionId, intent.Epoch) is not null)
                continue;

            TransactionRecord? record;
            try
            {
                record = await LookupDurableRecordRouted(
                    intent.TransactionId, intent.Epoch, intent.RecordAnchorKey, cancellationToken).ConfigureAwait(false);
            }
            catch (PartitionNotHostedException)
            {
                // This intent's anchor leader cannot be resolved from here right now; skip it —
                // the scan's retryable outcome stands for anything left unresolved.
                continue;
            }

            if (record is { IsTerminal: true })
                (decisions ??= [])[(intent.TransactionId, intent.Epoch)] = record.Decision;
        }

        return decisions;
    }

}
