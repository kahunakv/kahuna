using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

using Kahuna.Utils;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Prefix, bucket and range scans executed against this node's actors and, where the scan reaches past
/// what is resident, the backend.
///
/// Two properties of these scans are load-bearing and easy to lose. The union across actor instances is
/// a <b>newest-wins</b> merge on (revision, last-modified), not first-wins: a first-wins union resurrects
/// committed deletes carried by a lagging instance. And a refused page is not an empty page — the results
/// distinguish "scanned and found nothing" from "could not be scanned", which is what keeps a split or
/// merge from mistaking an in-flight write for an empty range.
/// </summary>
internal sealed class LocalScanOperations
{
    private const int MaxRetries = 3;

    private readonly KeyValuesRuntime runtime;

    private readonly LocalKeyValueOperations localKeyValues;

    internal LocalScanOperations(KeyValuesRuntime runtime, LocalKeyValueOperations localKeyValues)
    {
        this.runtime = runtime;
        this.localKeyValues = localKeyValues;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KeyValueLocator locator => runtime.Locator;

    private PreparedIntentStore preparedIntentStore => runtime.PreparedIntentStore;

    private TransactionRecordStore transactionRecordStore => runtime.TransactionRecordStore;

    private Kahuna.Server.Persistence.Backend.IPersistenceBackend persistenceBackend => runtime.PersistenceBackend;

    private IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances => runtime.Routers.EphemeralInstances;

    private IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> persistentInstances => runtime.Routers.PersistentInstances;

    private KeyValueActorRing ephemeralKeyValuesRouter => runtime.Routers.Ephemeral;

    private KeyValueActorRing persistentKeyValuesRouter => runtime.Routers.Persistent;

    private Task<IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>?> TryRouteForeignScanDecisions(
        IReadOnlyList<PreparedIntent> windowIntents,
        HLCTimestamp scanTransactionId,
        CancellationToken cancellationToken) =>
        localKeyValues.TryRouteForeignScanDecisions(windowIntents, scanTransactionId, cancellationToken);

    private static Task<KeyValueResponse?> AskKeyValueActor(
        KeyValueActorRing router,
        KeyValueRequest request) => KeyValueActorRouters.AskKeyValueActor(router, request);


    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix 
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return locator.ScanAllByPrefix(prefixKeyName, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Scans the current node and returns key/value pairs by prefix
    /// The returned values aren't consistent, they can contain stale data
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, bool includeTombstones = false)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.ScanByPrefix,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            prefixKeyName,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );

        request.ReadTimestamp = readTimestamp;
        request.IncludeTombstones = includeTombstones;

        List<(string, ReadOnlyKeyValueEntry)> items = [];
        
        if (durability == KeyValueDurability.Ephemeral)
        {
            List<Task<KeyValueResponse?>> tasks = new(ephemeralInstances.Count);
            
            // Ephemeral GetByBucket does a brute force search on every ephemeral actor
            foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in ephemeralInstances)
                tasks.Add(actor.Ask(request));
            
            KeyValueResponse?[] responses = await Task.WhenAll(tasks);

            foreach (KeyValueResponse? response in responses)
            {
                if (response is { Type: KeyValueResponseType.Get, Items: not null })
                    items.AddRange(response.Items);    
            }
            
            return new(KeyValueResponseType.Get, items);
        }

        if (durability == KeyValueDurability.Persistent)
        {
            List<Task<KeyValueResponse?>> tasks = new(persistentInstances.Count);
            
            // Persistent GetByBucket does a brute force search on every persistent actor
            foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in persistentInstances)
                tasks.Add(actor.Ask(request));
            
            KeyValueResponse?[] responses = await Task.WhenAll(tasks);

            foreach (KeyValueResponse? response in responses)
            {
                if (response is { Type: KeyValueResponseType.Get, Items: not null })
                    items.AddRange(response.Items);    
            }
            
            return new(KeyValueResponseType.Get, items);
        }

        throw new KahunaServerException("Unknown durability");
    }
    
    /// <summary>
    /// Scans the current node and returns key/value pairs by prefix
    /// The returned values aren't consistent, they can contain stale data
    /// </summary>
    /// <param name="prefixKeyName"></param>    
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByBucketResult> ScanByPrefixFromDisk(string prefixKeyName, HLCTimestamp readTimestamp, bool includeTombstones = false)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.ScanByPrefixFromDisk,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            prefixKeyName,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            null
        );

        request.ReadTimestamp = readTimestamp;
        request.IncludeTombstones = includeTombstones;

        KeyValueResponse? response = await AskKeyValueActor(persistentKeyValuesRouter, request);

        if (response is null)
            return new(KeyValueResponseType.Errored, []);
        
        if (response is { Type: KeyValueResponseType.Get, Items: not null })
            return new(response.Type, response.Items); 
        
        return new(response.Type, []);
    }

    /// <summary>
    /// Returns a consistent snapshot of key/value pairs that matches the specified prefix
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp transactionId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.GetByBucket,
            transactionId,
            HLCTimestamp.Zero,
            prefixKeyName,
            null,
            null,
            -1,
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
                    return new(KeyValueResponseType.Errored, []);

                if (response.Type != KeyValueResponseType.WaitingForReplication)
                {
                    // A bucket scan that retries only because it meets a committed-but-unsettled foreign intent
                    // whose canonical record lives on another partition would otherwise re-scan until settlement.
                    // Route those intents' decisions to their anchor leaders once, off the mailbox, and re-issue
                    // with the resolved set so the overlay serves them immediately. Persistent scans only; a
                    // genuinely undecided intent still stands at MustRetry.
                    if (durability != KeyValueDurability.Ephemeral
                        && !attemptedRoutedResolve
                        && response.Type == KeyValueResponseType.MustRetry)
                    {
                        attemptedRoutedResolve = true;

                        IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>? routed =
                            await TryRouteForeignScanDecisions(
                                preparedIntentStore.SnapshotBucket(prefixKeyName), transactionId, CancellationToken.None);

                        if (routed is not null)
                        {
                            request.ForeignScanDecisions = routed;
                            continue;
                        }
                    }

                    if (response is { Type: KeyValueResponseType.Get, Items: not null })
                        return new(response.Type, response.Items);
                    return new(response.Type, []);
                }

                if (Environment.TickCount64 >= deadline)
                    return new(KeyValueResponseType.MustRetry, []);

                Transactions.DurableTransactionMetrics.AddKvRetryWait("GetByBucket_6080");
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
    /// Executes a bounded, cursor-paged range scan over keys starting with <paramref name="prefix"/>.
    /// </summary>
    public async Task<KeyValueGetByRangeResult> GetByRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.RentRange(
            transactionId,
            prefix,
            startKey,
            startInclusive,
            endKey,
            endInclusive,
            limit,
            readTimestamp,
            durability,
            null
        );

        bool attemptedRoutedResolve = false;

        try
        {
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return new(KeyValueResponseType.Errored, [], null, false);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("GetByRange_6135");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                // A page that retries only because it meets a committed-but-unsettled foreign intent whose canonical
                // record lives on another partition would otherwise re-scan until settlement propagates. Route those
                // intents' decisions to their anchor leaders once, off the mailbox, and re-issue with the resolved
                // set so the overlay serves them immediately. Persistent scans only (ephemeral carry no durable
                // intents); a genuinely undecided intent still stands at MustRetry.
                if (durability != KeyValueDurability.Ephemeral
                    && !attemptedRoutedResolve
                    && (response.RangeResult?.Type ?? response.Type) == KeyValueResponseType.MustRetry)
                {
                    attemptedRoutedResolve = true;

                    (string rStart, bool rStartIncl, string? rEnd, bool rEndIncl) =
                        Handlers.TryGetByRangeHandler.ComputeBounds(request);

                    IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>? routed =
                        await TryRouteForeignScanDecisions(
                            preparedIntentStore.SnapshotScanWindow(rStart, rStartIncl, rEnd, rEndIncl),
                            transactionId, CancellationToken.None);

                    if (routed is not null)
                    {
                        request.ForeignScanDecisions = routed;
                        continue;
                    }
                }

                if (response.RangeResult is not null)
                    return response.RangeResult;

                return new(response.Type, [], null, false);
            }

            return new(KeyValueResponseType.MustRetry, [], null, false);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }
}
