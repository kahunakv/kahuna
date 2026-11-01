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
        request.ConflictPolicy = YieldingIntentPolicyScope.Current;

        try
        {
            bool attemptedRoutedResolve = false;
            bool lastAnswerWasWait = false;

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
                    lastAnswerWasWait = true;

                    // A wait on a foreign durable intent whose canonical record is remote: resolve the decision off
                    // the mailbox once and re-issue, so a committed holder is materialized and the write proceeds
                    // instead of waiting for its settlement.
                    if (response.DenialCause != WriteDenialCause.None
                        && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                    {
                        attemptedRoutedResolve = true;
                        if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                            continue;
                    }

                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TrySetKeyValue_3609");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                lastAnswerWasWait = false;

                // A MustRetry over a foreign prepared intent whose canonical record is remote (an older actor
                // answer): resolve the decision off the mailbox once and re-issue so a committed intent is
                // materialized instead of bouncing the caller until settlement.
                if (response.Type == KeyValueResponseType.MustRetry
                    && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                {
                    attemptedRoutedResolve = true;
                    if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                        continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            // The budget ran out. A wait (a replication intent in flight, a holder still settling or still deciding)
            // is lag the caller can give a longer budget than a deadlock wait, so it is answered as the wait it was;
            // anything else is the transient MustRetry.
            return (lastAnswerWasWait ? KeyValueResponseType.WaitingForReplication : KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Attempts to set multiple key-value pairs on the node.
    ///
    /// <para>A non-transactional batch is independent committed writes: every item goes through the ordinary
    /// <c>TrySet</c> path, launched together, and answers for itself. A transactional batch stages every item or
    /// none: the items each actor owns go to that actor as one <see cref="KeyValueRequestType.TrySetMany"/> message,
    /// which checks them all and stages them in one turn only when every one passed. A batch that spans several
    /// actors runs a check round on every actor first and a stage round only when every item passed, so a refused
    /// item on one actor stages nothing on another. The window between the two rounds is the one place a foreign
    /// write can refuse one actor's items after another actor staged its own; under pessimistic locking the caller
    /// holds exclusive locks on every key, so it cannot happen there.</para>
    /// </summary>
    /// <param name="items">A list of key-value set requests to be processed.</param>
    /// <param name="checkOnly">
    /// Check every item and stage nothing: the check round of a transactional batch that spans several partition
    /// leaders. An item that would stage answers <see cref="KeyValueResponseType.NotApplied"/>. Refused with
    /// <see cref="KeyValueResponseType.InvalidInput"/> for a non-transactional batch, which stages nothing to check.
    /// </param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a list of responses for each set request, in request order.</returns>
    public Task<List<KahunaSetKeyValueResponseItem>> SetManyNodeKeyValue(List<KahunaSetKeyValueRequestItem> items, bool checkOnly = false)
    {
        HLCTimestamp transactionId = items.Count > 0 ? items[0].TransactionId : HLCTimestamp.Zero;

        if (transactionId == HLCTimestamp.Zero)
        {
            if (checkOnly)
                return Task.FromResult(AllItems(items, KeyValueResponseType.InvalidInput));

            return SetManyIndependentNodeKeyValue(items);
        }

        return SetManyTransactionalNodeKeyValue(items, transactionId, checkOnly);
    }

    private static List<KahunaSetKeyValueResponseItem> AllItems(List<KahunaSetKeyValueRequestItem> items, KeyValueResponseType type)
    {
        List<KahunaSetKeyValueResponseItem> responses = new(items.Count);

        foreach (KahunaSetKeyValueRequestItem item in items)
            responses.Add(new() { Key = item.Key ?? "", Type = type, Durability = item.Durability });

        return responses;
    }

    /// <summary>
    /// Stages a transactional batch all or none across the actors that own its items (see <see cref="SetManyNodeKeyValue"/>).
    /// </summary>
    private async Task<List<KahunaSetKeyValueResponseItem>> SetManyTransactionalNodeKeyValue(List<KahunaSetKeyValueRequestItem> items, HLCTimestamp transactionId, bool checkOnly)
    {
        // Group the items by the actor that owns them, in request order inside each group. The ring hashes a key
        // by its key space, so one group is one key space (or several that share an actor); the actor serves the
        // group in one turn.
        Dictionary<(KeyValueDurability Durability, int Actor), List<int>> groups = [];

        for (int i = 0; i < items.Count; i++)
        {
            KahunaSetKeyValueRequestItem item = items[i];
            KeyValueActorRing ring = item.Durability == KeyValueDurability.Ephemeral ? ephemeralKeyValuesRouter : persistentKeyValuesRouter;
            (KeyValueDurability, int) owner = (item.Durability, ring.ActorIndexOf(item.Key ?? ""));

            if (groups.TryGetValue(owner, out List<int>? indices))
                indices.Add(i);
            else
                groups[owner] = [i];
        }

        KahunaSetKeyValueResponseItem[] responses = new KahunaSetKeyValueResponseItem[items.Count];

        if (groups.Count == 1)
        {
            foreach (List<int> indices in groups.Values)
                await SetManyGroupNodeKeyValue(items, indices, transactionId, checkOnly, responses);

            return [.. responses];
        }

        // Several actors: check everywhere, then stage everywhere. Each actor's turn is atomic for its own items;
        // the check round is what keeps a refusal on one actor from leaving another actor's items staged.
        await RunRound(checkOnly: true);

        if (checkOnly)
            return [.. responses];

        foreach (KahunaSetKeyValueResponseItem response in responses)
        {
            if (response.Type != KeyValueResponseType.NotApplied)
                return [.. responses];
        }

        if (runtime.BeforeSetManyStageRoundForTesting is { } beforeStageRound)
            await beforeStageRound(transactionId);

        await RunRound(checkOnly: false);

        return [.. responses];

        Task RunRound(bool checkOnly)
        {
            Task[] tasks = new Task[groups.Count];
            int g = 0;

            foreach (List<int> indices in groups.Values)
                tasks[g++] = SetManyGroupNodeKeyValue(items, indices, transactionId, checkOnly, responses);

            return Task.WhenAll(tasks);
        }
    }

    /// <summary>
    /// Sends the items at <paramref name="indices"/>, all owned by one actor, as one batch message and writes each
    /// item's answer at its position in <paramref name="responses"/>. A batch the actor answered as a whole (refused,
    /// or not admitted) gives every item that answer.
    /// </summary>
    private async Task SetManyGroupNodeKeyValue(
        List<KahunaSetKeyValueRequestItem> items,
        List<int> indices,
        HLCTimestamp transactionId,
        bool checkOnly,
        KahunaSetKeyValueResponseItem[] responses)
    {
        KahunaSetKeyValueRequestItem[] groupItems = new KahunaSetKeyValueRequestItem[indices.Count];
        for (int j = 0; j < indices.Count; j++)
            groupItems[j] = items[indices[j]];

        KahunaSetKeyValueRequestItem first = groupItems[0];

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TrySetMany,
            transactionId,
            HLCTimestamp.Zero,
            first.Key ?? "",
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            first.Durability,
            0,
            0,
            default
        );

        request.SetManyItems = groupItems;
        request.CheckOnly = checkOnly;

        try
        {
            KeyValueResponse? response = first.Durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            for (int j = 0; j < indices.Count; j++)
            {
                KahunaSetKeyValueRequestItem item = groupItems[j];

                if (response is null)
                {
                    responses[indices[j]] = new() { Key = item.Key ?? "", Type = KeyValueResponseType.Errored, Durability = item.Durability };
                    continue;
                }

                if (response.SetManyResults is { } results)
                {
                    KeyValueSetManyResult result = results[j];
                    responses[indices[j]] = new()
                    {
                        Key = item.Key ?? "",
                        Type = result.Type,
                        Revision = result.Revision,
                        LastModified = result.LastModified,
                        Durability = item.Durability,
                        HolderTransactionId = result.HolderTransactionId
                    };
                    continue;
                }

                responses[indices[j]] = new()
                {
                    Key = item.Key ?? "",
                    Type = response.Type,
                    Revision = response.Revision,
                    LastModified = response.Ticket,
                    Durability = item.Durability,
                    HolderTransactionId = response.HolderTransactionId
                };
            }
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Sets the items of a non-transactional batch as independent committed writes.
    /// </summary>
    private async Task<List<KahunaSetKeyValueResponseItem>> SetManyIndependentNodeKeyValue(List<KahunaSetKeyValueRequestItem> items)
    {
        // Fan every item out through the ordinary TrySet path, launched together. Persistent items meet in the
        // shared partition write aggregator — coalescing across this call, other concurrent many-key calls, and
        // single writes to the same partition — while ephemeral items keep their existing per-key behavior.
        // tasks[i] corresponds to items[i], so response order matches input order.
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
            request.ConflictPolicy = item.ConflictPolicy;

            try
            {
                KeyValueResponse? response;

                if (item.Durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return new() { Key = item.Key ?? "", Type = KeyValueResponseType.Errored, Durability = item.Durability };

                // A residual WaitingForReplication (a replication intent in flight, a holder still settling or
                // still deciding) is lag: answered as the wait it is, so the caller can give it a longer budget
                // than a deadlock wait, and never a terminal error.
                return new()
                {
                    Key = item.Key ?? "",
                    Type = response.Type,
                    Revision = response.Revision,
                    LastModified = response.Ticket,
                    Durability = item.Durability,
                    // A denial over a foreign writer names that writer so the caller can apply wait-die, or wait
                    // for the settlement of a holder that already decided.
                    HolderTransactionId = response.HolderTransactionId
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

            request.ConflictPolicy = item.ConflictPolicy;

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

                // The budget ran out on a wait: lag, answered as the wait it is so the caller can give it a longer
                // budget than a deadlock wait.
                return new()
                {
                    Key = item.Key ?? "",
                    Type = KeyValueResponseType.WaitingForReplication,
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

        request.ConflictPolicy = YieldingIntentPolicyScope.Current;

        try
        {
            bool attemptedRoutedResolve = false;
            bool lastAnswerWasWait = false;

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
                    lastAnswerWasWait = true;

                    // A wait on a foreign durable intent whose canonical record is remote: resolve the decision off
                    // the mailbox once and re-issue, so a committed holder is materialized and the write proceeds
                    // instead of waiting for its settlement.
                    if (response.DenialCause != WriteDenialCause.None
                        && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                    {
                        attemptedRoutedResolve = true;
                        if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                            continue;
                    }

                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryExtendKeyValue_3840");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                lastAnswerWasWait = false;

                // A MustRetry over a foreign prepared intent whose canonical record is remote (an older actor
                // answer): resolve the decision off the mailbox once and re-issue so a committed intent is
                // materialized instead of bouncing the caller until settlement.
                if (response.Type == KeyValueResponseType.MustRetry
                    && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                {
                    attemptedRoutedResolve = true;
                    if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                        continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            // The budget ran out. A wait (a replication intent in flight, a holder still settling or still deciding)
            // is lag the caller can give a longer budget than a deadlock wait, so it is answered as the wait it was;
            // anything else is the transient MustRetry.
            return (lastAnswerWasWait ? KeyValueResponseType.WaitingForReplication : KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
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

        request.ConflictPolicy = YieldingIntentPolicyScope.Current;

        try
        {
            bool attemptedRoutedResolve = false;
            bool lastAnswerWasWait = false;

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
                    lastAnswerWasWait = true;

                    // A wait on a foreign durable intent whose canonical record is remote: resolve the decision off
                    // the mailbox once and re-issue, so a committed holder is materialized and the write proceeds
                    // instead of waiting for its settlement.
                    if (response.DenialCause != WriteDenialCause.None
                        && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                    {
                        attemptedRoutedResolve = true;
                        if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                            continue;
                    }

                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryDeleteKeyValue_3914");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                lastAnswerWasWait = false;

                // A MustRetry over a foreign prepared intent whose canonical record is remote (an older actor
                // answer): resolve the decision off the mailbox once and re-issue so a committed intent is
                // materialized instead of bouncing the caller until settlement.
                if (response.Type == KeyValueResponseType.MustRetry
                    && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
                {
                    attemptedRoutedResolve = true;
                    if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                        continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            // The budget ran out. A wait (a replication intent in flight, a holder still settling or still deciding)
            // is lag the caller can give a longer budget than a deadlock wait, so it is answered as the wait it was;
            // anything else is the transient MustRetry.
            return (lastAnswerWasWait ? KeyValueResponseType.WaitingForReplication : KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
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
