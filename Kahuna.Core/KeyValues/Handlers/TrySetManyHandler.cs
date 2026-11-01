using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stages the items of one transactional set-many that this actor owns, in one actor turn, every item or none.
///
/// <para>The turn runs two passes. The check pass evaluates each item exactly as a single transactional set would
/// (a key this yielding transaction lost, a live replication intent, a foreign lock or intent and its conflict policy,
/// a committed head past the transaction's copy, the condition flags) and stages nothing. The stage pass runs only
/// when every item passed, and stages each item exactly as the single-item path does. Nothing else runs on the actor
/// between the two passes, which is what makes check-then-stage atomic without a lock.</para>
///
/// <para>When an item did not pass, every item answers its own result: the refused item <see cref="KeyValueResponseType.NotSet"/>,
/// a transient item its transient type with the holder, and every item that passed <see cref="KeyValueResponseType.NotApplied"/>.
/// A request marked <see cref="KeyValueRequest.CheckOnly"/> stops after the check pass, so a batch that spans several
/// actors or several partition leaders can check everywhere before it stages anywhere; its passed items answer
/// <see cref="KeyValueResponseType.NotApplied"/> too.</para>
///
/// <para>Persistent items whose heads are not resident are loaded with one batched backend read before the check
/// pass, so a large batch does not park the actor on one awaited read per item.</para>
/// </summary>
internal sealed class TrySetManyHandler : BaseHandler
{
    private readonly TrySetHandler setHandler;

    public TrySetManyHandler(KeyValueContext context, TrySetHandler setHandler) : base(context)
    {
        this.setHandler = setHandler;
    }

    public async ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        KahunaSetKeyValueRequestItem[]? items = message.SetManyItems;

        if (items is null || items.Length == 0 || message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.InvalidInputResponse;

        // One stamp for the whole turn: every item is checked, and then staged, at the same time, so an expiry that
        // the check evaluated one way cannot evaluate the other way in the stage that follows.
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);

        Dictionary<string, ReadOnlyKeyValueEntry?>? preloadedHeads = null;

        if (message.Durability == KeyValueDurability.Persistent)
        {
            string[] keys = new string[items.Length];
            for (int i = 0; i < items.Length; i++)
                keys[i] = items[i].Key ?? "";

            preloadedHeads = await LoadMissingPersistentHeads(keys);
        }

        KeyValueRequest view = new(
            KeyValueRequestType.TrySet, message.TransactionId, HLCTimestamp.Zero, items[0].Key ?? "", null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, message.Durability, 0, message.PartitionId, default);

        KeyValueSetManyResult[] results = new KeyValueSetManyResult[items.Length];
        bool allPassed = true;

        for (int i = 0; i < items.Length; i++)
        {
            ResetView(view, message, items[i]);

            KeyValueResponse? refusal = await setHandler.CheckTransactional(view, currentTime, preloadedHeads);

            if (refusal is null)
            {
                results[i] = KeyValueSetManyResult.NotApplied;
                continue;
            }

            results[i] = KeyValueSetManyResult.From(refusal);
            allPassed = false;
        }

        if (!allPassed || message.CheckOnly)
            return KeyValueResponse.ForSetMany(results);

        for (int i = 0; i < items.Length; i++)
        {
            ResetView(view, message, items[i]);

            KeyValueResponse staged = await setHandler.ExecuteTransactional(view, currentTime, preloadedHeads);
            results[i] = KeyValueSetManyResult.From(staged);

            // The stage re-evaluates what the check evaluated in this same turn, at the same stamp, so it cannot
            // disagree. A disagreement would leave the batch partially staged: count it and name the key.
            if (staged.Type != KeyValueResponseType.Set)
            {
                Transactions.DurableTransactionMetrics.SetManyStageDisagreements.Add(1);
                context.Logger.LogWarning(
                    "KeyValueActor/TrySetMany: item '{Key}' passed its check but answered {Type} when staged; the batch of {Count} items is partially staged",
                    items[i].Key, staged.Type, items.Length);
            }
        }

        return KeyValueResponse.ForSetMany(results);
    }

    private static void ResetView(KeyValueRequest view, KeyValueRequest message, KahunaSetKeyValueRequestItem item)
    {
        view.Reset(
            KeyValueRequestType.TrySet, message.TransactionId, HLCTimestamp.Zero, item.Key ?? "", item.Value, item.CompareValue,
            item.CompareRevision, item.Flags, item.ExpiresMs, HLCTimestamp.Zero, message.Durability, 0, message.PartitionId, default);

        view.RoutedGeneration = item.RoutedGeneration;
        view.ConflictPolicy = item.ConflictPolicy;
    }
}
