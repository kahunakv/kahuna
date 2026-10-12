
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Records the MVCC read pin of a transactional latest read that was answered off the actor from the key's
/// published head record. The read served the record because the record proved no transaction held a snapshot
/// on the key; this message, queued before the transaction's next request for the key can be issued, leaves
/// behind exactly the pin the actor path would have created: the revision the transaction observed, so its
/// later write or commit of the key is validated against that revision and aborts if the head moved.
///
/// <para>The pin is recorded at the record's revision even when the head already advanced — the transaction
/// read the older revision, and its next operation on the key must answer <c>Aborted</c> as it would today. A
/// pin that already exists for the transaction is kept: two reads of the same key could both be answered off
/// the actor before the first pin landed, and the earlier pin is the stricter one.</para>
///
/// <para>The entry can be gone when the pin arrives (evicted in the same turn the read was served). No pin can
/// be built then without a backend read on the actor, and the transaction's write would otherwise re-pin at
/// whatever head it finds and overwrite a value it never read. The key is instead marked as taken from the
/// transaction, so its next write of the key answers <c>Aborted</c> — a retryable outcome for a window that is
/// one eviction cycle wide.</para>
/// </summary>
internal sealed class RecordReadPinHandler : BaseHandler
{
    public RecordReadPinHandler(KeyValueContext context) : base(context)
    {
    }

    public KeyValueResponse? Execute(KeyValueRequest message)
    {
        PublishedHead? head = message.ReadPin;
        if (head is null || message.TransactionId == HLCTimestamp.Zero)
            return null;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            context.RecordYieldedIntent(message.Key, message.TransactionId, currentTime + context.SessionOwnedIntentCeilingMs);
            context.Logger.LogWarning(
                "KeyValueActor/RecordReadPin: entry for {Key} was evicted before the read pin of {TransactionId} landed; the transaction's next write of the key aborts",
                message.Key, message.TransactionId);
            return null;
        }

        entry.MvccEntries ??= new();

        if (entry.MvccEntries.ContainsKey(message.TransactionId))
            return null;

        bool mvccDictJustCreated = entry.MvccEntries.Count == 0;

        KeyValueMvccEntry pin = new()
        {
            Value = head.Value,
            Revision = head.Revision,
            Expires = head.Expires,
            LastUsed = currentTime,
            LastModified = head.LastModified,
            State = head.State
        };

        entry.MvccEntries.Add(message.TransactionId, pin);
        context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, pin.Value));

        return null;
    }
}
