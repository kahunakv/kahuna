
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Records the MVCC read pin of a transactional latest read that was answered off the actor from the key's
/// published head record. The read served the record because the record showed that the transaction held
/// nothing on the key; this message, queued before the transaction's next request for the key can be issued,
/// leaves behind exactly the pin the actor path would have created: the revision the transaction observed, so
/// its later write or commit of the key is validated against that revision and aborts if the head moved.
///
/// <para>The pin is recorded at the record's revision even when the head already advanced — the transaction
/// read the older revision, and its next operation on the key must answer <c>Aborted</c> as it would today. A
/// pin that already exists for the transaction is kept: the earlier pin is the stricter one.</para>
///
/// <para>The reader registered the pin before it sent it (see <see cref="PublishedHeadTable.TryRegisterPin"/>).
/// The registration ends here, on every path, and only after the pin is in the MVCC table: the add retracts
/// the record of the key, so the transaction shows in the registry or in a live record at any time.</para>
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
            context.PublishedHeads.ForgetPin(message.TransactionId, message.Key, head);
            return null;
        }

        entry.MvccEntries ??= new();

        if (entry.MvccEntries.ContainsKey(message.TransactionId))
        {
            context.PublishedHeads.ForgetPin(message.TransactionId, message.Key, head);
            return null;
        }

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

        // The add retracted the record of the key, and the record published at the end of this turn shows the
        // pin. The registration is removed only now, so the transaction is visible in one of the two at any time.
        context.PublishedHeads.ForgetPin(message.TransactionId, message.Key, head);

        return null;
    }
}
