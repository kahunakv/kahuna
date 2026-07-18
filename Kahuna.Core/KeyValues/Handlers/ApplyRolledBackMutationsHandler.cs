using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Clears a write intent that was staged for the partition-batched prepare but whose partition batch never
/// proposed — a no-Raft local rollback. Batched prepare installs each participant's write intent while
/// staging (to pin the entry across the propose window); if a later key in the same partition fails staging,
/// the batch cannot propose, and the already-staged keys hold intents with no Raft ticket. The coordinator
/// only rolls back participants that returned Prepared with a ticket, so these staged-but-unproposed intents
/// would otherwise leak and pin their entries until the lease expires. This handler unwinds one such intent.
///
/// <para>It performs no Raft round trip: nothing was proposed for these keys, so there is no ticket to roll
/// back — only local prepare state (write intent + the transaction's MVCC entry) to discard. It is idempotent
/// and safe on state that is already gone or owned by another transaction (returns RolledBack without
/// touching it), so a duplicate unwind cannot corrupt a concurrent owner.</para>
/// </summary>
internal sealed class ApplyRolledBackMutationsHandler : BaseHandler
{
    public ApplyRolledBackMutationsHandler(KeyValueContext context) : base(context)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot clear staged intent for missing transaction id");

            return KeyValueStaticResponses.ErroredResponse;
        }

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        // Nothing staged here, or the intent belongs to a different transaction: idempotent no-op. A batch
        // that partially staged before failing may ask to unwind a key it never actually reached, and a
        // concurrent transaction may already own the entry — neither is an error, and neither may be touched.
        if (entry?.WriteIntent is null || entry.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.RolledBack, 0);

        ApplyConfirmedRollback(entry, message.TransactionId, currentTime);

        return new(KeyValueResponseType.RolledBack, 0);
    }
}
