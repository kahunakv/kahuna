
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Commit-time conflict probe for optimistic transactions.
///
/// Checks whether a key carries a live write intent from a transaction other than the caller.
/// A positive result indicates that a concurrent transaction is preparing or has prepared a
/// write to this key, meaning a read-set entry for this key in the calling transaction is at
/// risk of write skew if both transactions commit.
///
/// This handler is intentionally separate from TryExistsHandler so that ordinary user reads
/// are never blocked by write intents (read-committed semantics), while the commit protocol
/// can still detect concurrent writers as a best-effort serialization guard.
///
/// Called only during TwoPhaseCommit for optimistic transactions, after ValidateReadSet passes.
/// Not used by pessimistic transactions (exclusive locks provide full serializability there).
/// </summary>
internal sealed class TryCheckWriteIntentHandler : BaseHandler
{
    public TryCheckWriteIntentHandler(KeyValueContext context) : base(context)
    {
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        if (entry?.WriteIntent is null)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (entry.WriteIntent.TransactionId == message.TransactionId)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        if (entry.WriteIntent.Expires - currentTime <= TimeSpan.Zero)
        {
            entry.WriteIntent = null;
            return KeyValueStaticResponses.DoesNotExistContextResponse;
        }

        // Live write intent from a different transaction — signal conflict to the caller.
        return KeyValueStaticResponses.AbortedResponse;
    }
}
