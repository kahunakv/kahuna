
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
/// Two sources of a concurrent writer are checked: the in-memory write intent placed while a peer stages its
/// writes, and — for the durable-intent path — a durable prepared intent. The durable intent is replicated, so
/// unlike the in-memory write intent (which a new Raft leader cannot reconstruct) it still detects a concurrent
/// writer after a leader change, closing the write-skew window across failover. Only an undecided durable intent
/// is a live concurrent writer here; committed/aborted outcomes are settled by the revision-based read validation.
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

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // Live in-memory write intent from a different transaction — signal conflict to the caller.
        if (entry?.WriteIntent is not null && entry.WriteIntent.TransactionId != message.TransactionId)
        {
            if (KeyValueWriteIntentLease.IsLive(entry.WriteIntent, currentTime))
                return KeyValueStaticResponses.AbortedResponse;

            entry.WriteIntent = null;
        }

        // Durable prepared intent from a concurrent transaction that is still undecided: a live writer that
        // survives leader change (the in-memory intent above does not). A pending intent whose canonical decision
        // is already commit/abort is not in flight — commit-staleness is caught by revision-based read validation
        // and an abort is no conflict — so it is not flagged here. No-op off the durable-intent path.
        if (context.PreparedIntentStore?.Get(message.Key) is { } foreignIntent
            && foreignIntent.TransactionId != message.TransactionId
            && DurableReadVisibility.IsUndecidedWriter(context, foreignIntent, message.ForeignDecisionHint))
            return KeyValueStaticResponses.AbortedResponse;

        return KeyValueStaticResponses.DoesNotExistContextResponse;
    }
}
