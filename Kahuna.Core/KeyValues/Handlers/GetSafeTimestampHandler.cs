
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Returns the minimum prepared <c>CommitTimestamp</c> across all live write intents in this
/// shard, or <c>HLCTimestamp.Zero</c> when the shard has no in-flight prepared transactions.
///
/// <para>Used by the coordinated-snapshot coordinator.  Only the manual (ephemeral) prepare stamps
/// the actor intent's commit timestamp; a durable transaction's pending mutation lives in the
/// prepared-intent store, which the node-level query reads alongside this scan.  See
/// <see cref="SnapshotCoordinator"/>.</para>
/// </summary>
internal sealed class GetSafeTimestampHandler : BaseHandler
{
    public GetSafeTimestampHandler(KeyValueContext context) : base(context) { }

    public ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        HLCTimestamp min = FindMinInFlightCommitTimestamp(context.Store, context.LocksByPrefix);
        return ValueTask.FromResult(new KeyValueResponse(KeyValueResponseType.SafeTimestamp, min));
    }

    /// <summary>
    /// Scans <paramref name="store"/> and <paramref name="locksByPrefix"/> for the minimum
    /// <see cref="KeyValueWriteIntent.CommitTimestamp"/> across all live prepared intents.
    /// Returns <see cref="HLCTimestamp.Zero"/> when no prepared intents exist.
    /// </summary>
    internal static HLCTimestamp FindMinInFlightCommitTimestamp(
        BTree<string, KeyValueEntry> store,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix)
    {
        HLCTimestamp min = HLCTimestamp.Zero;

        foreach (KeyValuePair<string, KeyValueEntry> kv in store.GetItems())
        {
            KeyValueWriteIntent? intent = kv.Value.WriteIntent;
            if (intent is null || intent.CommitTimestamp == HLCTimestamp.Zero)
                continue;
            if (min == HLCTimestamp.Zero || intent.CommitTimestamp.CompareTo(min) < 0)
                min = intent.CommitTimestamp;
        }

        foreach (KeyValueWriteIntent intent in locksByPrefix.Values)
        {
            if (intent.CommitTimestamp == HLCTimestamp.Zero)
                continue;
            if (min == HLCTimestamp.Zero || intent.CommitTimestamp.CompareTo(min) < 0)
                min = intent.CommitTimestamp;
        }

        return min;
    }
}
