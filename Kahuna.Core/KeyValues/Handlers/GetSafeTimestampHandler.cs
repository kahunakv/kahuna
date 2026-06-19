
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Returns the minimum prepared <c>CommitTimestamp</c> across all live write intents in this
/// shard, or <c>HLCTimestamp.Zero</c> when the shard has no in-flight prepared transactions.
///
/// <para>Used by the coordinated-snapshot coordinator.  Any T strictly below the returned
/// minimum avoids cutting a <em>currently prepared</em> transaction on this shard.  This does
/// not protect against already-committed cross-shard transactions whose per-shard WAL
/// <c>Time</c> values straddle T — those have no live <c>WriteIntent</c> and are invisible to
/// this scan.  See <see cref="SnapshotCoordinator"/> for the full limitation.</para>
/// </summary>
internal sealed class GetSafeTimestampHandler : BaseHandler
{
    public GetSafeTimestampHandler(KeyValueContext context) : base(context) { }

    public Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        HLCTimestamp min = FindMinInFlightCommitTimestamp(context.Store, context.LocksByPrefix);
        return Task.FromResult(new KeyValueResponse(KeyValueResponseType.SafeTimestamp, min));
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
