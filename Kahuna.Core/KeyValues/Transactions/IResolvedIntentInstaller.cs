using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Installs the committed value of a prepared intent as the key's visible revision, at the apply of the
/// materializing resolve that settles it (<see cref="ResolveIntentCommand.MaterializeOnResolve"/>). The prepared-
/// intent store calls it on the partition's ordered apply path, once per installed intent and before the resolve
/// and the remove of that intent apply, so the row is queued and the unflushed overlay holds it by the time the
/// settle takes the intent out of the live set.
/// </summary>
internal interface IResolvedIntentInstaller
{
    /// <summary>Installs <paramref name="intent"/>'s committed value for the entry at <paramref name="logIndex"/>.
    /// <paramref name="replay"/> is true for a restart replay of the write-ahead log, which rebuilds durable state
    /// only and never routes to the owning actors.</summary>
    void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay);

    /// <summary>Called once after the last install of the entry at <paramref name="logIndex"/>, so the artifacts
    /// the entry derives as a whole (the completion receipts of every installed key) are certified together.</summary>
    void CompleteEntry(int partitionId, long logIndex, bool replay);

    /// <summary>A restart replay reached the materializing resolve of the given transaction attempt at
    /// <paramref name="key"/> (entry <paramref name="logIndex"/>) with no intent to install from: not live, not
    /// kept as replay history, not retained after its settle. The installer decides whether the value is already
    /// durable here (the row's last-modified is the commit timestamp, and a key's commits are HLC-ordered) or the
    /// restart left it missing.</summary>
    void NoteUnresolvedOnReplay(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp);
}
