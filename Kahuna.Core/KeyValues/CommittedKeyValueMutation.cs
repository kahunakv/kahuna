using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// One committed key/value mutation as the apply paths consume it, whatever carried it: a key/value log record
/// (by value, or by reference to a prepared intent) or a materializing resolve that installs the intent directly.
/// The replicator and the restorer apply this shape only, so the durable effects of a commit cannot differ by the
/// record that delivered it.
/// </summary>
internal readonly record struct CommittedKeyValueMutation(
    string Key,
    byte[]? Value,
    KeyValueState State,
    long Revision,
    bool NoRevision,
    HLCTimestamp Expires,
    HLCTimestamp LastUsed,
    HLCTimestamp LastModified,
    HLCTimestamp TransactionId,
    string? RecordAnchorKey)
{
    /// <summary>True when the mutation belongs to a transaction and therefore derives a completion receipt.</summary>
    public bool IsTransactional => TransactionId != HLCTimestamp.Zero;

    /// <summary>The mutation a key/value log record carries. <paramref name="value"/> and <paramref name="state"/>
    /// come from the record itself, or from the prepared intent a by-reference record names.</summary>
    public static CommittedKeyValueMutation FromRecord(KeyValueMessage message, byte[]? value, KeyValueState state) => new(
        message.Key,
        value,
        state,
        message.Revision,
        message.NoRevision,
        new(message.ExpireNode, message.ExpirePhysical, message.ExpireCounter),
        new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
        new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
        new(message.TransactionIdNode, message.TransactionIdPhysical, message.TransactionIdCounter),
        message.HasRecordAnchorKey ? message.RecordAnchorKey : null);

    /// <summary>The mutation a committed prepared intent stages. The transaction's one commit timestamp stamps
    /// last-modified and last-used, exactly as the materialization record built from the same intent does.</summary>
    public static CommittedKeyValueMutation FromIntent(PreparedIntent intent) => new(
        intent.Key,
        intent.Value,
        intent.State,
        intent.Revision,
        intent.NoRevision,
        intent.Expires,
        intent.CommitTimestamp,
        intent.CommitTimestamp,
        intent.TransactionId,
        intent.RecordAnchorKey);
}
