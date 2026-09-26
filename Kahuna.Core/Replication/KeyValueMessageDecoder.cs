
using Kahuna.Server.KeyValues;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Replication;

/// <summary>
/// Single authoritative reading of a logged <see cref="KeyValueMessage"/>: its type (<see cref="RecordType"/>), how
/// consumers apply it (<see cref="Classify"/>), and the value-carrying decode (<see cref="Decode"/>). The replicator,
/// <c>KeyValueRestorer</c> (Raft log replay) and <c>RestoreEngine</c> (PITR WAL replay) all delegate here, and
/// producers take the number they log from <see cref="ToLoggedType"/>, so logging a new
/// <see cref="KeyValueRequestType"/> is one edit in one place. <c>TestLoggedRecordTypeCoverage</c> fails when a
/// request type has no kind, and drives each logged type through the restore paths.
/// </summary>
internal static class KeyValueMessageDecoder
{
    /// <summary>
    /// The number one published build wrote for <see cref="KeyValueRequestType.MaterializeIntent"/>. That build
    /// inserted <see cref="KeyValueRequestType.DropLeaderState"/> ahead of it in the enum, which shifted it from
    /// 29 to 30; the values were then pinned with <c>MaterializeIntent</c> back at 29. Records that build wrote
    /// still sit in Raft logs, WAL segments and backups with type 30.
    /// </summary>
    private const int LegacyMaterializeIntentType = 30;

    /// <summary>
    /// Reads the operation a persisted or replicated <see cref="KeyValueMessage"/> names. Every consumer of a
    /// logged record must read its type through here rather than casting <c>Type</c> directly.
    ///
    /// <para>A logged type 30 is a by-reference commit from the build that shifted
    /// <see cref="KeyValueRequestType.MaterializeIntent"/> to 30. Reading it as
    /// <see cref="KeyValueRequestType.DropLeaderState"/> skips the commit, which silently loses the write on the
    /// node that replays it. The mapping is unambiguous because <c>DropLeaderState</c> is an actor-internal
    /// message that is never serialized into a log record, so no build ever logged 30 with any other
    /// meaning.</para>
    /// </summary>
    internal static KeyValueRequestType RecordType(KeyValueMessage msg)
    {
        int type = msg.Type;
        return type == LegacyMaterializeIntentType ? KeyValueRequestType.MaterializeIntent : (KeyValueRequestType)type;
    }

    /// <summary>
    /// Classifies a request type by how the consumers of the key-value log apply it. Every member of
    /// <see cref="KeyValueRequestType"/> is listed by name on purpose: a new member falls to
    /// <see cref="KeyValueRecordKind.Unknown"/> until someone decides whether it is logged, and a test fails until
    /// they do. A member classified as a mutation is applied by every consumer with no further edit.
    /// </summary>
    internal static KeyValueRecordKind Classify(KeyValueRequestType type) => type switch
    {
        KeyValueRequestType.TrySet
            or KeyValueRequestType.TryExtend
            or KeyValueRequestType.TryDelete => KeyValueRecordKind.ValueMutation,

        KeyValueRequestType.MaterializeIntent => KeyValueRecordKind.ByReferenceMutation,

        KeyValueRequestType.TryGet
            or KeyValueRequestType.TryExists
            or KeyValueRequestType.TryAcquireExclusiveLock
            or KeyValueRequestType.TryAcquireExclusivePrefixLock
            or KeyValueRequestType.TryAcquireExclusiveRangeLock
            or KeyValueRequestType.TryReleaseExclusiveLock
            or KeyValueRequestType.TryReleaseExclusivePrefixLock
            or KeyValueRequestType.TryReleaseExclusiveRangeLock
            or KeyValueRequestType.TryPrepareMutations
            or KeyValueRequestType.TryCommitMutations
            or KeyValueRequestType.TryRollbackMutations
            or KeyValueRequestType.ScanByPrefix
            or KeyValueRequestType.ScanByPrefixFromDisk
            or KeyValueRequestType.GetByBucket
            or KeyValueRequestType.GetByRange
            or KeyValueRequestType.CompleteProposal
            or KeyValueRequestType.ReleaseProposal
            or KeyValueRequestType.Collect
            or KeyValueRequestType.TryCheckWriteIntent
            or KeyValueRequestType.GetRangeLocks
            or KeyValueRequestType.ImportRangeLocks
            or KeyValueRequestType.GetSafeTimestamp
            or KeyValueRequestType.ResumeRead
            or KeyValueRequestType.InvalidateOrApply
            or KeyValueRequestType.FlushAck
            or KeyValueRequestType.EvictPartition
            or KeyValueRequestType.DropLeaderState
            or KeyValueRequestType.TryFinalizeMutation
            or KeyValueRequestType.RunActorTurn => KeyValueRecordKind.NotLogged,

        _ => KeyValueRecordKind.Unknown
    };

    /// <summary>
    /// Returns the number a producer writes as the <c>Type</c> of a key-value log record, and refuses a type that
    /// no consumer applies. Every producer of a <see cref="ReplicationTypes.KeyValues"/> record must take its type
    /// from here. A record of a type outside the mutation kinds would replicate and replay as an unknown message
    /// and be skipped on every node, so failing the write loudly is the only safe outcome. To log a new request
    /// type, classify it as a mutation in <see cref="Classify"/>.
    /// </summary>
    internal static int ToLoggedType(KeyValueRequestType type)
    {
        KeyValueRecordKind kind = Classify(type);

        if (kind is KeyValueRecordKind.ValueMutation or KeyValueRecordKind.ByReferenceMutation)
            return (int)type;

        throw new InvalidOperationException(
            $"{type} ({(int)type}) is classified as {kind} and no consumer of the key-value log applies it; " +
            "classify it as a mutation in KeyValueMessageDecoder.Classify before logging it.");
    }

    /// <summary>
    /// Decodes a <see cref="KeyValueRecordKind.ValueMutation"/> record into the state it writes and its value
    /// bytes. Returns <c>KeyValueState.Undefined</c> for every other kind. Every type that <see cref="Classify"/>
    /// calls a value mutation must map to a defined state here; the coverage test fails otherwise.
    ///
    /// <para><see cref="KeyValueRequestType.MaterializeIntent"/> is deliberately NOT decoded here: it
    /// carries no value, so its mutation cannot be read from the record at all. Every consumer resolves it
    /// against a prepared intent instead.</para>
    /// </summary>
    internal static (KeyValueState state, byte[]? value) Decode(KeyValueMessage msg)
    {
        KeyValueState state = RecordType(msg) switch
        {
            KeyValueRequestType.TrySet    => KeyValueState.Set,
            KeyValueRequestType.TryExtend => KeyValueState.Set,
            KeyValueRequestType.TryDelete => KeyValueState.Deleted,
            _                             => KeyValueState.Undefined
        };

        if (state == KeyValueState.Undefined)
            return (KeyValueState.Undefined, null);

        // The proposal encoder clears the value field for a value-less write, so presence is the only
        // thing separating "set to no value" from "set to zero bytes". Reading the field alone would give
        // a follower an empty array where the leader holds null, and that divergence outlives the apply:
        // it becomes what the next read of this key returns once the follower is elected.
        byte[]? value = ByteStringPayload.GetArrayOrNull(msg.HasValue, msg.Value);

        return (state, value);
    }
}
