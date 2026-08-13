using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Turns a committed prepared intent into the ordinary key/value log record that makes its value the visible KV
/// revision. On commit resolution the durable intent's mutation is replayed as a normal
/// <see cref="ReplicationTypes.KeyValues"/> record so the existing replicator/restorer applies it to MVCC and
/// persistence exactly as a direct write would — no separate apply path. Every field is stamped with the
/// transaction's one canonical <see cref="PreparedIntent.CommitTimestamp"/> (invariant: one commit timestamp per
/// transaction), never a per-key staging time.
/// </summary>
internal static class PreparedIntentMaterializer
{
    /// <summary>
    /// Encodes the intent into <paramref name="scratch"/> and returns the serialized record. The scratch message
    /// exists so a materialization loop allocates one message for the whole loop instead of one per intent; every
    /// field the record carries is overwritten on every call, so nothing from a previous intent can leak into the
    /// next record. The returned array is freshly allocated each call and safe to retain (the replication log keeps
    /// it); only the scratch message is reused, and it is fully consumed before this method returns.
    /// </summary>
    public static byte[] ToKeyValueRecord(PreparedIntent intent, KeyValueMessage scratch)
    {
        KeyValueRequestType type = intent.State == KeyValueState.Deleted
            ? KeyValueRequestType.TryDelete
            : KeyValueRequestType.TrySet;

        scratch.Type = (int)type;
        scratch.Key = intent.Key;
        scratch.Revision = intent.Revision;
        scratch.NoRevision = intent.NoRevision;
        scratch.ExpireNode = intent.Expires.N; scratch.ExpirePhysical = intent.Expires.L; scratch.ExpireCounter = intent.Expires.C;
        // One canonical commit timestamp stamps last-modified/last-used/time.
        scratch.LastModifiedNode = intent.CommitTimestamp.N; scratch.LastModifiedPhysical = intent.CommitTimestamp.L; scratch.LastModifiedCounter = intent.CommitTimestamp.C;
        scratch.LastUsedNode = intent.CommitTimestamp.N; scratch.LastUsedPhysical = intent.CommitTimestamp.L; scratch.LastUsedCounter = intent.CommitTimestamp.C;
        scratch.TimeNode = intent.CommitTimestamp.N; scratch.TimePhysical = intent.CommitTimestamp.L; scratch.TimeCounter = intent.CommitTimestamp.C;
        scratch.TransactionIdNode = intent.TransactionId.N;
        scratch.TransactionIdPhysical = intent.TransactionId.L;
        scratch.TransactionIdCounter = intent.TransactionId.C;
        scratch.RecordAnchorKey = intent.RecordAnchorKey;
        scratch.Value = intent.Value is null ? ByteString.Empty : UnsafeByteOperations.UnsafeWrap(intent.Value);

        return ReplicationSerializer.Serialize(scratch);
    }
}
