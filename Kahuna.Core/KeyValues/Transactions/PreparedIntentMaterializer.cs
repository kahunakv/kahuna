using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Turns a committed prepared intent into the key/value log record that makes its value the visible KV
/// revision. Two forms of the same record exist:
///
/// <para><b>By value</b> (<see cref="KeyValueRequestType.TrySet"/> / <see cref="KeyValueRequestType.TryDelete"/>):
/// the mutation is replayed as a normal record, so the existing replicator/restorer applies it to MVCC and
/// persistence exactly as a direct write would. This form copies the committed value into the log a second
/// time — the prepare delta already carried it.</para>
///
/// <para><b>By reference</b> (<see cref="KeyValueRequestType.MaterializeIntent"/>): the record names the intent
/// — <c>(TransactionId, Epoch, Key)</c> — and carries no value. Every replica already holds the value in its own
/// prepared-intent store from the moment the prepare delta applied, so it resolves the value locally. The record
/// still carries the revision, the state and the one commit timestamp, so the point-in-time-recovery as-of cut
/// and the same-revision collision witness keep working from the record alone.</para>
///
/// <para>Every field is stamped with the transaction's one canonical <see cref="PreparedIntent.CommitTimestamp"/>
/// (invariant: one commit timestamp per transaction), never a per-key staging time.</para>
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
    /// <param name="byReference">
    /// True writes the value-free <see cref="KeyValueRequestType.MaterializeIntent"/> form. Only enable it once
    /// every node in the cluster runs a build that applies that record: an older node treats it as an unknown
    /// message type and skips it, which is a silently lost write on that node.
    /// </param>
    public static byte[] ToKeyValueRecord(PreparedIntent intent, KeyValueMessage scratch, bool byReference = false)
    {
        KeyValueRequestType type;

        if (byReference)
            type = KeyValueRequestType.MaterializeIntent;
        else if (intent.State == KeyValueState.Deleted)
            type = KeyValueRequestType.TryDelete;
        else
            type = KeyValueRequestType.TrySet;

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

        // The epoch completes the intent identity a consumer resolves the value with. It is written on both
        // forms so the scratch message never carries a previous intent's epoch into the next record.
        scratch.Epoch = byReference ? intent.Epoch : 0;

        // The by-reference form deliberately carries no value: the consumer reads it from its own intent. The
        // state does travel, because a delete's record must stay distinguishable from a set's. On the by-value
        // form the value field is presence-tracked, exactly as on a direct write's record: a null value clears
        // the field (the reused scratch would otherwise leak the previous intent's presence bit), so a committed
        // set with no value replays as a valueless key rather than an empty one.
        if (byReference || intent.Value is null)
            scratch.ClearValue();
        else
            scratch.Value = UnsafeByteOperations.UnsafeWrap(intent.Value);

        return ReplicationSerializer.Serialize(scratch);
    }
}
