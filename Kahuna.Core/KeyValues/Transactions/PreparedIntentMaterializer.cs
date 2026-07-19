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
    public static byte[] ToKeyValueRecord(PreparedIntent intent)
    {
        KeyValueRequestType type = intent.State == KeyValueState.Deleted
            ? KeyValueRequestType.TryDelete
            : KeyValueRequestType.TrySet;

        KeyValueMessage message = new()
        {
            Type = (int)type,
            Key = intent.Key,
            Revision = intent.Revision,
            NoRevision = intent.NoRevision,
            ExpireNode = intent.Expires.N, ExpirePhysical = intent.Expires.L, ExpireCounter = intent.Expires.C,
            // One canonical commit timestamp stamps last-modified/last-used/time.
            LastModifiedNode = intent.CommitTimestamp.N, LastModifiedPhysical = intent.CommitTimestamp.L, LastModifiedCounter = intent.CommitTimestamp.C,
            LastUsedNode = intent.CommitTimestamp.N, LastUsedPhysical = intent.CommitTimestamp.L, LastUsedCounter = intent.CommitTimestamp.C,
            TimeNode = intent.CommitTimestamp.N, TimePhysical = intent.CommitTimestamp.L, TimeCounter = intent.CommitTimestamp.C
        };

        if (intent.Value is not null)
            message.Value = UnsafeByteOperations.UnsafeWrap(intent.Value);

        return ReplicationSerializer.Serialize(message);
    }
}
