
using Google.Protobuf;
using Kahuna.Server.Replication.Protos;

namespace Kahuna.Server.Replication;

/// <summary>
/// Provides serialization and deserialization utilities for replication messages,
/// specifically for LockMessage, KeyValueMessage, and RangeMapMessage types.
/// </summary>
public static class ReplicationSerializer
{
    // Writes straight into the exactly-sized destination array, so encoding a message costs one allocation — the
    // payload the log entry keeps — and no intermediate stream.
    private static byte[] Encode(IMessage message)
    {
        byte[] buffer = new byte[message.CalculateSize()];
        message.WriteTo(buffer.AsSpan());
        return buffer;
    }

    public static byte[] Serialize(LockMessage message)
    {
        return Encode(message);
    }

    public static LockMessage UnserializeLockMessage(ReadOnlySpan<byte> serializedData) =>
        LockMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(KeyValueMessage message)
    {
        return Encode(message);
    }

    public static KeyValueMessage UnserializeKeyValueMessage(ReadOnlySpan<byte> serializedData) =>
        KeyValueMessage.Parser.ParseFrom(serializedData);

    /// <summary>
    /// Message shell reused by <see cref="UnserializeKeyValueMessageThreadCached"/> on this thread.
    /// </summary>
    [ThreadStatic]
    private static KeyValueMessage? threadCachedKeyValueMessage;

    /// <summary>
    /// Parses a <see cref="KeyValueMessage"/> into a thread-cached instance instead of a fresh
    /// allocation, for the per-log-entry apply hot path. The returned instance is only valid until the
    /// next call on the same thread: the caller must copy every field it needs out before then, must not
    /// retain the instance, and must not hand it to another thread. Byte payloads extracted from it are
    /// safe to keep — each parse creates fresh ByteStrings; only the shell is reused.
    /// </summary>
    public static KeyValueMessage UnserializeKeyValueMessageThreadCached(ReadOnlySpan<byte> serializedData)
    {
        KeyValueMessage message = threadCachedKeyValueMessage ??= new();

        ResetKeyValueMessage(message);
        message.MergeFrom(serializedData);

        return message;
    }

    /// <summary>
    /// Restores every field of a reused <see cref="KeyValueMessage"/> to its default before a merge.
    /// The reset must be complete: proto3 omits default-valued fields from the wire and
    /// <c>MergeFrom</c> leaves omitted fields untouched, so any field missed here would leak the
    /// previous record's value into the next one (a delete inheriting the prior set's value, a
    /// non-transactional write inheriting the prior transaction id). A new proto field must be added
    /// here too — the reset-completeness unit test fails when one is missed.
    /// </summary>
    private static void ResetKeyValueMessage(KeyValueMessage message)
    {
        message.Type = 0;
        message.Key = string.Empty;
        message.ClearValue();
        message.Revision = 0;
        message.ExpireNode = 0;
        message.ExpirePhysical = 0;
        message.ExpireCounter = 0;
        message.LastUsedNode = 0;
        message.LastUsedPhysical = 0;
        message.LastUsedCounter = 0;
        message.LastModifiedNode = 0;
        message.LastModifiedPhysical = 0;
        message.LastModifiedCounter = 0;
        message.TimeNode = 0;
        message.TimePhysical = 0;
        message.TimeCounter = 0;
        message.ClearNoRevision();
        message.TransactionIdNode = 0;
        message.TransactionIdPhysical = 0;
        message.TransactionIdCounter = 0;
        message.ClearRecordAnchorKey();
        message.ClearEmbeddedDecision();
    }

    public static byte[] Serialize(RangeMapMessage message)
    {
        return Encode(message);
    }

    public static RangeMapMessage UnserializeRangeMapMessage(ReadOnlySpan<byte> serializedData) =>
        RangeMapMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(SnapshotFloorMessage message)
    {
        return Encode(message);
    }

    public static SnapshotFloorMessage UnserializeSnapshotFloorMessage(ReadOnlySpan<byte> serializedData) =>
        SnapshotFloorMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(SnapshotFloorDeltaMessage message)
    {
        return Encode(message);
    }

    public static SnapshotFloorDeltaMessage UnserializeSnapshotFloorDeltaMessage(ReadOnlySpan<byte> serializedData) =>
        SnapshotFloorDeltaMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(MetaSystemStateMessage message)
    {
        return Encode(message);
    }

    public static MetaSystemStateMessage UnserializeMetaSystemStateMessage(ReadOnlySpan<byte> serializedData) =>
        MetaSystemStateMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(CoordinatorDecisionDeltaMessage message)
    {
        return Encode(message);
    }

    public static CoordinatorDecisionDeltaMessage UnserializeCoordinatorDecisionDeltaMessage(ReadOnlySpan<byte> serializedData) =>
        CoordinatorDecisionDeltaMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(CoordinatorDecisionSnapshotMessage message)
    {
        return Encode(message);
    }

    public static CoordinatorDecisionSnapshotMessage UnserializeCoordinatorDecisionSnapshotMessage(ReadOnlySpan<byte> serializedData) =>
        CoordinatorDecisionSnapshotMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(TransactionRecordDeltaMessage message)
    {
        return Encode(message);
    }

    public static TransactionRecordDeltaMessage UnserializeTransactionRecordDeltaMessage(ReadOnlySpan<byte> serializedData) =>
        TransactionRecordDeltaMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(PreparedIntentDeltaMessage message)
    {
        return Encode(message);
    }

    public static PreparedIntentDeltaMessage UnserializePreparedIntentDeltaMessage(ReadOnlySpan<byte> serializedData) =>
        PreparedIntentDeltaMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(TransactionRecordSnapshotMessage message)
    {
        return Encode(message);
    }

    public static TransactionRecordSnapshotMessage UnserializeTransactionRecordSnapshotMessage(ReadOnlySpan<byte> serializedData) =>
        TransactionRecordSnapshotMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(PreparedIntentSnapshotMessage message)
    {
        return Encode(message);
    }

    public static PreparedIntentSnapshotMessage UnserializePreparedIntentSnapshotMessage(ReadOnlySpan<byte> serializedData) =>
        PreparedIntentSnapshotMessage.Parser.ParseFrom(serializedData);
}
