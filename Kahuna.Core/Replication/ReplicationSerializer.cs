
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
