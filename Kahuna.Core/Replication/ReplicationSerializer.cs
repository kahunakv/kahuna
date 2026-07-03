
using Google.Protobuf;
using Kahuna.Server.Replication.Protos;

namespace Kahuna.Server.Replication;

/// <summary>
/// Provides serialization and deserialization utilities for replication messages,
/// specifically for LockMessage, KeyValueMessage, and RangeMapMessage types.
/// </summary>
public static class ReplicationSerializer
{
    public static byte[] Serialize(LockMessage message)
    {
        int size = message.CalculateSize();
        byte[] buf = new byte[size];
        using CodedOutputStream cos = new(buf);
        message.WriteTo(cos);
        return buf;
    }

    public static LockMessage UnserializeLockMessage(ReadOnlySpan<byte> serializedData) =>
        LockMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(KeyValueMessage message)
    {
        int size = message.CalculateSize();
        byte[] buf = new byte[size];
        using CodedOutputStream cos = new(buf);
        message.WriteTo(cos);
        return buf;
    }

    public static KeyValueMessage UnserializeKeyValueMessage(ReadOnlySpan<byte> serializedData) =>
        KeyValueMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(RangeMapMessage message)
    {
        int size = message.CalculateSize();
        byte[] buf = new byte[size];
        using CodedOutputStream cos = new(buf);
        message.WriteTo(cos);
        return buf;
    }

    public static RangeMapMessage UnserializeRangeMapMessage(ReadOnlySpan<byte> serializedData) =>
        RangeMapMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(SnapshotFloorMessage message)
    {
        int size = message.CalculateSize();
        byte[] buf = new byte[size];
        using CodedOutputStream cos = new(buf);
        message.WriteTo(cos);
        return buf;
    }

    public static SnapshotFloorMessage UnserializeSnapshotFloorMessage(ReadOnlySpan<byte> serializedData) =>
        SnapshotFloorMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(SnapshotFloorDeltaMessage message)
    {
        int size = message.CalculateSize();
        byte[] buf = new byte[size];
        using CodedOutputStream cos = new(buf);
        message.WriteTo(cos);
        return buf;
    }

    public static SnapshotFloorDeltaMessage UnserializeSnapshotFloorDeltaMessage(ReadOnlySpan<byte> serializedData) =>
        SnapshotFloorDeltaMessage.Parser.ParseFrom(serializedData);

    public static byte[] Serialize(MetaSystemStateMessage message)
    {
        int size = message.CalculateSize();
        byte[] buf = new byte[size];
        using CodedOutputStream cos = new(buf);
        message.WriteTo(cos);
        return buf;
    }

    public static MetaSystemStateMessage UnserializeMetaSystemStateMessage(ReadOnlySpan<byte> serializedData) =>
        MetaSystemStateMessage.Parser.ParseFrom(serializedData);
}
