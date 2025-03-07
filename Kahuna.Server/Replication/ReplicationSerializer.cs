
using Google.Protobuf;
using Kahuna.Replication.Protos;

namespace Kahuna.Replication;

public static class ReplicationSerializer
{
    public static byte[] Serialize(LockMessage message)
    {
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    public static LockMessage UnserializeLockMessage(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return LockMessage.Parser.ParseFrom(memoryStream);
    }
    
    public static byte[] Serialize(KeyValueMessage message)
    {
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }
    
    public static KeyValueMessage UnserializeKeyValueMessage(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return KeyValueMessage.Parser.ParseFrom(memoryStream);
    }
}