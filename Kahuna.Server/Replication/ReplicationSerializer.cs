
using Google.Protobuf;

using Kahuna.Server.Protos;

namespace Kahuna.Replication;

public static class ReplicationSerializer
{
    public static byte[] Serialize(LockMessage message)
    {
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    public static LockMessage Unserializer(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return LockMessage.Parser.ParseFrom(memoryStream);
    }
}