
using Google.Protobuf;
using Kahuna.Server.Replication.Protos;
using Microsoft.IO;

namespace Kahuna.Server.Replication;

public static class ReplicationSerializer
{
    private const int MaxMessageSize = 1024;
    
    private static readonly RecyclableMemoryStreamManager manager = new();
    
    public static byte[] Serialize(LockMessage message)
    {
        if (!message.Owner.IsEmpty && message.Owner.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recycledMemoryStream = manager.GetStream();
            message.WriteTo((Stream)recycledMemoryStream);
            return recycledMemoryStream.ToArray();
        }
        
        using MemoryStream memoryStream = manager.GetStream();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    public static LockMessage UnserializeLockMessage(ReadOnlySpan<byte> serializedData)
    {
        if (serializedData.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recycledMemoryStream = manager.GetStream(serializedData);
            return LockMessage.Parser.ParseFrom(recycledMemoryStream);
        }
        
        using MemoryStream memoryStream = manager.GetStream(serializedData);
        return LockMessage.Parser.ParseFrom(memoryStream);
    }
    
    public static byte[] Serialize(KeyValueMessage message)
    {
        if (!message.Value.IsEmpty && message.Value.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recycledMemoryStream = manager.GetStream();
            message.WriteTo((Stream)recycledMemoryStream);
            return recycledMemoryStream.ToArray();    
        }
        
        using MemoryStream memoryStream = manager.GetStream();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }
    
    public static KeyValueMessage UnserializeKeyValueMessage(ReadOnlySpan<byte> serializedData)
    {
        if (serializedData.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recycledMemoryStream = manager.GetStream(serializedData);
            return KeyValueMessage.Parser.ParseFrom(recycledMemoryStream);
        }
        
        using MemoryStream memoryStream = manager.GetStream(serializedData);
        return KeyValueMessage.Parser.ParseFrom(memoryStream);
    }
}