
using System.Runtime.InteropServices;
using Google.Protobuf;

namespace Kahuna.Shared.Communication.Grpc;

public static class ByteStringPayload
{
    /// <summary>
    /// Returns the ByteString's bytes as an array without copying when the backing array is fully
    /// owned by it (the normal case for a freshly parsed message); falls back to a copy for
    /// sliced or rope-backed values so callers never observe bytes outside the payload.
    /// </summary>
    public static byte[] GetArray(ByteString value)
    {
        if (MemoryMarshal.TryGetArray(value.Memory, out ArraySegment<byte> segment)
            && segment.Array is not null
            && segment.Offset == 0
            && segment.Count == segment.Array.Length)
            return segment.Array;

        return value.ToByteArray();
    }
}
