
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

    /// <summary>
    /// Decodes a presence-tracked (proto3 <c>optional</c>) bytes field, preserving the difference the
    /// store keeps between a key that holds no value and a key that holds zero bytes: an absent field
    /// decodes to null, a present one to its bytes, empty included.
    ///
    /// <para>Every encoder in this repository writes that difference — it leaves the field unset for a
    /// null payload rather than writing an empty one — so a decoder that reads the field alone silently
    /// promotes "no value" to "empty value". The generated getter cannot report the difference (it
    /// substitutes <see cref="ByteString.Empty"/> for an unset field), which is why the caller must pass
    /// the message's generated <c>Has…</c> flag and never a test on the decoded array.</para>
    /// </summary>
    public static byte[]? GetArrayOrNull(bool present, ByteString value)
    {
        return present ? GetArray(value) : null;
    }
}
