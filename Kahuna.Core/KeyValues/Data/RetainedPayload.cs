
using Kahuna.Server.Configuration;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Places a value payload that is about to become a resident committed head where the garbage collector will
/// not copy it again.
///
/// <para>A committed value stays referenced for as long as its key is resident: as the head, then as an archived
/// revision, and by the persistence row until the writer flushes it. Allocated in gen0 like every request
/// payload, it survives the first collection and is copied into gen1, then again into gen2; at a few tens of
/// thousands of writes per second with kilobyte values that copying is the bulk of every gen0 and gen1 pause.
/// An array on the pinned object heap is collected only with gen2 and never compacted, so it is written once
/// and never moved. The wire array the request arrived in dies young. The <c>byte[]</c> contract is unchanged:
/// published heads, revision entries, persistence rows and zero-copy responses share the retained array exactly
/// as they shared the wire array.</para>
/// </summary>
internal static class RetainedPayload
{
    /// <summary>
    /// The array to install as the committed head for <paramref name="value"/>: a pinned copy when
    /// <see cref="KahunaConfiguration.PinnedValuePayloads"/> is on and the payload is at least
    /// <see cref="KahunaConfiguration.PinnedValuePayloadMinBytes"/> long, otherwise the array itself. Small
    /// payloads stay on the normal heap because the pinned allocator and the copy cost more than their promotion.
    /// </summary>
    public static byte[]? Retain(byte[]? value, KahunaConfiguration configuration)
    {
        if (value is null || !configuration.PinnedValuePayloads || value.Length < configuration.PinnedValuePayloadMinBytes)
            return value;

        byte[] pinned = GC.AllocateUninitializedArray<byte>(value.Length, pinned: true);
        value.AsSpan().CopyTo(pinned);
        return pinned;
    }
}
