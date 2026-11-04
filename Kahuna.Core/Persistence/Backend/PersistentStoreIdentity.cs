
using System.Buffers.Binary;
using System.Text;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Who a persistent store belongs to, written into the store itself the first time a node opens it.
/// A store is private to one node's Raft log: its durability floors certify that log's entries, so a
/// store copied from another node (floors that vouch for a different log) or a store that was wiped
/// (no floors, no identity, while the log holds history) must not be opened as if it were this node's.
/// The identity binds the store to the node's Raft endpoint, which is also how the cluster roster
/// names the member; a creation id and time tell two stores of the same endpoint apart in a forensic
/// comparison.
/// </summary>
/// <param name="NodeEndpoint">The Raft endpoint (<c>host:port</c>) of the node that created the store.</param>
/// <param name="CreationId">A random id minted when the store was created.</param>
/// <param name="CreatedAtUnixMs">Wall-clock creation time, informational only (never used for ordering).</param>
public sealed record PersistentStoreIdentity(string NodeEndpoint, Guid CreationId, long CreatedAtUnixMs)
{
    // Layout, little-endian: magic (4) | version u16 (2) | creation id (16) | created unix ms i64 (8) |
    // endpoint length u16 (2) | endpoint UTF-8. The magic and the version make a damaged or foreign row
    // decode as damage instead of as an identity with a garbage endpoint.
    private static ReadOnlySpan<byte> Magic => "KSID"u8;

    private const ushort Version = 1;

    private const int HeaderSize = 4 + 2 + 16 + 8 + 2;

    private const int MaxEndpointBytes = ushort.MaxValue;

    /// <summary>A new identity for <paramref name="nodeEndpoint"/>, minted now.</summary>
    public static PersistentStoreIdentity Create(string nodeEndpoint) =>
        new(nodeEndpoint, Guid.NewGuid(), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

    public byte[] Encode()
    {
        int endpointBytes = Encoding.UTF8.GetByteCount(NodeEndpoint);
        if (endpointBytes > MaxEndpointBytes)
            throw new ArgumentException($"The node endpoint '{NodeEndpoint}' is longer than {MaxEndpointBytes} UTF-8 bytes.");

        byte[] buffer = new byte[HeaderSize + endpointBytes];
        Span<byte> span = buffer;

        Magic.CopyTo(span);
        BinaryPrimitives.WriteUInt16LittleEndian(span[4..], Version);
        CreationId.TryWriteBytes(span.Slice(6, 16));
        BinaryPrimitives.WriteInt64LittleEndian(span[22..], CreatedAtUnixMs);
        BinaryPrimitives.WriteUInt16LittleEndian(span[30..], (ushort)endpointBytes);
        Encoding.UTF8.GetBytes(NodeEndpoint, span[HeaderSize..]);

        return buffer;
    }

    /// <summary>
    /// Decodes a stored identity. False for anything that is not exactly an identity row of a known
    /// version: the caller treats that as damage to the store's bookkeeping, not as an absent identity.
    /// </summary>
    public static bool TryDecode(ReadOnlySpan<byte> data, out PersistentStoreIdentity? identity)
    {
        identity = null;

        if (data.Length < HeaderSize || !data[..4].SequenceEqual(Magic))
            return false;

        if (BinaryPrimitives.ReadUInt16LittleEndian(data[4..]) != Version)
            return false;

        Guid creationId = new(data.Slice(6, 16));
        long createdAt = BinaryPrimitives.ReadInt64LittleEndian(data[22..]);
        int endpointBytes = BinaryPrimitives.ReadUInt16LittleEndian(data[30..]);

        if (endpointBytes == 0 || data.Length != HeaderSize + endpointBytes)
            return false;

        string endpoint;
        try
        {
            endpoint = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true)
                .GetString(data.Slice(HeaderSize, endpointBytes));
        }
        catch (DecoderFallbackException)
        {
            return false;
        }

        identity = new PersistentStoreIdentity(endpoint, creationId, createdAt);
        return true;
    }

    public override string ToString() =>
        $"node {NodeEndpoint}, creation {CreationId:N}, created {DateTimeOffset.FromUnixTimeMilliseconds(CreatedAtUnixMs):O}";
}
