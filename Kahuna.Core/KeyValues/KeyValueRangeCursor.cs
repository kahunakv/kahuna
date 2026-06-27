
using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Text;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Opaque cursor codec for paginated GetByRange scans.
///
/// Wire layout (little-endian):
///   [0]       version byte  = 0x01
///   [1..4]    lastKey UTF-8 byte count (int32)
///   [5..N]    lastKey UTF-8 bytes
///   [N+1..4]  prefix UTF-8 byte count (int32)
///   [..]      prefix UTF-8 bytes
///   [1]       durability (byte)
///   [4]       readTimestamp.N (int32)
///   [8]       readTimestamp.L (int64)
///   [4]       readTimestamp.C (uint32)
///
/// The entire buffer is Base64Url-encoded (no padding) to produce an opaque string.
/// </summary>
internal static class KeyValueRangeCursor
{
    private const byte Version = 0x01;
    private const int FixedOverhead = 26; // 1 + 4 + 4 + 1 + 4 + 8 + 4
    private const int StackThreshold = 256; // bytes

    /// <summary>
    /// Encodes a cursor that captures the last returned key, the scan prefix, the
    /// requested durability tier, and the snapshot read timestamp.
    /// </summary>
    public static string Encode(
        string lastKey,
        KeyValueDurability durability,
        string prefix,
        HLCTimestamp readTimestamp)
    {
        int lastKeyByteCount = Encoding.UTF8.GetByteCount(lastKey);
        int prefixByteCount  = Encoding.UTF8.GetByteCount(prefix);
        int totalBytes = FixedOverhead + lastKeyByteCount + prefixByteCount;

        byte[]? rented = null;
        Span<byte> buf = totalBytes <= StackThreshold
            ? stackalloc byte[StackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(totalBytes));
        buf = buf[..totalBytes];

        try
        {
            int pos = 0;

            buf[pos++] = Version;

            BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], lastKeyByteCount);
            pos += 4;
            Encoding.UTF8.GetBytes(lastKey.AsSpan(), buf[pos..]);
            pos += lastKeyByteCount;

            BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], prefixByteCount);
            pos += 4;
            Encoding.UTF8.GetBytes(prefix.AsSpan(), buf[pos..]);
            pos += prefixByteCount;

            buf[pos++] = (byte)durability;

            BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], readTimestamp.N);
            pos += 4;
            BinaryPrimitives.WriteInt64LittleEndian(buf[pos..], readTimestamp.L);
            pos += 8;
            BinaryPrimitives.WriteUInt32LittleEndian(buf[pos..], readTimestamp.C);

            return Base64Url.EncodeToString(buf);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Decodes a cursor produced by <see cref="Encode"/>.
    /// Returns false if the cursor is malformed or uses an unsupported version.
    /// </summary>
    public static bool TryDecode(
        string cursor,
        out string lastKey,
        out KeyValueDurability durability,
        out string prefix,
        out HLCTimestamp readTimestamp)
    {
        lastKey       = string.Empty;
        durability    = KeyValueDurability.Persistent;
        prefix        = string.Empty;
        readTimestamp = HLCTimestamp.Zero;

        if (string.IsNullOrEmpty(cursor))
            return false;

        int maxDecodedBytes = Base64Url.GetMaxDecodedLength(cursor.Length);

        byte[]? rented = null;
        Span<byte> buf = maxDecodedBytes <= StackThreshold
            ? stackalloc byte[StackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(maxDecodedBytes));

        try
        {
            if (!Base64Url.TryDecodeFromChars(cursor.AsSpan(), buf, out int bytesWritten))
                return false;

            buf = buf[..bytesWritten];
            int pos = 0;

            if (buf.Length < 1 || buf[pos++] != Version)
                return false;

            if (pos + 4 > buf.Length) return false;
            int lastKeyByteCount = BinaryPrimitives.ReadInt32LittleEndian(buf[pos..]);
            pos += 4;
            if (lastKeyByteCount < 0 || pos + lastKeyByteCount > buf.Length) return false;
            lastKey = Encoding.UTF8.GetString(buf.Slice(pos, lastKeyByteCount));
            pos += lastKeyByteCount;

            if (pos + 4 > buf.Length) return false;
            int prefixByteCount = BinaryPrimitives.ReadInt32LittleEndian(buf[pos..]);
            pos += 4;
            if (prefixByteCount < 0 || pos + prefixByteCount > buf.Length) return false;
            prefix = Encoding.UTF8.GetString(buf.Slice(pos, prefixByteCount));
            pos += prefixByteCount;

            if (pos >= buf.Length) return false;
            durability = (KeyValueDurability)buf[pos++];

            if (pos + 4 > buf.Length) return false;
            int tsN = BinaryPrimitives.ReadInt32LittleEndian(buf[pos..]);
            pos += 4;

            if (pos + 8 > buf.Length) return false;
            long tsL = BinaryPrimitives.ReadInt64LittleEndian(buf[pos..]);
            pos += 8;

            if (pos + 4 > buf.Length) return false;
            uint tsC = BinaryPrimitives.ReadUInt32LittleEndian(buf[pos..]);

            readTimestamp = new HLCTimestamp(tsN, tsL, tsC);
            return true;
        }
        catch
        {
            return false;
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
