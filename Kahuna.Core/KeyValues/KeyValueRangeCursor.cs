
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

        // 1 (version) + 4 + lastKeyBytes + 4 + prefixBytes + 1 + 4 + 8 + 4
        int totalBytes = 1 + 4 + lastKeyByteCount + 4 + prefixByteCount + 1 + 4 + 8 + 4;

        byte[] buf = new byte[totalBytes];
        int pos = 0;

        buf[pos++] = Version;

        WriteInt32(buf, ref pos, lastKeyByteCount);
        Encoding.UTF8.GetBytes(lastKey, 0, lastKey.Length, buf, pos);
        pos += lastKeyByteCount;

        WriteInt32(buf, ref pos, prefixByteCount);
        Encoding.UTF8.GetBytes(prefix, 0, prefix.Length, buf, pos);
        pos += prefixByteCount;

        buf[pos++] = (byte)durability;

        WriteInt32(buf,  ref pos, readTimestamp.N);
        WriteInt64(buf,  ref pos, readTimestamp.L);
        WriteUInt32(buf, ref pos, readTimestamp.C);

        return Convert.ToBase64String(buf).TrimEnd('=').Replace('+', '-').Replace('/', '_');
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

        try
        {
            string padded = cursor.Replace('-', '+').Replace('_', '/');
            int mod = padded.Length % 4;
            if (mod != 0) padded += new string('=', 4 - mod);

            byte[] buf = Convert.FromBase64String(padded);
            int pos = 0;

            if (buf.Length < 1 || buf[pos++] != Version)
                return false;

            if (!TryReadInt32(buf, ref pos, out int lastKeyByteCount) || lastKeyByteCount < 0)
                return false;
            if (pos + lastKeyByteCount > buf.Length)
                return false;
            lastKey = Encoding.UTF8.GetString(buf, pos, lastKeyByteCount);
            pos += lastKeyByteCount;

            if (!TryReadInt32(buf, ref pos, out int prefixByteCount) || prefixByteCount < 0)
                return false;
            if (pos + prefixByteCount > buf.Length)
                return false;
            prefix = Encoding.UTF8.GetString(buf, pos, prefixByteCount);
            pos += prefixByteCount;

            if (pos >= buf.Length)
                return false;
            durability = (KeyValueDurability)buf[pos++];

            if (!TryReadInt32(buf,  ref pos, out int tsN))  return false;
            if (!TryReadInt64(buf,  ref pos, out long tsL)) return false;
            if (!TryReadUInt32(buf, ref pos, out uint tsC)) return false;

            readTimestamp = new HLCTimestamp(tsN, tsL, tsC);
            return true;
        }
        catch
        {
            return false;
        }
    }

    // ── little-endian helpers ──────────────────────────────────────────────────

    private static void WriteInt32(byte[] buf, ref int pos, int value)
    {
        buf[pos++] = (byte)(value);
        buf[pos++] = (byte)(value >> 8);
        buf[pos++] = (byte)(value >> 16);
        buf[pos++] = (byte)(value >> 24);
    }

    private static void WriteInt64(byte[] buf, ref int pos, long value)
    {
        buf[pos++] = (byte)(value);
        buf[pos++] = (byte)(value >> 8);
        buf[pos++] = (byte)(value >> 16);
        buf[pos++] = (byte)(value >> 24);
        buf[pos++] = (byte)(value >> 32);
        buf[pos++] = (byte)(value >> 40);
        buf[pos++] = (byte)(value >> 48);
        buf[pos++] = (byte)(value >> 56);
    }

    private static void WriteUInt32(byte[] buf, ref int pos, uint value)
    {
        buf[pos++] = (byte)(value);
        buf[pos++] = (byte)(value >> 8);
        buf[pos++] = (byte)(value >> 16);
        buf[pos++] = (byte)(value >> 24);
    }

    private static bool TryReadInt32(byte[] buf, ref int pos, out int value)
    {
        value = 0;
        if (pos + 4 > buf.Length) return false;
        value = buf[pos] | (buf[pos + 1] << 8) | (buf[pos + 2] << 16) | (buf[pos + 3] << 24);
        pos += 4;
        return true;
    }

    private static bool TryReadInt64(byte[] buf, ref int pos, out long value)
    {
        value = 0;
        if (pos + 8 > buf.Length) return false;
        value = (long)buf[pos]
              | ((long)buf[pos + 1] << 8)
              | ((long)buf[pos + 2] << 16)
              | ((long)buf[pos + 3] << 24)
              | ((long)buf[pos + 4] << 32)
              | ((long)buf[pos + 5] << 40)
              | ((long)buf[pos + 6] << 48)
              | ((long)buf[pos + 7] << 56);
        pos += 8;
        return true;
    }

    private static bool TryReadUInt32(byte[] buf, ref int pos, out uint value)
    {
        value = 0;
        if (pos + 4 > buf.Length) return false;
        value = (uint)(buf[pos] | (buf[pos + 1] << 8) | (buf[pos + 2] << 16) | (buf[pos + 3] << 24));
        pos += 4;
        return true;
    }
}
