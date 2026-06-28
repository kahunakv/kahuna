using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after comparison for the cursor-codec rewrite in <c>KeyValueRangeCursor</c>.
///
/// <para><b>Old</b> = <c>new byte[]</c> buffer + manual little-endian writes +
/// <c>Convert.ToBase64String().TrimEnd('=').Replace(...)</c> on encode, and
/// <c>Replace + Convert.FromBase64String</c> on decode. <b>New</b> = bounded
/// <c>stackalloc</c>/<see cref="ArrayPool{T}"/> + <see cref="BinaryPrimitives"/> +
/// <see cref="Base64Url"/>.</para>
///
/// Both are reproduced verbatim because the production type is <c>internal</c>; the <b>New</b>
/// copies are kept identical to the shipped code so the numbers are honest.
/// </summary>
[MemoryDiagnoser]
public class CursorCodecBenchmark
{
    private const byte Version = 0x01;
    private const int FixedOverhead = 26;
    private const int StackThreshold = 256;

    // Representative cursor payload: a service-scoped key + short prefix (fits the stack path).
    private const string LastKey = "svc/00000042";
    private const string Prefix = "svc";
    private const byte Durability = 0; // Persistent
    private static readonly (int N, long L, uint C) Ts = (7, 1_700_000_000_000L, 42u);

    private string _oldCursor = "";
    private string _newCursor = "";

    [GlobalSetup]
    public void Setup()
    {
        _oldCursor = OldEncode();
        _newCursor = NewEncode();
        if (_oldCursor != _newCursor)
            throw new InvalidOperationException("Old and New encoders disagree — benchmark is invalid.");
    }

    [Benchmark(Baseline = true)]
    public string OldEncodeBench() => OldEncode();

    [Benchmark]
    public string NewEncodeBench() => NewEncode();

    [Benchmark]
    public bool OldDecodeBench() => OldDecode(_oldCursor);

    [Benchmark]
    public bool NewDecodeBench() => NewDecode(_newCursor);

    // ── old implementation ──────────────────────────────────────────────────────

    private static string OldEncode()
    {
        int lastKeyByteCount = Encoding.UTF8.GetByteCount(LastKey);
        int prefixByteCount = Encoding.UTF8.GetByteCount(Prefix);
        int totalBytes = 1 + 4 + lastKeyByteCount + 4 + prefixByteCount + 1 + 4 + 8 + 4;

        byte[] buf = new byte[totalBytes];
        int pos = 0;
        buf[pos++] = Version;
        OldWriteI32(buf, ref pos, lastKeyByteCount);
        Encoding.UTF8.GetBytes(LastKey, 0, LastKey.Length, buf, pos);
        pos += lastKeyByteCount;
        OldWriteI32(buf, ref pos, prefixByteCount);
        Encoding.UTF8.GetBytes(Prefix, 0, Prefix.Length, buf, pos);
        pos += prefixByteCount;
        buf[pos++] = Durability;
        OldWriteI32(buf, ref pos, Ts.N);
        OldWriteI64(buf, ref pos, Ts.L);
        OldWriteU32(buf, ref pos, Ts.C);

        return Convert.ToBase64String(buf).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    private static bool OldDecode(string cursor)
    {
        try
        {
            string padded = cursor.Replace('-', '+').Replace('_', '/');
            int mod = padded.Length % 4;
            if (mod != 0) padded += new string('=', 4 - mod);

            byte[] buf = Convert.FromBase64String(padded);
            int pos = 0;
            if (buf.Length < 1 || buf[pos++] != Version) return false;
            if (!OldReadI32(buf, ref pos, out int lastKeyByteCount) || lastKeyByteCount < 0) return false;
            if (pos + lastKeyByteCount > buf.Length) return false;
            _ = Encoding.UTF8.GetString(buf, pos, lastKeyByteCount);
            pos += lastKeyByteCount;
            if (!OldReadI32(buf, ref pos, out int prefixByteCount) || prefixByteCount < 0) return false;
            if (pos + prefixByteCount > buf.Length) return false;
            _ = Encoding.UTF8.GetString(buf, pos, prefixByteCount);
            pos += prefixByteCount;
            if (pos >= buf.Length) return false;
            pos++;
            if (!OldReadI32(buf, ref pos, out _)) return false;
            if (!OldReadI64(buf, ref pos, out _)) return false;
            if (!OldReadU32(buf, ref pos, out _)) return false;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static void OldWriteI32(byte[] b, ref int p, int v) { b[p++] = (byte)v; b[p++] = (byte)(v >> 8); b[p++] = (byte)(v >> 16); b[p++] = (byte)(v >> 24); }
    private static void OldWriteI64(byte[] b, ref int p, long v) { for (int i = 0; i < 8; i++) b[p++] = (byte)(v >> (i * 8)); }
    private static void OldWriteU32(byte[] b, ref int p, uint v) { b[p++] = (byte)v; b[p++] = (byte)(v >> 8); b[p++] = (byte)(v >> 16); b[p++] = (byte)(v >> 24); }
    private static bool OldReadI32(byte[] b, ref int p, out int v) { v = 0; if (p + 4 > b.Length) return false; v = b[p] | (b[p + 1] << 8) | (b[p + 2] << 16) | (b[p + 3] << 24); p += 4; return true; }
    private static bool OldReadI64(byte[] b, ref int p, out long v) { v = 0; if (p + 8 > b.Length) return false; for (int i = 0; i < 8; i++) v |= (long)b[p + i] << (i * 8); p += 8; return true; }
    private static bool OldReadU32(byte[] b, ref int p, out uint v) { v = 0; if (p + 4 > b.Length) return false; v = (uint)(b[p] | (b[p + 1] << 8) | (b[p + 2] << 16) | (b[p + 3] << 24)); p += 4; return true; }

    // ── new implementation (as shipped) ─────────────────────────────────────────

    private static string NewEncode()
    {
        int lastKeyByteCount = Encoding.UTF8.GetByteCount(LastKey);
        int prefixByteCount = Encoding.UTF8.GetByteCount(Prefix);
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
            Encoding.UTF8.GetBytes(LastKey.AsSpan(), buf[pos..]);
            pos += lastKeyByteCount;
            BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], prefixByteCount);
            pos += 4;
            Encoding.UTF8.GetBytes(Prefix.AsSpan(), buf[pos..]);
            pos += prefixByteCount;
            buf[pos++] = Durability;
            BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], Ts.N);
            pos += 4;
            BinaryPrimitives.WriteInt64LittleEndian(buf[pos..], Ts.L);
            pos += 8;
            BinaryPrimitives.WriteUInt32LittleEndian(buf[pos..], Ts.C);

            return Base64Url.EncodeToString(buf);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static bool NewDecode(string cursor)
    {
        if (string.IsNullOrEmpty(cursor)) return false;

        int maxDecodedBytes = Base64Url.GetMaxDecodedLength(cursor.Length);
        byte[]? rented = null;
        Span<byte> buf = maxDecodedBytes <= StackThreshold
            ? stackalloc byte[StackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(maxDecodedBytes));
        try
        {
            if (!Base64Url.TryDecodeFromChars(cursor.AsSpan(), buf, out int bytesWritten)) return false;
            buf = buf[..bytesWritten];
            int pos = 0;
            if (buf.Length < 1 || buf[pos++] != Version) return false;
            if (pos + 4 > buf.Length) return false;
            int lastKeyByteCount = BinaryPrimitives.ReadInt32LittleEndian(buf[pos..]);
            pos += 4;
            if (lastKeyByteCount < 0 || pos + lastKeyByteCount > buf.Length) return false;
            _ = Encoding.UTF8.GetString(buf.Slice(pos, lastKeyByteCount));
            pos += lastKeyByteCount;
            if (pos + 4 > buf.Length) return false;
            int prefixByteCount = BinaryPrimitives.ReadInt32LittleEndian(buf[pos..]);
            pos += 4;
            if (prefixByteCount < 0 || pos + prefixByteCount > buf.Length) return false;
            _ = Encoding.UTF8.GetString(buf.Slice(pos, prefixByteCount));
            pos += prefixByteCount;
            if (pos >= buf.Length) return false;
            pos++;
            if (pos + 4 > buf.Length) return false;
            _ = BinaryPrimitives.ReadInt32LittleEndian(buf[pos..]);
            pos += 4;
            if (pos + 8 > buf.Length) return false;
            _ = BinaryPrimitives.ReadInt64LittleEndian(buf[pos..]);
            pos += 8;
            if (pos + 4 > buf.Length) return false;
            _ = BinaryPrimitives.ReadUInt32LittleEndian(buf[pos..]);
            return true;
        }
        catch
        {
            return false;
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
