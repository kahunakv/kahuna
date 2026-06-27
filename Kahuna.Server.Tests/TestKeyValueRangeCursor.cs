
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

public class TestKeyValueRangeCursor
{
    [Fact]
    public void TestRoundTripBasic()
    {
        HLCTimestamp ts = new(7, 1_700_000_000_000L, 42);

        string cursor = KeyValueRangeCursor.Encode("svc/0005", KeyValueDurability.Persistent, "svc", ts);

        Assert.True(KeyValueRangeCursor.TryDecode(cursor,
            out string lastKey, out KeyValueDurability durability,
            out string prefix, out HLCTimestamp readTs));

        Assert.Equal("svc/0005", lastKey);
        Assert.Equal(KeyValueDurability.Persistent, durability);
        Assert.Equal("svc", prefix);
        Assert.Equal(7,                    readTs.N);
        Assert.Equal(1_700_000_000_000L,   readTs.L);
        Assert.Equal(42u,                  readTs.C);
    }

    [Fact]
    public void TestRoundTripEphemeralDurability()
    {
        HLCTimestamp ts = new(1, 999L, 0);

        string cursor = KeyValueRangeCursor.Encode("bucket/key-last", KeyValueDurability.Ephemeral, "bucket", ts);

        Assert.True(KeyValueRangeCursor.TryDecode(cursor,
            out string lastKey, out KeyValueDurability durability,
            out string prefix, out HLCTimestamp readTs));

        Assert.Equal("bucket/key-last", lastKey);
        Assert.Equal(KeyValueDurability.Ephemeral, durability);
        Assert.Equal("bucket", prefix);
        Assert.Equal(ts, readTs);
    }

    [Fact]
    public void TestRoundTripZeroReadTimestamp()
    {
        string cursor = KeyValueRangeCursor.Encode("x/y/z", KeyValueDurability.Persistent, "x", HLCTimestamp.Zero);

        Assert.True(KeyValueRangeCursor.TryDecode(cursor,
            out string lastKey, out _, out string prefix, out HLCTimestamp readTs));

        Assert.Equal("x/y/z", lastKey);
        Assert.Equal("x", prefix);
        Assert.Equal(HLCTimestamp.Zero, readTs);
    }

    [Fact]
    public void TestRoundTripMaxValues()
    {
        HLCTimestamp ts = new(int.MaxValue, long.MaxValue, uint.MaxValue);
        string longKey = new('k', 512);
        string longPrefix = new('p', 256);

        string cursor = KeyValueRangeCursor.Encode(longKey, KeyValueDurability.Persistent, longPrefix, ts);

        Assert.True(KeyValueRangeCursor.TryDecode(cursor,
            out string lastKey, out _, out string prefix, out HLCTimestamp readTs));

        Assert.Equal(longKey, lastKey);
        Assert.Equal(longPrefix, prefix);
        Assert.Equal(ts, readTs);
    }

    [Fact]
    public void TestRoundTripUnicodeKeys()
    {
        HLCTimestamp ts = new(3, 12345L, 1);

        // Keys with Unicode content (CamusDB table names can include non-ASCII)
        string cursor = KeyValueRangeCursor.Encode("tbl/键值/0005", KeyValueDurability.Persistent, "tbl/键值", ts);

        Assert.True(KeyValueRangeCursor.TryDecode(cursor,
            out string lastKey, out _, out string prefix, out HLCTimestamp readTs));

        Assert.Equal("tbl/键值/0005", lastKey);
        Assert.Equal("tbl/键值", prefix);
        Assert.Equal(ts, readTs);
    }

    [Fact]
    public void TestCursorIsOpaqueBase64Url()
    {
        string cursor = KeyValueRangeCursor.Encode("k", KeyValueDurability.Persistent, "p", HLCTimestamp.Zero);

        // Must not contain standard base64 chars that are unsafe in URLs
        Assert.DoesNotContain("+", cursor);
        Assert.DoesNotContain("/", cursor);
        Assert.DoesNotContain("=", cursor);
    }

    [Fact]
    public void TestEncodedStringMatchesLegacyBase64UrlFormat()
    {
        // Verifies format stability: the new encoder must produce byte-for-byte identical
        // strings to the old Convert.ToBase64String + TrimEnd + Replace approach.
        HLCTimestamp ts = new(7, 1_700_000_000_000L, 42);
        string lastKey = "svc/0005";
        string prefix  = "svc";

        string newCursor = KeyValueRangeCursor.Encode(lastKey, KeyValueDurability.Persistent, prefix, ts);

        // Reproduce old encoding inline so both paths are compared.
        byte[] binaryBuf = BuildLegacyCursorBytes(lastKey, KeyValueDurability.Persistent, prefix, ts);
        string legacyCursor = Convert.ToBase64String(binaryBuf).TrimEnd('=').Replace('+', '-').Replace('/', '_');

        Assert.Equal(legacyCursor, newCursor);
    }

    private static byte[] BuildLegacyCursorBytes(string lastKey, KeyValueDurability durability, string prefix, HLCTimestamp ts)
    {
        int lastKeyByteCount = System.Text.Encoding.UTF8.GetByteCount(lastKey);
        int prefixByteCount  = System.Text.Encoding.UTF8.GetByteCount(prefix);
        byte[] buf = new byte[1 + 4 + lastKeyByteCount + 4 + prefixByteCount + 1 + 4 + 8 + 4];
        int pos = 0;
        buf[pos++] = 0x01;
        void WriteI32(int v) { buf[pos++]=(byte)v; buf[pos++]=(byte)(v>>8); buf[pos++]=(byte)(v>>16); buf[pos++]=(byte)(v>>24); }
        void WriteI64(long v) { buf[pos++]=(byte)v; buf[pos++]=(byte)(v>>8); buf[pos++]=(byte)(v>>16); buf[pos++]=(byte)(v>>24); buf[pos++]=(byte)(v>>32); buf[pos++]=(byte)(v>>40); buf[pos++]=(byte)(v>>48); buf[pos++]=(byte)(v>>56); }
        void WriteU32(uint v) { buf[pos++]=(byte)v; buf[pos++]=(byte)(v>>8); buf[pos++]=(byte)(v>>16); buf[pos++]=(byte)(v>>24); }
        WriteI32(lastKeyByteCount); System.Text.Encoding.UTF8.GetBytes(lastKey, 0, lastKey.Length, buf, pos); pos += lastKeyByteCount;
        WriteI32(prefixByteCount);  System.Text.Encoding.UTF8.GetBytes(prefix,  0, prefix.Length,  buf, pos); pos += prefixByteCount;
        buf[pos++] = (byte)durability;
        WriteI32(ts.N); WriteI64(ts.L); WriteU32(ts.C);
        return buf;
    }

    [Fact]
    public void TestDecodeGarbageReturnsFalse()
    {
        Assert.False(KeyValueRangeCursor.TryDecode("not-a-valid-cursor!!!",
            out _, out _, out _, out _));
    }

    [Fact]
    public void TestDecodeEmptyStringReturnsFalse()
    {
        Assert.False(KeyValueRangeCursor.TryDecode("",
            out _, out _, out _, out _));
    }

    [Fact]
    public void TestRoundTripEmptyLastKey()
    {
        HLCTimestamp ts = new(1, 500L, 0);
        string cursor = KeyValueRangeCursor.Encode("", KeyValueDurability.Persistent, "svc", ts);

        Assert.True(KeyValueRangeCursor.TryDecode(cursor,
            out string lastKey, out _, out string prefix, out HLCTimestamp readTs));

        Assert.Equal("", lastKey);
        Assert.Equal("svc", prefix);
        Assert.Equal(ts, readTs);
    }

    [Fact]
    public void TestRoundTripEmptyPrefix()
    {
        HLCTimestamp ts = new(2, 999L, 7);
        string cursor = KeyValueRangeCursor.Encode("some/key", KeyValueDurability.Ephemeral, "", ts);

        Assert.True(KeyValueRangeCursor.TryDecode(cursor,
            out string lastKey, out KeyValueDurability durability, out string prefix, out HLCTimestamp readTs));

        Assert.Equal("some/key", lastKey);
        Assert.Equal("", prefix);
        Assert.Equal(KeyValueDurability.Ephemeral, durability);
        Assert.Equal(ts, readTs);
    }

    [Fact]
    public void TestRoundTripLongKeyForcesArrayPoolPath()
    {
        // lastKeyBytes (768) + prefixBytes (200) + 26 fixed = 994 > 256 threshold → ArrayPool
        string longKey    = new('好', 256); // 768 UTF-8 bytes (3 bytes/char) — also exercises multi-byte UTF-8
        string longPrefix = new('p', 200);  // 200 UTF-8 bytes (ASCII)
        HLCTimestamp ts = new(5, 123456789L, 99);

        string cursor = KeyValueRangeCursor.Encode(longKey, KeyValueDurability.Persistent, longPrefix, ts);

        Assert.True(KeyValueRangeCursor.TryDecode(cursor,
            out string decodedKey, out _, out string decodedPrefix, out HLCTimestamp readTs));

        Assert.Equal(longKey, decodedKey);
        Assert.Equal(longPrefix, decodedPrefix);
        Assert.Equal(ts, readTs);
    }

    [Fact]
    public void TestDecodeWrongVersionReturnsFalse()
    {
        // Build a cursor with version byte = 0x02 (unsupported)
        byte[] buf = new byte[22];
        buf[0] = 0x02;
        string cursor = Convert.ToBase64String(buf).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        Assert.False(KeyValueRangeCursor.TryDecode(cursor, out _, out _, out _, out _));
    }

    [Fact]
    public void TestDistinctInputsProduceDistinctCursors()
    {
        HLCTimestamp ts = new(1, 1000L, 0);

        string c1 = KeyValueRangeCursor.Encode("svc/0001", KeyValueDurability.Persistent, "svc", ts);
        string c2 = KeyValueRangeCursor.Encode("svc/0002", KeyValueDurability.Persistent, "svc", ts);
        string c3 = KeyValueRangeCursor.Encode("svc/0001", KeyValueDurability.Ephemeral,  "svc", ts);
        string c4 = KeyValueRangeCursor.Encode("svc/0001", KeyValueDurability.Persistent, "svc", ts + 1);

        Assert.NotEqual(c1, c2);
        Assert.NotEqual(c1, c3);
        Assert.NotEqual(c1, c4);
    }
}
