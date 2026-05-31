
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Tests.Server;

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
