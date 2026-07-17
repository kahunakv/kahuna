using System.IO;
using System.IO.Hashing;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Pins the operation-digest algorithm and byte layout: an xxHash128 over fields framed as
/// length-prefixed UTF-8 (a 4-byte little-endian byte count, −1 for null, followed by the bytes).
/// The reference below encodes strings the straightforward way (a fresh <c>Encoding.UTF8.GetBytes</c>
/// array) and hashes with the same xxHash128; the production digest encodes strings through a bounded
/// stack/pooled buffer. Asserting they match across null/empty/ASCII/multi-byte/long inputs proves the
/// bounded encoder is byte-for-byte identical, so identical declarations still collide (and distinct
/// declarations still don't).
/// </summary>
public sealed class TestOperationDigest
{
    private static void RefTag(BinaryWriter w, OperationKind kind) => w.Write((int)kind);

    private static void RefString(BinaryWriter w, string? s)
    {
        if (s is null) { w.Write(-1); return; }
        byte[] b = Encoding.UTF8.GetBytes(s);
        w.Write(b.Length);
        w.Write(b);
    }

    private static void RefBytes(BinaryWriter w, byte[]? b)
    {
        w.Write(b?.Length ?? -1);
        if (b is not null) w.Write(b);
    }

    private static void RefHlc(BinaryWriter w, HLCTimestamp t)
    {
        w.Write(t.N);
        w.Write(t.L);
        w.Write((long)t.C);
    }

    private static byte[] Digest(Action<BinaryWriter> build)
    {
        using MemoryStream ms = new();
        using (BinaryWriter w = new(ms, Encoding.UTF8, leaveOpen: true))
            build(w);
        return XxHash128.Hash(ms.ToArray());
    }

    public static IEnumerable<object?[]> StringSamples()
    {
        yield return [null];
        yield return [""];
        yield return ["orders/eu-west-1"];
        yield return ["ключ/セマフォ/🔒"];                    // multi-byte UTF-8
        yield return [new string('x', 400)];                  // exceeds the 256-byte stack threshold
    }

    [Theory]
    [MemberData(nameof(StringSamples))]
    public void ForRangeScan_MatchesLengthPrefixedUtf8Framing(string? boundKey)
    {
        HLCTimestamp read = new(1, 987_654_321L, 5u);

        byte[] expected = Digest(w =>
        {
            RefTag(w, OperationKind.Scan);
            RefString(w, "acc/orders");     // prefix
            RefString(w, boundKey);         // startKey
            w.Write(1);                     // startInclusive
            RefString(w, boundKey);         // endKey
            w.Write(0);                     // endInclusive
            w.Write(64);                    // limit
            RefHlc(w, read);
            w.Write((int)KeyValueDurability.Persistent);
        });

        byte[] actual = OperationDigest.ForRangeScan(
            "acc/orders", boundKey, true, boundKey, false, 64, read, KeyValueDurability.Persistent);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(StringSamples))]
    public void ForSet_MatchesLengthPrefixedUtf8Framing(string? key)
    {
        // ForSet's key parameter is non-null in practice; skip the null sample for it.
        if (key is null) return;

        byte[] value = [9, 8, 7];
        byte[] expected = Digest(w =>
        {
            RefTag(w, OperationKind.Set);
            RefString(w, key);
            RefBytes(w, value);
            RefBytes(w, null);              // compareValue
            w.Write(0L);                    // compareRevision
            w.Write((int)KeyValueFlags.None);
            w.Write(1000);                  // expiresMs
            w.Write((int)KeyValueDurability.Ephemeral);
        });

        byte[] actual = OperationDigest.ForSet(
            key, value, null, 0, KeyValueFlags.None, 1000, KeyValueDurability.Ephemeral);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Digest_IsDeterministic_AndBoundarySensitive()
    {
        byte[] a1 = OperationDigest.ForSet("ab", [1], null, 0, KeyValueFlags.None, 0, KeyValueDurability.Ephemeral);
        byte[] a2 = OperationDigest.ForSet("ab", [1], null, 0, KeyValueFlags.None, 0, KeyValueDurability.Ephemeral);
        byte[] b  = OperationDigest.ForSet("a",  [1], null, 0, KeyValueFlags.None, 0, KeyValueDurability.Ephemeral);

        Assert.Equal(a1, a2);       // deterministic
        Assert.NotEqual(a1, b);     // field boundaries bind ("ab" != "a")
        Assert.Equal(16, a1.Length); // xxHash128 digest is 16 bytes
    }
}
