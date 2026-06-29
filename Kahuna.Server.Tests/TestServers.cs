using System.Globalization;
using System.Text;
using System.Threading;
using Google.Protobuf;
using Kahuna.Client;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication.Protos;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

public class TestServers
{
    [Fact]
    public void TestInvalidScriptReportsLineAndColumn()
    {
        ScriptParserProcessor parser = new(new KahunaConfiguration(), NullLogger<IKahuna>.Instance);

        KahunaScriptException ex = Assert.Throws<KahunaScriptException>(() =>
            parser.Parse("SET pp 'hello world'\nBROKEN command"));

        Assert.Equal(2, ex.Line);
        Assert.Equal(1, ex.Column);
        Assert.Contains("line 2, column 1", ex.Message);
        Assert.Contains("BROKEN", ex.Message);
        Assert.DoesNotContain("line 0", ex.Message);
    }

    [Fact]
    public void TestLargeScriptUsesArrayPoolFallback()
    {
        // A script whose UTF-8 encoding exceeds the 4096-byte stackalloc threshold so the
        // ArrayPool fallback path in ScriptParserProcessor.Parse(string) is exercised.
        ScriptParserProcessor parser = new(new KahunaConfiguration(), NullLogger<IKahuna>.Instance);

        // Build a valid script: LET x = 'aaaa...' RETURN x with a string literal long enough
        // that the total script UTF-8 byte count exceeds 4096.
        string largeValue = new string('a', 4200);
        string script = $"LET x = '{largeValue}'\nRETURN x";

        Assert.True(Encoding.UTF8.GetByteCount(script) > 4096, "pre-condition: script must exceed the 4096-byte stack threshold");

        NodeAst ast = parser.Parse(script);

        Assert.NotNull(ast);
    }

    // ── S5: EncodeDouble / ParseLongFromBytes ────────────────────────────────────

    [Fact]
    public void TestEncodeDoublePositive()
    {
        byte[]? bytes = new KeyValueExpressionResult(3.14).ToBytes();
        Assert.Equal("3.14", Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void TestEncodeDoubleNegative()
    {
        byte[]? bytes = new KeyValueExpressionResult(-1.5).ToBytes();
        Assert.Equal("-1.5", Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void TestEncodeDoubleRoundTrip()
    {
        // A value whose ToString representation requires > 15 digits to preserve
        // the round-trip invariant — verifies EncodeDouble uses full-precision formatting.
        double value = 0.1 + 0.2;
        string expected = value.ToString(CultureInfo.InvariantCulture);
        byte[]? bytes = new KeyValueExpressionResult(value).ToBytes();
        Assert.Equal(expected, Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void TestEncodeDoubleNaN()
    {
        byte[]? bytes = new KeyValueExpressionResult(double.NaN).ToBytes();
        Assert.Equal("NaN", Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void TestEncodeDoublePositiveInfinity()
    {
        byte[]? bytes = new KeyValueExpressionResult(double.PositiveInfinity).ToBytes();
        Assert.Equal("Infinity", Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void TestEncodeDoubleNegativeInfinity()
    {
        byte[]? bytes = new KeyValueExpressionResult(double.NegativeInfinity).ToBytes();
        Assert.Equal("-Infinity", Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void TestEncodeDoubleIgnoresThreadCulture()
    {
        // fr-FR uses ',' as decimal separator — output must still use '.' (InvariantCulture)
        CultureInfo saved = Thread.CurrentThread.CurrentCulture;
        try
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("fr-FR");
            byte[]? bytes = new KeyValueExpressionResult(3.14).ToBytes();
            Assert.Equal("3.14", Encoding.UTF8.GetString(bytes!));
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = saved;
        }
    }

    [Fact]
    public void TestParseLongFromBytesPositive()
    {
        var result = new KeyValueExpressionResult(KeyValueExpressionType.BytesType);
        result.BytesValue = "42"u8.ToArray();
        Assert.Equal(42, result.ToLong());
    }

    [Fact]
    public void TestParseLongFromBytesNegative()
    {
        var result = new KeyValueExpressionResult(KeyValueExpressionType.BytesType);
        result.BytesValue = "-9876543210"u8.ToArray();
        Assert.Equal(-9876543210L, result.ToLong());
    }

    [Fact]
    public void TestParseLongFromBytesRoundTrip()
    {
        // Long encoded by EncodeLong, decoded by ToLong — round-trips through UTF-8 bytes
        var encoded = new KeyValueExpressionResult(long.MaxValue).ToBytes();
        var roundTrip = new KeyValueExpressionResult(KeyValueExpressionType.BytesType);
        roundTrip.BytesValue = encoded;
        Assert.Equal(long.MaxValue, roundTrip.ToLong());
    }

    [Fact]
    public void TestParseLongFromBytesNullReturnsMinusOne()
    {
        var result = new KeyValueExpressionResult(KeyValueExpressionType.BytesType);
        // BytesValue is null (not set) → must return -1
        Assert.Equal(-1, result.ToLong());
    }

    // ── S8: FnvHashStream ────────────────────────────────────────────────────────

    [Fact]
    public void TestFnvHashStreamMatchesMemoryStreamChecksum()
    {
        const ulong FnvOffsetBasis = 14695981039346656037UL;
        const ulong FnvPrime       = 1099511628211UL;

        RangeSnapshotEntry[] entries =
        [
            new() { Key = "alpha",   Revision = 1, Value = ByteString.CopyFromUtf8("hello world") },
            new() { Key = "beta",    Revision = 2 },
            new() { Key = "gamma",   Revision = 3, Value = ByteString.CopyFromUtf8(new string('x', 4096)) },
        ];

        // Old path: buffer all bytes then hash
        ulong expected = FnvOffsetBasis;
        using (MemoryStream ms = new())
        {
            foreach (RangeSnapshotEntry e in entries)
                e.WriteTo(ms);
            foreach (byte b in ms.GetBuffer().AsSpan(0, (int)ms.Length))
            {
                expected ^= b;
                expected *= FnvPrime;
            }
        }

        // New path: hash bytes as they are written
        KvStateMachineTransfer.FnvHashStream hasher = new();
        foreach (RangeSnapshotEntry e in entries)
            e.WriteTo(hasher);

        Assert.Equal(expected, hasher.Hash);
    }

    [Fact]
    public void TestFnvHashStreamEmptyEntriesYieldsOffsetBasis()
    {
        const ulong FnvOffsetBasis = 14695981039346656037UL;

        KvStateMachineTransfer.FnvHashStream hasher = new();
        Assert.Equal(FnvOffsetBasis, hasher.Hash);
    }

    [Fact]
    public void TestLockOwnerIs32ByteLowercaseHex()
    {
        byte[] owner = KahunaClient.NewLockOwner();

        Assert.Equal(32, owner.Length);
        string asString = Encoding.ASCII.GetString(owner);
        Assert.Equal(32, asString.Length);
        Assert.Matches("^[0-9a-f]{32}$", asString);
    }

    /*[Fact]
    public void TestParserBegin()
    {
        NodeAst ast = ScriptParserProcessor.Parse("BEGIN LET x = GET yy END");

        Assert.Equal(NodeType.Begin, ast.nodeType);

        Assert.NotNull(ast.leftAst);
        Assert.Equal(NodeType.Get, ast.leftAst.nodeType);
    }

    [Fact]
    public void TestParserReturn()
    {
        NodeAst ast = ScriptParserProcessor.Parse("RETURN p");

        Assert.Equal(NodeType.Return, ast.nodeType);

        Assert.NotNull(ast.leftAst);
        Assert.Equal(NodeType.Identifier, ast.leftAst.nodeType);
    }*/


}
