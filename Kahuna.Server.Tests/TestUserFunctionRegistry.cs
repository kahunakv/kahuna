using System.Text;

using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Extensibility;
using Kahuna.Server.KeyValues.Transactions.Functions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers the registration rules and the value model of the user-defined function API. Nothing here
/// starts a node: every rule these tests assert is enforced at registration time, which is the point
/// of the design — a misconfigured deployment must fail to start rather than fail one transaction.
/// </summary>
public class TestUserFunctionRegistry
{
    private static KahunaValue Noop(in KahunaFunctionContext context, ReadOnlySpan<KahunaValue> args) => KahunaValue.Null;

    [Fact]
    public void TestRegisterAndLookUp()
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_one", Noop);

        Assert.True(registry.Contains("acme_one"));
        Assert.False(registry.Contains("acme_two"));
        Assert.Equal(1, registry.Count);
        Assert.Equal(["acme_one"], registry.Names);
    }

    [Fact]
    public void TestRegisterIsChainable()
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_a", Noop).Register("acme_b", Noop).Register("acme_c", Noop);

        Assert.Equal(3, registry.Count);
        Assert.Equal(["acme_a", "acme_b", "acme_c"], registry.Names);
    }

    [Fact]
    public void TestDuplicateNameThrows()
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_one", Noop);

        ArgumentException ex = Assert.Throws<ArgumentException>(() => registry.Register("acme_one", Noop));

        Assert.Contains("already registered", ex.Message, StringComparison.Ordinal);

        // The rejected registration must not have disturbed the first one.
        Assert.Equal(1, registry.Count);
    }

    [Fact]
    public void TestNameMatchingIsCaseSensitive()
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_one", Noop);
        registry.Register("ACME_ONE", Noop);

        Assert.Equal(2, registry.Count);
        Assert.True(registry.Contains("acme_one"));
        Assert.True(registry.Contains("ACME_ONE"));
    }

    [Theory]
    [InlineData("abs")]
    [InlineData("concat")]
    [InlineData("current_time")]
    [InlineData("to_int")]
    [InlineData("to_integer")]
    [InlineData("to_long")]
    [InlineData("to_number")]
    [InlineData("is_str")]
    [InlineData("is_string")]
    [InlineData("rev")]
    [InlineData("revision")]
    [InlineData("len")]
    [InlineData("length")]
    public void TestBuiltInNameIsReserved(string name)
    {
        KahunaFunctionRegistry registry = new();

        ArgumentException ex = Assert.Throws<ArgumentException>(() => registry.Register(name, Noop));

        Assert.Contains("reserved", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The reserved set is read from the engine's own table, so a built-in added later becomes
    /// reserved with it. A hand-copied list here would defeat that, so this walks the live table.
    /// </summary>
    [Fact]
    public void TestEveryBuiltInNameAndAliasIsReserved()
    {
        KahunaFunctionRegistry registry = new();

        Assert.NotEmpty(CallFunction.BuiltInNames);

        foreach (string name in CallFunction.BuiltInNames)
            Assert.Throws<ArgumentException>(() => registry.Register(name, Noop));

        Assert.Equal(0, registry.Count);
    }

    [Theory]
    [InlineData("")]
    [InlineData("1acme")]
    [InlineData("acme-one")]
    [InlineData("acme one")]
    [InlineData("acme.one")]
    [InlineData("acme(")]
    [InlineData("ñoño")]
    public void TestInvalidIdentifierThrows(string name)
    {
        KahunaFunctionRegistry registry = new();

        Assert.Throws<ArgumentException>(() => registry.Register(name, Noop));
    }

    [Theory]
    [InlineData("_acme")]
    [InlineData("acme_1")]
    [InlineData("ACME")]
    [InlineData("a")]
    public void TestValidIdentifierIsAccepted(string name)
    {
        KahunaFunctionRegistry registry = new();

        registry.Register(name, Noop);

        Assert.True(registry.Contains(name));
    }

    [Fact]
    public void TestNullDelegateThrows()
    {
        KahunaFunctionRegistry registry = new();

        Assert.Throws<ArgumentNullException>(() => registry.Register("acme_one", null!));
    }

    [Theory]
    [InlineData(-1, -1)]
    [InlineData(-5, 3)]
    [InlineData(3, 2)]
    [InlineData(1, 0)]
    public void TestBadArityBoundsThrow(int minArgs, int maxArgs)
    {
        KahunaFunctionRegistry registry = new();

        Assert.Throws<ArgumentOutOfRangeException>(() => registry.Register("acme_one", Noop, minArgs, maxArgs));
    }

    [Theory]
    [InlineData(0, -1)]
    [InlineData(0, 0)]
    [InlineData(2, 2)]
    [InlineData(1, 8)]
    [InlineData(3, -1)]
    public void TestGoodArityBoundsAreAccepted(int minArgs, int maxArgs)
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_one", Noop, minArgs, maxArgs);

        Assert.True(registry.Contains("acme_one"));
    }

    [Fact]
    public void TestRegisterAfterFreezeThrows()
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_one", Noop);
        registry.Freeze(out _);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => registry.Register("acme_two", Noop));

        Assert.Contains("frozen", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestFreezeReturnsEveryEntry()
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_a", Noop, 1, 2);
        registry.Register("acme_b", Noop);

        KahunaFunctionEntry[] entries = registry.Freeze(out string fingerprint);

        Assert.Equal(2, entries.Length);
        Assert.Equal(fingerprint, registry.Fingerprint);

        KahunaFunctionEntry first = Assert.Single(entries, entry => entry.Name == "acme_a");

        Assert.Equal(1, first.MinArgs);
        Assert.Equal(2, first.MaxArgs);
    }

    [Fact]
    public void TestFingerprintIsStableAcrossRegistrationOrder()
    {
        KahunaFunctionRegistry one = new();
        KahunaFunctionRegistry two = new();

        one.Register("acme_a", Noop, 1, 2).Register("acme_b", Noop, 0, -1).Register("acme_c", Noop);
        two.Register("acme_c", Noop).Register("acme_b", Noop, 0, -1).Register("acme_a", Noop, 1, 2);

        Assert.Equal(one.Fingerprint, two.Fingerprint);
    }

    [Fact]
    public void TestFingerprintChangesWithName()
    {
        KahunaFunctionRegistry one = new();
        KahunaFunctionRegistry two = new();

        one.Register("acme_a", Noop);
        two.Register("acme_z", Noop);

        Assert.NotEqual(one.Fingerprint, two.Fingerprint);
    }

    [Fact]
    public void TestFingerprintChangesWithArity()
    {
        KahunaFunctionRegistry one = new();
        KahunaFunctionRegistry two = new();

        one.Register("acme_a", Noop, 1, 2);
        two.Register("acme_a", Noop, 1, 3);

        Assert.NotEqual(one.Fingerprint, two.Fingerprint);
    }

    [Fact]
    public void TestFingerprintDoesNotChangeWithImplementation()
    {
        KahunaFunctionRegistry one = new();
        KahunaFunctionRegistry two = new();

        one.Register("acme_a", Noop, 1, 1);
        two.Register("acme_a", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(7L), 1, 1);

        // The fingerprint describes the callable surface, not the code behind it. Two nodes running
        // different builds of the same function are a deployment problem the assembly hash catches,
        // not something a registry hash can see.
        Assert.Equal(one.Fingerprint, two.Fingerprint);
    }

    [Fact]
    public void TestFingerprintIsSixteenLowercaseHexCharacters()
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_a", Noop);

        string fingerprint = registry.Fingerprint;

        Assert.Equal(16, fingerprint.Length);
        Assert.All(fingerprint, character => Assert.True(char.IsAsciiDigit(character) || (character >= 'a' && character <= 'f')));
    }

    [Fact]
    public void TestEmptyRegistryStillHasAFingerprint()
    {
        KahunaFunctionRegistry registry = new();

        Assert.Equal(16, registry.Fingerprint.Length);
        Assert.Equal(0, registry.Count);
        Assert.NotEqual(new KahunaFunctionRegistry().Register("acme_a", Noop).Fingerprint, registry.Fingerprint);
    }

    [Fact]
    public void TestFingerprintTracksLateRegistration()
    {
        KahunaFunctionRegistry registry = new();

        registry.Register("acme_a", Noop);

        string before = registry.Fingerprint;

        registry.Register("acme_b", Noop);

        Assert.NotEqual(before, registry.Fingerprint);
    }

    [Fact]
    public void TestNullValue()
    {
        Assert.Equal(KahunaValueKind.Null, KahunaValue.Null.Kind);
        Assert.Equal(KahunaValueKind.Null, default(KahunaValue).Kind);
        Assert.True(KahunaValue.Null.IsNull);
        Assert.True(KahunaValue.From((string?)null).IsNull);
        Assert.True(KahunaValue.From((byte[]?)null).IsNull);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TestBoolRoundTrip(bool value)
    {
        KahunaValue result = KahunaValue.From(value);

        Assert.Equal(KahunaValueKind.Bool, result.Kind);
        Assert.Equal(value, result.AsBool());
        Assert.True(result.TryGetBool(out bool got));
        Assert.Equal(value, got);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    [InlineData(9007199254740993L)]
    public void TestLongRoundTrip(long value)
    {
        KahunaValue result = KahunaValue.From(value);

        Assert.Equal(KahunaValueKind.Long, result.Kind);

        // 9007199254740993 is the smallest integer a double cannot hold. It is here on purpose: a
        // shared floating-point slot would silently return 9007199254740992 instead.
        Assert.Equal(value, result.AsLong());
        Assert.True(result.TryGetLong(out long got));
        Assert.Equal(value, got);
    }

    [Theory]
    [InlineData(0d)]
    [InlineData(1.5d)]
    [InlineData(-1.5d)]
    [InlineData(double.MaxValue)]
    [InlineData(double.Epsilon)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void TestDoubleRoundTrip(double value)
    {
        KahunaValue result = KahunaValue.From(value);

        Assert.Equal(KahunaValueKind.Double, result.Kind);
        Assert.Equal(value, result.AsDouble());
        Assert.True(result.TryGetDouble(out double got));
        Assert.Equal(value, got);
    }

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("ñ € 漢")]
    public void TestStringRoundTrip(string value)
    {
        KahunaValue result = KahunaValue.From(value);

        Assert.Equal(KahunaValueKind.String, result.Kind);
        Assert.Equal(value, result.AsString());
        Assert.True(result.TryGetString(out string got));
        Assert.Equal(value, got);
    }

    [Fact]
    public void TestBytesRoundTrip()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("payload");

        KahunaValue result = KahunaValue.From(bytes);

        Assert.Equal(KahunaValueKind.Bytes, result.Kind);
        Assert.Equal(bytes, result.AsBytes().ToArray());
        Assert.True(result.TryGetBytes(out ReadOnlyMemory<byte> got));
        Assert.Equal(bytes, got.ToArray());
    }

    [Fact]
    public void TestEmptyBytesRoundTrip()
    {
        KahunaValue result = KahunaValue.From(Array.Empty<byte>());

        Assert.Equal(KahunaValueKind.Bytes, result.Kind);
        Assert.Equal(0, result.AsBytes().Length);
    }

    [Fact]
    public void TestSlicedBytesKeepsItsBounds()
    {
        byte[] bytes = [1, 2, 3, 4, 5, 6, 7, 8];

        KahunaValue result = KahunaValue.From(new ReadOnlyMemory<byte>(bytes, 2, 3));

        Assert.Equal(KahunaValueKind.Bytes, result.Kind);
        Assert.Equal<byte[]>([3, 4, 5], result.AsBytes().ToArray());
    }

    [Fact]
    public void TestLargeBytesKeepsItsBounds()
    {
        // The offset and the length share one 64-bit slot. A buffer past the 16-bit and 32-bit
        // boundaries proves the packing, which a small buffer cannot.
        byte[] bytes = new byte[100_000];

        bytes[99_999] = 42;

        KahunaValue result = KahunaValue.From(new ReadOnlyMemory<byte>(bytes, 70_000, 30_000));

        Assert.Equal(30_000, result.AsBytes().Length);
        Assert.Equal(42, result.AsBytes().Span[29_999]);
    }

    [Fact]
    public void TestArrayRoundTrip()
    {
        KahunaValue result = KahunaValue.FromArray([KahunaValue.From(1L), KahunaValue.From("two")]);

        Assert.Equal(KahunaValueKind.Array, result.Kind);

        IReadOnlyList<KahunaValue> values = result.AsArray();

        Assert.Equal(2, values.Count);
        Assert.Equal(1L, values[0].AsLong());
        Assert.Equal("two", values[1].AsString());
    }

    [Fact]
    public void TestNestedArrayRoundTrip()
    {
        KahunaValue inner = KahunaValue.FromArray([KahunaValue.From(true), KahunaValue.Null]);
        KahunaValue outer = KahunaValue.FromArray([inner, KahunaValue.From(3.5d)]);

        IReadOnlyList<KahunaValue> values = outer.AsArray();

        Assert.Equal(2, values.Count);
        Assert.Equal(KahunaValueKind.Array, values[0].Kind);
        Assert.True(values[0].AsArray()[0].AsBool());
        Assert.True(values[0].AsArray()[1].IsNull);
        Assert.Equal(3.5d, values[1].AsDouble());
    }

    [Fact]
    public void TestEmptyAndNullArray()
    {
        Assert.Empty(KahunaValue.FromArray([]).AsArray());
        Assert.Empty(KahunaValue.FromArray(null).AsArray());
    }

    [Fact]
    public void TestAccessorKindMismatchThrows()
    {
        KahunaValue value = KahunaValue.From(1L);

        Assert.Throws<KahunaFunctionException>(() => value.AsBool());
        Assert.Throws<KahunaFunctionException>(() => value.AsDouble());
        Assert.Throws<KahunaFunctionException>(() => value.AsString());
        Assert.Throws<KahunaFunctionException>(() => value.AsBytes());
        Assert.Throws<KahunaFunctionException>(() => value.AsArray());
        Assert.Throws<KahunaFunctionException>(() => KahunaValue.Null.AsLong());
    }

    [Fact]
    public void TestMismatchMessageNamesBothKinds()
    {
        KahunaFunctionException ex = Assert.Throws<KahunaFunctionException>(() => KahunaValue.From("text").AsLong());

        Assert.Contains("Long", ex.Message, StringComparison.Ordinal);
        Assert.Contains("String", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TestTryGetReturnsFalseInsteadOfThrowing()
    {
        KahunaValue value = KahunaValue.From(1L);

        Assert.False(value.TryGetBool(out _));
        Assert.False(value.TryGetDouble(out _));
        Assert.False(value.TryGetString(out string text));
        Assert.Equal(string.Empty, text);
        Assert.False(value.TryGetBytes(out ReadOnlyMemory<byte> bytes));
        Assert.Equal(0, bytes.Length);
        Assert.False(value.TryGetArray(out IReadOnlyList<KahunaValue> values));
        Assert.Empty(values);
    }

    [Fact]
    public void TestContextFailThrowsWithItsMessage()
    {
        KahunaFunctionContext context = new("acme_one", 7, default, default, "node-1", NullLogger.Instance);

        KahunaFunctionException ex = Assert.Throws<KahunaFunctionException>(() => context.Fail("bad input"));

        Assert.Equal("bad input", ex.Message);
        Assert.Equal("acme_one", context.FunctionName);
        Assert.Equal(7, context.Line);
        Assert.Equal("node-1", context.NodeName);
    }
}
