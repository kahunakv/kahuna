using System.Text;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Xunit;

namespace Kahuna.Client.Tests;

/// <summary>
/// Locks in the observable contract of <see cref="KahunaKeyValue.ValueAsLong"/> and
/// <see cref="KahunaKeyValue.ValueAsBool"/>: the fast UTF-8 parse path must accept exactly the same
/// inputs (and reject exactly the same inputs) as decoding the bytes to a string and parsing that,
/// including surrounding whitespace, a leading sign, case-variant booleans, and malformed/partial
/// content. These do not connect to a cluster.
/// </summary>
public class TestClientValueParsing
{
    private static KahunaKeyValue Kv(byte[]? value) =>
        new(null!, "k", success: true, value, revision: 0, KeyValueDurability.Ephemeral, timeElapsedMs: 0);

    private static KahunaKeyValue Kv(string value) => Kv(Encoding.UTF8.GetBytes(value));

    [Theory]
    [InlineData("0", 0L)]
    [InlineData("123", 123L)]
    [InlineData("-123", -123L)]
    [InlineData("+123", 123L)]          // leading '+' — fallback path
    [InlineData(" 123 ", 123L)]         // surrounding whitespace — fallback path
    [InlineData("9223372036854775807", long.MaxValue)]
    [InlineData("-9223372036854775808", long.MinValue)]
    public void ValueAsLong_ParsesValidIntegers(string text, long expected)
    {
        Assert.Equal(expected, Kv(text).ValueAsLong());
    }

    [Theory]
    [InlineData("")]                    // empty
    [InlineData("abc")]                 // non-numeric
    [InlineData("12.5")]                // not an integer
    [InlineData("123abc")]              // partially valid
    [InlineData("99999999999999999999999999")] // overflow
    public void ValueAsLong_ThrowsOnMalformed(string text)
    {
        Assert.Throws<KahunaException>(() => Kv(text).ValueAsLong());
    }

    [Fact]
    public void ValueAsLong_ThrowsOnNull()
    {
        Assert.Throws<KahunaException>(() => Kv((byte[]?)null).ValueAsLong());
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    [InlineData("True", true)]          // case variant
    [InlineData("FALSE", false)]        // case variant
    [InlineData(" true ", true)]        // surrounding whitespace — fallback path
    public void ValueAsBool_ParsesValidBooleans(string text, bool expected)
    {
        Assert.Equal(expected, Kv(text).ValueAsBool());
    }

    [Theory]
    [InlineData("")]                    // empty
    [InlineData("1")]                   // not "true"/"false"
    [InlineData("yes")]
    [InlineData("truex")]               // partially valid
    public void ValueAsBool_ThrowsOnMalformed(string text)
    {
        Assert.Throws<KahunaException>(() => Kv(text).ValueAsBool());
    }

    [Fact]
    public void ValueAsBool_ThrowsOnNull()
    {
        Assert.Throws<KahunaException>(() => Kv((byte[]?)null).ValueAsBool());
    }
}
