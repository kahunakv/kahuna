
using System.Buffers.Text;
using System.Globalization;
using System.Text;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

public sealed class KeyValueExpressionResult
{
    private static readonly byte[] TrueBytes  = [.. "true"u8];
    private static readonly byte[] FalseBytes = [.. "false"u8];

    public KeyValueExpressionType Type { get; }

    public bool BoolValue { get; }

    public string? StrValue { get; }

    public long LongValue { get;  }

    public double DoubleValue { get; }

    public byte[]? BytesValue { get; set; }

    public List<KeyValueExpressionResult>? ArrayValue { get; }

    public long Revision { get; }

    public long Expires { get; }

    public KeyValueExpressionResult(KeyValueExpressionType type)
    {
        Type = type;
        Revision = -1;
        Expires = 0;
    }

    public KeyValueExpressionResult(bool boolValue, long revision = -1, long expires = 0)
    {
        Type = KeyValueExpressionType.BoolType;
        BoolValue = boolValue;
        Revision = revision;
        Expires = expires;
    }

    public KeyValueExpressionResult(long longValue, long revision = -1, long expires = 0)
    {
        Type = KeyValueExpressionType.LongType;
        LongValue = longValue;
        Revision = revision;
        Expires = expires;
    }

    public KeyValueExpressionResult(double doubleValue, long revision = -1, long expires = 0)
    {
        Type = KeyValueExpressionType.DoubleType;
        DoubleValue = doubleValue;
        Revision = revision;
        Expires = expires;
    }

    public KeyValueExpressionResult(string? strValue, long revision = -1, long expires = 0)
    {
        Type = KeyValueExpressionType.StringType;
        StrValue = strValue;
        Revision = revision;
        Expires = expires;
    }

    public KeyValueExpressionResult(List<KeyValueExpressionResult>? arrayValue)
    {
        Type = KeyValueExpressionType.ArrayType;
        ArrayValue = arrayValue;
    }

    public override string ToString()
    {
        return Type switch
        {
            KeyValueExpressionType.NullType   => "(null)",
            KeyValueExpressionType.BoolType   => BoolValue ? "true" : "false",
            KeyValueExpressionType.LongType   => LongValue.ToString(),
            KeyValueExpressionType.DoubleType => DoubleValue.ToString(CultureInfo.InvariantCulture),
            KeyValueExpressionType.StringType => StrValue ?? "(null)",
            KeyValueExpressionType.BytesType  => "(bytes)",
            KeyValueExpressionType.ArrayType  => throw new InvalidOperationException("Cannot convert array to bytes"),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public byte[]? ToBytes()
    {
        return Type switch
        {
            KeyValueExpressionType.NullType   => [],
            KeyValueExpressionType.BoolType   => BoolValue ? TrueBytes : FalseBytes,
            KeyValueExpressionType.LongType   => EncodeLong(LongValue),
            KeyValueExpressionType.DoubleType => EncodeDouble(DoubleValue),
            KeyValueExpressionType.StringType => StrValue is not null ? Encoding.UTF8.GetBytes(StrValue) : null,
            KeyValueExpressionType.BytesType  => BytesValue,
            KeyValueExpressionType.ArrayType  => throw new InvalidOperationException("Cannot convert array to bytes"),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public long ToLong()
    {
        return Type switch
        {
            KeyValueExpressionType.NullType   => 0,
            KeyValueExpressionType.BoolType   => BoolValue ? 1 : 0,
            KeyValueExpressionType.LongType   => LongValue,
            KeyValueExpressionType.DoubleType => (long)DoubleValue,
            KeyValueExpressionType.StringType => StrValue is not null ? long.Parse(StrValue, NumberStyles.Integer, CultureInfo.InvariantCulture) : -1,
            KeyValueExpressionType.BytesType  => BytesValue is not null ? ParseLongFromBytes(BytesValue) : -1,
            KeyValueExpressionType.ArrayType  => throw new InvalidOperationException("Cannot convert array to bytes"),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public KeyValueTransactionResult ToTransactionResult()
    {
        return Type switch
        {
            KeyValueExpressionType.NullType => new()
            {
                Type = KeyValueResponseType.Get,
                Values = [new() { Value = [], Revision = Revision, Expires = new(0, Expires, 0) }]
            },
            KeyValueExpressionType.BoolType => new()
            {
                Type = KeyValueResponseType.Get,
                Values = [new() { Value = BoolValue ? TrueBytes : FalseBytes, Revision = Revision, Expires = new(0, Expires, 0) }]
            },
            KeyValueExpressionType.LongType => new()
            {
                Type = KeyValueResponseType.Get,
                Values = [new() { Value = EncodeLong(LongValue), Revision = Revision, Expires = new(0, Expires, 0) }]
            },
            KeyValueExpressionType.DoubleType => new()
            {
                Type = KeyValueResponseType.Get,
                Values = [new() { Value = EncodeDouble(DoubleValue), Revision = Revision, Expires = new(0, Expires, 0) }]
            },
            KeyValueExpressionType.StringType => new()
            {
                Type = KeyValueResponseType.Get,
                Values = [new() { Value = StrValue is not null ? Encoding.UTF8.GetBytes(StrValue) : null, Revision = Revision, Expires = new(0, Expires, 0) }]
            },
            KeyValueExpressionType.BytesType => new()
            {
                Type = KeyValueResponseType.Get,
                Values = [new() { Value = BytesValue, Revision = Revision, Expires = new(0, Expires, 0) }]
            },
            KeyValueExpressionType.ArrayType => new()
            {
                Type = KeyValueResponseType.Get,
                Values = GetArrayValues(ArrayValue)
            },
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    private static KeyValueTransactionResultValue ToTransactionResultValue(KeyValueExpressionResult value)
    {
        return value.Type switch
        {
            KeyValueExpressionType.NullType   => new() { Value = [], Revision = value.Revision, Expires = new(0, value.Expires, 0) },
            KeyValueExpressionType.BoolType   => new() { Value = value.BoolValue ? TrueBytes : FalseBytes, Revision = value.Revision, Expires = new(0, value.Expires, 0) },
            KeyValueExpressionType.LongType   => new() { Value = EncodeLong(value.LongValue), Revision = value.Revision, Expires = new(0, value.Expires, 0) },
            KeyValueExpressionType.DoubleType => new() { Value = EncodeDouble(value.DoubleValue), Revision = value.Revision, Expires = new(0, value.Expires, 0) },
            KeyValueExpressionType.StringType => new() { Value = value.StrValue is not null ? Encoding.UTF8.GetBytes(value.StrValue) : null, Revision = value.Revision, Expires = new(0, value.Expires, 0) },
            KeyValueExpressionType.BytesType  => new() { Value = value.BytesValue, Revision = value.Revision, Expires = new(0, value.Expires, 0) },
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    private static List<KeyValueTransactionResultValue> GetArrayValues(List<KeyValueExpressionResult>? arrayValue)
    {
        if (arrayValue is null)
            return [];

        List<KeyValueTransactionResultValue> values = new(arrayValue.Count);

        foreach (KeyValueExpressionResult item in arrayValue)
            values.Add(ToTransactionResultValue(item));

        return values;
    }

    private static byte[] EncodeLong(long value)
    {
        Span<byte> buf = stackalloc byte[32];
        Utf8Formatter.TryFormat(value, buf, out int written);
        return buf[..written].ToArray();
    }

    private static byte[] EncodeDouble(double value)
    {
        Span<byte> buf = stackalloc byte[32];
        value.TryFormat(buf, out int written, default, CultureInfo.InvariantCulture);
        return buf[..written].ToArray();
    }

    private static long ParseLongFromBytes(byte[] bytes)
    {
        if (Utf8Parser.TryParse(bytes, out long value, out int consumed) && consumed == bytes.Length)
            return value;
        return long.Parse(Encoding.UTF8.GetString(bytes), NumberStyles.Integer, CultureInfo.InvariantCulture);
    }
}
