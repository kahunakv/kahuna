
using System.Globalization;
using System.Text;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

public sealed class KeyValueExpressionResult
{
    public KeyValueExpressionType Type { get; }
    
    public bool BoolValue { get; }
    
    public string? StrValue { get; }
    
    public long LongValue { get;  }
    
    public double DoubleValue { get; }
    
    public byte[]? BytesValue { get; set; }
    
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

    public override string ToString()
    {
        return Type switch
        {
            KeyValueExpressionType.NullType => "(null)",
            KeyValueExpressionType.BoolType => BoolValue ? "true" : "false",
            KeyValueExpressionType.LongType => LongValue.ToString(),
            KeyValueExpressionType.DoubleType => DoubleValue.ToString(CultureInfo.InvariantCulture),
            KeyValueExpressionType.StringType => StrValue ?? "(null)",
            KeyValueExpressionType.BytesType => "(bytes)",
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public byte[]? ToBytes()
    {
        return Type switch
        {
            KeyValueExpressionType.NullType => [],
            KeyValueExpressionType.BoolType => BoolValue ? "true"u8.ToArray() : "false"u8.ToArray(),
            KeyValueExpressionType.LongType => Encoding.UTF8.GetBytes(LongValue.ToString()),
            KeyValueExpressionType.DoubleType => Encoding.UTF8.GetBytes(DoubleValue.ToString(CultureInfo.InvariantCulture)),
            KeyValueExpressionType.StringType => StrValue is not null ? Encoding.UTF8.GetBytes(StrValue) : null,
            KeyValueExpressionType.BytesType => BytesValue,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
    
    public long ToLong()
    {
        return Type switch
        {
            KeyValueExpressionType.NullType => 0,
            KeyValueExpressionType.BoolType => BoolValue ? 1 : 0,
            KeyValueExpressionType.LongType => LongValue,
            KeyValueExpressionType.DoubleType => (long)DoubleValue,
            KeyValueExpressionType.StringType => StrValue is not null ? long.Parse(StrValue, NumberStyles.Integer, CultureInfo.InvariantCulture) : -1,
            KeyValueExpressionType.BytesType => BytesValue is not null ? long.Parse(Encoding.UTF8.GetString(BytesValue), NumberStyles.Integer, CultureInfo.InvariantCulture) : -1,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}