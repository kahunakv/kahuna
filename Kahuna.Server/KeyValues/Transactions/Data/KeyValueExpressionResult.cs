
using System.Globalization;
using System.Text;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

public class KeyValueExpressionResult
{
    public KeyValueExpressionType Type { get; set; }
    
    public bool BoolValue { get; set; }
    
    public string? StrValue { get; set; }
    
    public long LongValue { get; set; }
    
    public double DoubleValue { get; set; }
    
    public byte[]? BytesValue { get; set; }
    
    public long Revision { get; set; }

    public KeyValueTransactionResult ToTransactionResult()
    {
        return Type switch
        {
            KeyValueExpressionType.NullType => new() { Type = KeyValueResponseType.Get, Value = null },
            KeyValueExpressionType.BoolType => new() { Type = KeyValueResponseType.Get, Value = BoolValue ? "true"u8.ToArray() : "false"u8.ToArray() },
            KeyValueExpressionType.LongType => new() { Type = KeyValueResponseType.Get, Value = Encoding.UTF8.GetBytes(LongValue.ToString()) },
            KeyValueExpressionType.DoubleType => new() { Type = KeyValueResponseType.Get, Value = Encoding.UTF8.GetBytes(DoubleValue.ToString(CultureInfo.InvariantCulture)) },
            KeyValueExpressionType.StringType => new() { Type = KeyValueResponseType.Get, Value = StrValue is not null ? Encoding.UTF8.GetBytes(StrValue) : null },
            KeyValueExpressionType.BytesType => new() { Type = KeyValueResponseType.Get, Value = BytesValue },
            _ => throw new ArgumentOutOfRangeException()
        };
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
            KeyValueExpressionType.NullType => null,
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
            KeyValueExpressionType.NullType => -1,
            KeyValueExpressionType.BoolType => -1,
            KeyValueExpressionType.LongType => LongValue,
            KeyValueExpressionType.DoubleType => -1,
            KeyValueExpressionType.StringType => StrValue is not null ? long.Parse(StrValue, NumberStyles.Integer, CultureInfo.InvariantCulture) : -1,
            KeyValueExpressionType.BytesType => BytesValue is not null ? long.Parse(Encoding.UTF8.GetString(BytesValue), NumberStyles.Integer, CultureInfo.InvariantCulture) : -1,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}