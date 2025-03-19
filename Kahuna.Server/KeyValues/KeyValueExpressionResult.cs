
using System.Globalization;
using System.Text;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

public enum KeyValueExpressionType
{
    Null,
    Bool,
    Long,
    Double,
    String,
    Bytes,
}

public class KeyValueExpressionResult
{
    public KeyValueExpressionType Type { get; set; }
    
    public bool BoolValue { get; set; }
    
    public string? StrValue { get; set; }
    
    public long LongValue { get; set; }
    
    public double DoubleValue { get; set; }
    
    public byte[]? BytesValue { get; set; }
    
    public long Revision { get; set; }

    public KeyValueTransactionResult ToTransactionRsult()
    {
        return Type switch
        {
            KeyValueExpressionType.Null => new() { Type = KeyValueResponseType.Get, Value = null },
            KeyValueExpressionType.Bool => new() { Type = KeyValueResponseType.Get, Value = BoolValue ? "true"u8.ToArray() : "false"u8.ToArray() },
            KeyValueExpressionType.Long => new() { Type = KeyValueResponseType.Get, Value = Encoding.UTF8.GetBytes(LongValue.ToString()) },
            KeyValueExpressionType.Double => new() { Type = KeyValueResponseType.Get, Value = Encoding.UTF8.GetBytes(DoubleValue.ToString(CultureInfo.InvariantCulture)) },
            KeyValueExpressionType.String => new() { Type = KeyValueResponseType.Get, Value = StrValue is not null ? Encoding.UTF8.GetBytes(StrValue) : null },
            KeyValueExpressionType.Bytes => new() { Type = KeyValueResponseType.Get, Value = BytesValue },
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}