
using System.Text;
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValueTransactionResult
{
    public string? ServedFrom { get; set; }
    
    public KeyValueResponseType Type { get; set; }
    
    public byte[]? Value { get; set; }
    
    public long Revision { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public string? Reason { get; set; } 

    public KeyValueExpressionResult ToExpressionResult()
    {
        return new KeyValueExpressionResult()
        {
            Type = KeyValueExpressionType.String,
            StrValue = Value is not null ? Encoding.UTF8.GetString(Value) : null,
        };
    }
}