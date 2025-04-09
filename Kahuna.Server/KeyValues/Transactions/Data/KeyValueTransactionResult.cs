
using System.Text;
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

public sealed class KeyValueTransactionResult
{
    public string? ServedFrom { get; set; }
    
    public KeyValueResponseType Type { get; set; }
    
    public byte[]? Value { get; set; }
    
    public long Revision { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public HLCTimestamp LastModified { get; set; }
    
    public string? Reason { get; set; } 

    public KeyValueExpressionResult ToExpressionResult()
    {
        return new()
        {
            Type = KeyValueExpressionType.StringType,
            StrValue = Value is not null ? Encoding.UTF8.GetString(Value) : null,
        };
    }
}