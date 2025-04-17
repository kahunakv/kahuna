
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

public sealed class KeyValueTransactionResultValue
{
    public string? Key { get; set; }
    
    public byte[]? Value { get; set; }
    
    public long Revision { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public HLCTimestamp LastModified { get; set; }
}

public sealed class KeyValueTransactionResult
{
    public string? ServedFrom { get; set; }
    
    public KeyValueResponseType Type { get; set; }
    
    public List<KeyValueTransactionResultValue>? Values { get; set; }
    
    public string? Reason { get; set; }

    public long Revision
    {
        get
        {
            if (Values == null || Values.Count == 0)            
                return 0;

            return Values[0].Revision;
        }
    }
    
    public byte[]? Value
    {
        get
        {
            if (Values == null || Values.Count == 0)            
                return null;

            return Values[0].Value;
        }
    }
}