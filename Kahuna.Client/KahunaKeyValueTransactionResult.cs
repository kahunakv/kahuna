
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

public class KahunaKeyValueTransactionResult
{
    public string? ServedFrom { get; set; }
    
    public KeyValueResponseType Type { get; set; }
    
    public byte[]? Value { get; set; }
    
    public long Revision { get; set; }
    
    public HLCTimestamp Expires { get; set; }
}