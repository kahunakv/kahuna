
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

public class KahunaKeyValueTransactionResultValue
{
    public string? Key { get; set; }
    
    public byte[]? Value { get; set; }
    
    public long Revision { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public HLCTimestamp LastModified { get; set; }
}

public class KahunaKeyValueTransactionResult
{
    public string? ServedFrom { get; set; }
    
    public KeyValueResponseType Type { get; set; }
       
    public List<KahunaKeyValueTransactionResultValue>? Values { get; set; }
    
    public long FirstRevision
    {
        get
        {
            if (Values is null || Values.Count == 0)
                return 0;

            return Values[0].Revision;
        }
    }
    
    public byte[]? FirstValue
    {
        get
        {
            if (Values is null || Values.Count == 0)
                return null;

            return Values[0].Value;
        }
    }
}