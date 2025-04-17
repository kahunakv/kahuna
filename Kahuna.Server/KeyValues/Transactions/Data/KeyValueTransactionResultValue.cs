
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

public sealed class KeyValueTransactionResultValue
{
    public string? Key { get; set; }
    
    public byte[]? Value { get; set; }
    
    public long Revision { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public HLCTimestamp LastModified { get; set; }
}