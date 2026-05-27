
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueWriteIntent
{
    public HLCTimestamp TransactionId { get; set; }
    
    public HLCTimestamp Expires { get; set; }
}