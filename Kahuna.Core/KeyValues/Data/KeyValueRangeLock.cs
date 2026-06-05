
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueRangeLock
{
    public HLCTimestamp TransactionId { get; set; }
    public HLCTimestamp Expires       { get; set; }
    public string?      StartKey      { get; set; }
    public bool         StartInclusive { get; set; }
    public string?      EndKey        { get; set; }
    public bool         EndInclusive  { get; set; }
}
