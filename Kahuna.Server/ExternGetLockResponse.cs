
using Kommander.Time;

namespace Kahuna;

public sealed class ExternGetLockResponse
{
    public LockResponseType Type { get; set; }
    
    public string? Owner { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public long FencingToken { get; set; }
}