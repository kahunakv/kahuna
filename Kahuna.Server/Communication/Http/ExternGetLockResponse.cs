
using Kahuna.Locks;
using Kommander.Time;

namespace Kahuna.Communication.Http;

public sealed class ExternGetLockResponse
{
    public string? ServedFrom { get; set; }
    
    public LockResponseType Type { get; set; }
    
    public string? Owner { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public long FencingToken { get; set; }
}