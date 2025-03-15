
using Kommander.Time;
using Kahuna.Shared.Locks;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetLockResponse
{
    public string? ServedFrom { get; set; }
    
    public LockResponseType Type { get; set; }
    
    public byte[]? Owner { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public long FencingToken { get; set; }
}