
namespace Kahuna;

public sealed class ExternGetLockResponse
{
    public LockResponseType Type { get; set; }
    
    public string? Owner { get; set; }
    
    public DateTime Expires { get; set; }
    
    public long FencingToken { get; set; }
}