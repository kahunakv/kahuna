
namespace Kahuna;

public sealed class ExternLockResponse
{
    public LockResponseType Type { get; set; }
    
    public long FencingToken { get; set; }
}