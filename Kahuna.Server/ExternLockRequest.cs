
namespace Kahuna;

public sealed class ExternLockRequest
{
    public string? LockName { get; set; }
    
    public string? LockId { get; set; }
    
    public int ExpiresMs { get; set; }
}