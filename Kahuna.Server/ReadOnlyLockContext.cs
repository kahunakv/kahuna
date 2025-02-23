
namespace Kahuna;

public sealed class ReadOnlyLockContext
{
    public string? Owner { get; }
    
    public DateTime Expires { get; }
    
    public long FencingToken { get; }
    
    public ReadOnlyLockContext(string? owner, DateTime expires, long fencingToken)
    {
        Owner = owner;
        Expires = expires;
        FencingToken = fencingToken;
    }
}