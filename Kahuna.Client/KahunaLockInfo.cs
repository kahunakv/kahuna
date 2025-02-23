
namespace Kahuna.Client;

public sealed class KahunaLockInfo
{
    public string Owner { get; }
    
    public DateTime Expires { get; }
    
    public long FencingToken { get; }
    
    public KahunaLockInfo(string owner, DateTime expires, long fencingToken)
    {
        Owner = owner;
        Expires = expires;
        FencingToken = fencingToken;
    }
}