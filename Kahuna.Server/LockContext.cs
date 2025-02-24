
using Kommander.Time;

namespace Kahuna;

/// <summary>
/// 
/// </summary>
public sealed class LockContext
{
    public string? Owner { get; set; }
    
    public HLCTimestamp Expires { get; set; }
    
    public long FencingToken { get; set; }
}