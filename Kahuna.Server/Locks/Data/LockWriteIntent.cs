
using Kommander.Time;

namespace Kahuna.Server.Locks.Data;

internal sealed class LockWriteIntent
{
    public int ProposalId { get; set; }
    
    public HLCTimestamp Expires { get; set; }
}