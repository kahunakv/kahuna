
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueReplicationIntent
{
    public int ProposalId { get; set; }
    
    public HLCTimestamp Expires { get; set; }
}