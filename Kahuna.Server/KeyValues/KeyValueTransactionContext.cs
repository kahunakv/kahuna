
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValueTransactionContext
{
    public HLCTimestamp TransactionId { get; set; }
    
    public KeyValueTransactionResult? Result { get; set; }

    public KeyValueTransactionAction Action { get; set; }
    
    public KeyValueExecutionStatus Status { get; set; } = KeyValueExecutionStatus.Continue;
    
    public List<string>? LocksAcquired { get; set; }
}