
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

public sealed class KeyValueTransactionResult
{
    public string? ServedFrom { get; set; }
    
    public KeyValueResponseType Type { get; set; }
    
    public List<KeyValueTransactionResultValue>? Values { get; set; }
    
    public string? Reason { get; set; }

    /// <summary>
    /// True when an <see cref="KeyValueResponseType.Aborted"/> result refused the commit because a lock or a
    /// staging the transaction relied on was dropped by a partition leader change, not because of a conflict
    /// with another transaction (<see cref="TransactionAbortClass.LostExclusion"/>). An interactive session
    /// still has to start over: its reads were made under the lost exclusion and the leadership term will not
    /// return. A self-contained script is run again as a new transaction, so the executor answers it as
    /// <see cref="KeyValueResponseType.MustRetry"/>.
    /// </summary>
    internal bool ExclusionLost { get; init; }

    public long Revision
    {
        get
        {
            if (Values == null || Values.Count == 0)            
                return 0;

            return Values[0].Revision;
        }
    }
    
    public byte[]? Value
    {
        get
        {
            if (Values == null || Values.Count == 0)            
                return null;

            return Values[0].Value;
        }
    }
}