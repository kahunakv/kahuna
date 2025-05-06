
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Nixie.Routers;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a request for performing operations on a KeyValue actor
/// </summary>
/// <remarks>
/// The <see cref="KeyValueRequest"/> class is designed to encapsulate all the information
/// required to perform specific key-value operations. It supports operations such as setting,
/// comparing, deleting, and managing locks for keys within the store. This request is constructed
/// with multiple attributes to ensure consistency and control across distributed systems.
/// </remarks>
public sealed class KeyValueRequest : IConsistentHashable
{
    public KeyValueRequestType Type { get; private set; }

    public HLCTimestamp TransactionId { get; private set; }

    public HLCTimestamp CommitId { get; private set; }
    
    public string Key { get; private set; }
    
    public byte[]? Value { get; private set; }
    
    public byte[]? CompareValue { get; private set; }
    
    public long CompareRevision { get; private set; }
    
    public KeyValueFlags Flags { get; private set; }
    
    public int ExpiresMs { get; private set; }
    
    public HLCTimestamp ProposalTicketId { get; private set; }
    
    public KeyValueDurability Durability { get; private set;  }
    
    /// <summary>
    /// 
    /// </summary>
    public int ProposalId { get; private set; }
    
    /// <summary>
    /// 
    /// </summary>
    public int PartitionId { get; private set; }
    
    /// <summary>
    /// 
    /// </summary>
    public TaskCompletionSource<KeyValueResponse?>? Promise { get; private set; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    public KeyValueRequest(
        KeyValueRequestType type,
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key, 
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability,
        int proposalId, 
        int partitionId,
        TaskCompletionSource<KeyValueResponse?>? promise
    )
    {
        Type = type;
        TransactionId = transactionId;
        CommitId = commitId;
        Key = key;
        Value = value;
        CompareValue = compareValue;
        CompareRevision = compareRevision;
        Flags = flags;
        ExpiresMs = expiresMs;
        ProposalTicketId = proposalTicketId;
        Durability = durability;
        ProposalId = proposalId;
        PartitionId = partitionId;
        Promise = promise;
    }
    
    public void Reset(
        KeyValueRequestType type,
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key, 
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability,
        int proposalId, 
        int partitionId,
        TaskCompletionSource<KeyValueResponse?>? promise
    )
    {
        Type = type;
        TransactionId = transactionId;
        CommitId = commitId;
        Key = key;
        Value = value;
        CompareValue = compareValue;
        CompareRevision = compareRevision;
        Flags = flags;
        ExpiresMs = expiresMs;
        ProposalTicketId = proposalTicketId;
        Durability = durability;
        ProposalId = proposalId;
        PartitionId = partitionId;
        Promise = promise;
    }
    
    public void Clear()
    {
        Key = string.Empty;
        Value = null;
        Promise = null;
    }

    public int GetHash()
    {
        if (Type is KeyValueRequestType.GetByBucket or KeyValueRequestType.ScanByPrefix)
            return (int)HashUtils.SimpleHash(Key);
        
        return (int)HashUtils.InversePrefixedStaticHash(Key, '/');
    }
}