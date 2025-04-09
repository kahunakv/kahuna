
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Nixie.Routers;
using Standart.Hash.xxHash;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a request to perform an action on a key-value actor
/// </summary>
public sealed class KeyValueRequest : IConsistentHashable
{
    public KeyValueRequestType Type { get; }
    
    public HLCTimestamp TransactionId { get; }
    
    public HLCTimestamp CommitId { get; }
    
    public string Key { get; }
    
    public byte[]? Value { get; }
    
    public byte[]? CompareValue { get; }
    
    public long CompareRevision { get; }
    
    public KeyValueFlags Flags { get; }
    
    public int ExpiresMs { get; }
    
    public HLCTimestamp ProposalTicketId { get; }
    
    public KeyValueDurability Durability { get; }
    
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
        KeyValueDurability durability
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
    }

    public int GetHash()
    {
        return (int)HashUtils.InversePrefixedStaticHash(Key, '/');
    }
}