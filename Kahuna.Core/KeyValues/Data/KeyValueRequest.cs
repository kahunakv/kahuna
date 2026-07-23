
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
    
    /// <summary>Range scan: inclusive lower bound key (null = start of prefix).</summary>
    public string? StartKey { get; internal set; }

    /// <summary>Range scan: whether the lower bound is inclusive.</summary>
    public bool StartInclusive { get; internal set; }

    /// <summary>Range scan: inclusive/exclusive upper bound key (null = end of prefix).</summary>
    public string? EndKey { get; internal set; }

    /// <summary>Range scan: whether the upper bound is inclusive.</summary>
    public bool EndInclusive { get; internal set; }

    /// <summary>Mode for range-lock acquire requests (Exclusive = 0 default, Shared = 1).</summary>
    public RangeLockMode RangeLockMode { get; internal set; }

    /// <summary>Lock entries to inject into LocksByRange for ImportRangeLocks requests.</summary>
    public List<KeyValueRangeLock>? RangeLockImportList { get; internal set; }

    /// <summary>Range scan: maximum items to return per page.</summary>
    public int Limit { get; internal set; }

    /// <summary>Range scan: fixed snapshot timestamp for consistent paging (Zero = latest).</summary>
    public HLCTimestamp ReadTimestamp { get; internal set; }

    /// <summary>Optional: a foreign prepared intent's canonical decision, resolved off-mailbox by routing to the
    /// transaction's anchor-partition leader, so a read that meets a still-pending remote-anchor intent resolves
    /// the outcome instead of retrying until settlement. Default (Zero identity) means no hint.</summary>
    internal Transactions.Data.ForeignDecisionHint ForeignDecisionHint { get; set; }

    /// <summary>Optional: canonical decisions for the still-pending foreign intents a scan page meets, keyed by
    /// intent identity <c>(TransactionId, Epoch)</c>. A scan page can straddle many foreign intents with different
    /// remote anchors, so — unlike a point read's single <see cref="ForeignDecisionHint"/> — the routed decisions
    /// are resolved off-mailbox as a set and the scan is re-issued with the whole map, letting the overlay resolve
    /// each committed-but-unsettled intent instead of retrying the page until settlement. Null means no routed
    /// decisions.</summary>
    internal IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), Transactions.Data.TransactionDecision>? ForeignScanDecisions { get; set; }

    /// <summary>
    /// Key-range routing generation the request was resolved on (descriptor fence). 0 = hash space / not
    /// range-routed. Set by the locator for KeyRange write ops; threaded into the proposal so the
    /// replication chokepoint can reject a stale-generation write with <c>MustRetry</c>.
    /// </summary>
    public long RoutedGeneration { get; internal set; }

    /// <summary>
    /// Transaction record anchor carried on a prepare request so the participant leader can stamp it into
    /// the write intent (and, later, the durable completion receipt). Null for non-prepare requests and
    /// for transactions with no confirmed persistent write.
    /// </summary>
    public string? RecordAnchorKey { get; internal set; }


    /// <summary>
    ///
    /// </summary>
    public TaskCompletionSource<KeyValueResponse?>? Promise { get; private set; }

    /// <summary>
    /// Stage-3 continuation for a resumable backend read (ResumeRead messages only).
    /// Carries the per-type reconciliation logic and the caller's Promise; null for all
    /// other request types.
    /// </summary>
    internal Handlers.ReadContinuation? Continuation { get; set; }

    /// <summary>
    /// Structured payload for <c>InvalidateOrApply</c> messages. Non-null only when
    /// <see cref="Type"/> is <see cref="KeyValueRequestType.InvalidateOrApply"/>.
    /// </summary>
    internal InvalidateOrApplyData? InvalidateOrApplyData { get; private set; }

    /// <summary>
    /// Constructs an <c>InvalidateOrApply</c> message with named fields carried in
    /// <see cref="InvalidateOrApplyData"/> rather than punned into general-purpose fields.
    /// </summary>
    internal static KeyValueRequest ForInvalidateOrApply(
        string key, long revision, byte[]? value,
        HLCTimestamp expires, HLCTimestamp lastUsed, HLCTimestamp lastModified, KeyValueState state,
        bool forceResident = false, HLCTimestamp transactionId = default, int partitionId = 0, bool noRevision = false,
        bool isRollback = false)
    {
        KeyValueRequest req = new(
            KeyValueRequestType.InvalidateOrApply,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            null);
        req.InvalidateOrApplyData = new(revision, value, expires, lastUsed, lastModified, state, forceResident, transactionId, partitionId, noRevision, isRollback);
        return req;
    }

    /// <summary>
    /// Constructs a <c>FlushAck</c> message: the background writer notifying the owning actor that
    /// <paramref name="key"/> has been durably stored up to <paramref name="revision"/>. The revision
    /// travels in <see cref="CompareRevision"/> (unused by this message type); routing keys on
    /// <paramref name="key"/> so it reaches the same actor that holds the entry.
    /// </summary>
    internal static KeyValueRequest ForFlushAck(string key, long revision)
        => new(
            KeyValueRequestType.FlushAck,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            revision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            null);

    /// <summary>
    /// Creates a type-only request (e.g. periodic cache collection).
    /// </summary>
    public KeyValueRequest(KeyValueRequestType type)
        : this(
            type,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            string.Empty,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            null
        )
    {
    }

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
        CompareValue = null;
        Promise = null;
        StartKey = null;
        EndKey = null;
        StartInclusive = false;
        EndInclusive = false;
        RangeLockMode = RangeLockMode.Exclusive;
        RangeLockImportList = null;
        Limit = 0;
        ReadTimestamp = HLCTimestamp.Zero;
        ForeignDecisionHint = default;
        ForeignScanDecisions = null;
        Continuation = null;
        InvalidateOrApplyData = null;
        RoutedGeneration = 0;
        RecordAnchorKey = null;
    }

    public int GetHash()
    {
        if (Type is KeyValueRequestType.GetByBucket
            or KeyValueRequestType.GetByRange
            or KeyValueRequestType.ScanByPrefix
            or KeyValueRequestType.TryAcquireExclusivePrefixLock
            or KeyValueRequestType.TryReleaseExclusivePrefixLock
            or KeyValueRequestType.TryAcquireExclusiveRangeLock
            or KeyValueRequestType.TryReleaseExclusiveRangeLock
            or KeyValueRequestType.GetRangeLocks
            or KeyValueRequestType.ImportRangeLocks)
            return (int)HashUtils.SimpleHash(Key);

        return (int)HashUtils.InversePrefixedStaticHash(Key, '/');
    }
}