
using Kommander;
using Kommander.Time;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.Communication.Internode;

namespace Kahuna;

/// <summary>
/// Internal accessors and hook-carrying variants used by the in-process tests to drive and
/// inspect state that is not part of the production API surface.
/// </summary>
public sealed partial class KahunaManager
{
    /// <summary>The node's placement projection, for consumers and tests that need hosted-set answers.</summary>
    internal PartitionPlacementView PlacementView => placement.View;

    /// <summary>The lock subsystem, for in-process tests that drive internal maintenance paths.</summary>
    internal LockManager Locks => locks;

    /// <summary>
    /// Exposes the persistence backend for PITR bootstrap tests that need to extract a
    /// checkpoint from an already-running node before seeding a joining peer.
    /// </summary>
    internal IPersistenceBackend PersistenceBackend => persistenceBackend;

    /// <summary>
    /// Staged-base variant of <c>LocateAndTryExistsManyValues</c> used by the commit-time write-side
    /// compare-and-set; see <see cref="Server.KeyValues.KeyValueLocator.LocateAndTryExistsManyValuesUnconfirmed"/>
    /// for the leadership contract restricting its callers. Diagnostic/test access.
    /// </summary>
    internal Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValuesUnconfirmed(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    ) => keyValues.LocateAndTryExistsManyValuesUnconfirmed(transactionId, readTimestamp, keys, cancellationToken);

    /// <summary>Node-local persistent-participant completion receipts. Diagnostic/test access.</summary>
    internal CompletionReceiptStore CompletionReceiptStore => keyValues.CompletionReceiptStore;

    /// <summary>Durable prepared-intent store. Diagnostic/test access.</summary>
    internal Server.KeyValues.Transactions.PreparedIntentStore DurablePreparedIntentStore => keyValues.DurablePreparedIntentStore;

    /// <summary>The interactive-transaction coordinator, for tests that install finalizer interleaving hooks.</summary>
    internal Server.KeyValues.Transactions.TransactionCoordinator TransactionCoordinator => keyValues.Coordinator;

    /// <summary>Durable transaction-record store (canonical decisions). Diagnostic/test access.</summary>
    internal Server.KeyValues.Transactions.TransactionRecordStore DurableTransactionRecordStore => keyValues.DurableTransactionRecordStore;

    internal Task RunCollectOnAllInstancesAsync() => keyValues.RunCollectOnAllInstancesAsync();

    /// <summary>
    /// Direct access to the <see cref="BackgroundWriterActor"/> instance for test injection
    /// (e.g., setting <see cref="BackgroundWriterActor.BeforePruneSampleHook"/>) and for the
    /// backup applied-index probe. Nixie instantiates the actor during <c>Spawn</c>, so this is
    /// populated as soon as the manager's constructor has spawned the writer; the nullability comes
    /// from the runner's own contract, not from lazy instantiation.
    /// </summary>
    internal BackgroundWriterActor? BackgroundWriterActor =>
        backgroundWriter.Runner.Actor as BackgroundWriterActor;

    /// <summary>
    /// Exposes the KeyValuesManager for in-process test inspection of accounting state.
    /// Not part of the production API surface.
    /// </summary>
    internal KeyValuesManager KeyValues => keyValues;

    /// <summary>Test-only: Raft instance for HLC timestamp generation.</summary>
    internal IRaft Raft => keyValues.Raft;

    /// <summary>Test-only: inter-node transport for fault/latency injection (cast assumes memory transport).</summary>
    internal MemoryInterNodeCommmunication GetInterNodeCommunication() =>
        (MemoryInterNodeCommmunication)keyValues.InterNodeCommunication;

    /// <summary>
    /// Issues a persistent key-range write on the <b>local</b> node carrying an explicit routed
    /// generation (descriptor fence). Must be called on the descriptor partition's leader. Lets tests
    /// inject a stale generation; production routes through the locator which captures the live one.
    /// </summary>
    internal Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValueRanged(
        HLCTimestamp transactionId, string key, byte[]? value, long routedGeneration) =>
        keyValues.TrySetKeyValue(transactionId, key, value, null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, routedGeneration);

    /// <summary>
    /// Test seam: forces a split of the descriptor covering <paramref name="splitKey"/> at that
    /// exact key without requiring a pre-computed partition ID or threshold-sized data.
    /// Handles <c>ComputeNextPartitionId → CreatePartitionAsync → SplitAsync</c> internally.
    /// Pass <paramref name="duringQuiesce"/> to race an operation into the quiesce window.
    /// </summary>
    internal Task<SplitOutcome> ForceSplitAtKeyAsync(
        string keySpace,
        string splitKey,
        Func<Task>? duringQuiesce = null,
        CancellationToken ct = default) =>
        keyValues.ForceSplitAtKeyAsync(keySpace, splitKey, duringQuiesce, ct);

    /// <summary>
    /// Test seam for the multi-range bucket fan-out: <paramref name="beforeQuery"/> is called before each descriptor's paged query
    /// starts; <paramref name="afterDescriptor"/> is called after each descriptor's pages are fully
    /// collected. Used by <c>Bucket_FanOut_IsParallelAndLeaderCoalesced</c> (gate-based concurrency
    /// proof) and <c>Bucket_SplitMidScan_NoDupNoMissing</c> (mid-fan-out split injection).
    /// </summary>
    internal Task<KeyValueGetByBucketResult> LocateAndGetByBucketWithHooks(
        HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability,
        Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken) =>
        keyValues.LocateAndGetByBucketWithHooks(
            transactionId, prefixedKey, durability, beforeQuery, afterDescriptor, cancellationToken);

    /// <summary>
    /// Test seam: acquires a range lock with a callback invoked after the initial
    /// <c>FindIntersecting</c> snapshot but before any sub-lock RPC. Lets tests inject a split
    /// into that window to drive the generation fence deterministically.
    /// </summary>
    internal Task<(KeyValueResponseType, HLCTimestamp)> AcquireExclusiveRangeLockWithHook(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        Func<Task> afterSnapshot,
        CancellationToken cancellationToken
    ) => keyValues.LocateAndTryAcquireExclusiveRangeLockWithHook(
            transactionId, prefix, startKey, startInclusive, endKey, endInclusive,
            expiresMs, durability, afterSnapshot, cancellationToken);

    /// <summary>
    /// Test seam: the register-remote range-lock acquire with a split-injection hook, so a test can
    /// assert how the generation fence interacts with the coordinator-owned working set (a fenced acquire
    /// must record no range descriptor).
    /// </summary>
    internal Task<(KeyValueResponseType, HLCTimestamp)> RegisterAndAcquireRangeLockWithHook(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs,
        KeyValueDurability durability, RangeLockMode mode, Func<Task> afterSnapshot, CancellationToken cancellationToken
    ) => keyValues.RegisterAndAcquireRangeLockWithHook(
            transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive,
            expiresMs, durability, mode, afterSnapshot, cancellationToken);
}
