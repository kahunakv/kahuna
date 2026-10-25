
using Kommander;
using Kommander.Time;
using Kommander.Diagnostics;

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kahuna.Server.Routing;
using Kahuna.Shared.Routing;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Locates the appropriate leader node for a given key and executes the corresponding key-value operations.
/// The routing core and each family of operations live in their own class; this type is the single entry
/// point the rest of the server calls and forwards each call to the class that owns it.
/// </summary>
internal sealed class KeyValueLocator
{
    private readonly LocatorRouting routing;

    private readonly LocatorWriteOperations writes;

    private readonly LocatorReadOperations reads;

    private readonly LocatorProbeOperations probes;

    private readonly LocatorWriteIntentOperations writeIntents;

    private readonly LocatorLockOperations locks;

    private readonly LocatorMutationTicketOperations tickets;

    private readonly LocatorScanOperations scans;

    private readonly LocatorTransactionOperations transactions;

    public KeyValueLocator(
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        IRaft raft,
        IInterNodeCommunication interNodeCommunication,
        KeySpaceRegistry keySpaceRegistry,
        PartitionDivergenceContainment divergenceContainment,
        ILogger<IKahuna> logger
    )
    {
        routing = new LocatorRouting(manager, configuration, raft, interNodeCommunication, keySpaceRegistry, divergenceContainment, logger);
        writes = new LocatorWriteOperations(routing);
        reads = new LocatorReadOperations(routing);
        writeIntents = new LocatorWriteIntentOperations(routing);
        locks = new LocatorLockOperations(routing);
        tickets = new LocatorMutationTicketOperations(routing);
        scans = new LocatorScanOperations(routing);
        probes = new LocatorProbeOperations(routing, locks, scans);
        transactions = new LocatorTransactionOperations(routing);
    }

    internal SessionRegistrationCounts SessionRegistrationCounts => transactions.SessionRegistrationCounts;

    internal SessionRegistrationForwardDurations SessionRegistrationForwardDurations => transactions.SessionRegistrationForwardDurations;

    public ValueTask<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0) =>
        writes.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);

    public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken) =>
        writes.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken);

    public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken) =>
        writes.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken) =>
        writes.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) =>
        writes.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, KeyValueReadMode readMode = KeyValueReadMode.Default) =>
        reads.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken, readMode);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) =>
        reads.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken) =>
        reads.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValuesUnconfirmed(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken) =>
        reads.LocateAndTryExistsManyValuesUnconfirmed(transactionId, readTimestamp, keys, cancellationToken);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken) =>
        reads.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken);

    public Task<KeyValueProbeResult[]> LocateAndTryProbeMany(HLCTimestamp transactionId, HLCTimestamp readTimestamp, IReadOnlyList<string> keys, KeyValueProbeLock lockMode, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, KeyValueReadMode readMode = KeyValueReadMode.Default) =>
        probes.LocateAndTryProbeMany(transactionId, readTimestamp, keys, lockMode, expiresMs, durability, cancellationToken, readMode);

    public Task<KeyValueProbeRangeResult[]> LocateAndTryProbeManyRanges(HLCTimestamp transactionId, HLCTimestamp readTimestamp, string prefix, IReadOnlyList<KeyValueProbeRange> seeks, KeyValueProbeLock lockMode, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, KeyValueReadMode readMode = KeyValueReadMode.Default) =>
        probes.LocateAndTryProbeManyRanges(transactionId, readTimestamp, prefix, seeks, lockMode, expiresMs, durability, cancellationToken, readMode);

    public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken) =>
        writeIntents.LocateAndTryCheckWriteIntent(transactionId, key, durability, cancellationToken);

    public Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> LocateAndTryCheckManyWriteIntents(HLCTimestamp transactionId, List<KeyValueConflictProbe> keys, CancellationToken cancellationToken) =>
        writeIntents.LocateAndTryCheckManyWriteIntents(transactionId, keys, cancellationToken);

    internal Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntentsConfirmedLocally(HLCTimestamp transactionId, List<KeyValueConflictProbe> keys, CancellationToken cancellationToken) =>
        writeIntents.TryCheckManyWriteIntentsConfirmedLocally(transactionId, keys, cancellationToken);

    public Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> LocateAndTryCheckManyWriteIntentsUnconfirmed(HLCTimestamp transactionId, List<KeyValueConflictProbe> keys, CancellationToken cancellationToken) =>
        writeIntents.LocateAndTryCheckManyWriteIntentsUnconfirmed(transactionId, keys, cancellationToken);

    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId, long BaseRevision)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken) =>
        locks.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancelationToken);

    public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) =>
        locks.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId, long BaseRevision)>> LocateAndTryAcquireManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancelationToken) =>
        locks.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancelationToken);

    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken) =>
        locks.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken);

    public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken) =>
        locks.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken) =>
        locks.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) =>
        locks.LocateAndTryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, cancellationToken);

    internal Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, Func<Task>? afterSnapshot, CancellationToken cancellationToken) =>
        locks.LocateAndTryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, afterSnapshot, cancellationToken);

    internal Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, Func<Task>? afterSnapshot, CancellationToken cancellationToken) =>
        locks.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, afterSnapshot, cancellationToken);

    public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken) =>
        locks.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancelationToken) =>
        locks.LocateAndTryReleaseManyExclusiveLocks(transactionId, keys, cancelationToken);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, CancellationToken cancelationToken, long routedGeneration = 0, string? recordAnchorKey = null) =>
        tickets.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancelationToken, routedGeneration, recordAnchorKey);

    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancelationToken, string? recordAnchorKey = null) =>
        tickets.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancelationToken, recordAnchorKey);

    public Task<bool> IsLocallyLedHashKey(string key, CancellationToken cancellationToken) =>
        tickets.IsLocallyLedHashKey(key, cancellationToken);

    public Task<(KeyValueResponseType, Handlers.KeyValueFinalizeStage)?> TryFinalizeMutationIfLocal(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, string? recordAnchorKey, CancellationToken cancellationToken) =>
        tickets.TryFinalizeMutationIfLocal(transactionId, commitId, key, durability, recordAnchorKey, cancellationToken);

    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken) =>
        tickets.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancelationToken);

    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancelationToken) =>
        tickets.LocateAndTryCommitManyMutations(transactionId, keys, cancelationToken);

    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken) =>
        tickets.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancelationToken);

    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancelationToken) =>
        tickets.LocateAndTryRollbackManyMutations(transactionId, keys, cancelationToken);

    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) =>
        scans.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);

    internal Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor, CancellationToken cancellationToken) =>
        scans.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, beforeQuery, afterDescriptor, cancellationToken);

    public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, bool snapshotAtLeader = false, KeyValueReadMode readMode = KeyValueReadMode.Default) =>
        scans.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, snapshotAtLeader, readMode);

    public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) =>
        scans.ScanAllByPrefix(prefixKeyName, readTimestamp, durability, cancellationToken);

    public Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken) =>
        transactions.LocateAndStartTransaction(options, cancellationToken);

    public Task<TransactionCommitAnswer> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken) =>
        transactions.LocateAndCommitTransaction(handle, cancellationToken);

    public Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken) =>
        transactions.LocateAndRollbackTransaction(handle, cancellationToken);

    public ValueTask<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken) =>
        transactions.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);

    public int LocatePartition(string coordinatorKey) =>
        transactions.LocatePartition(coordinatorKey);

    internal void CountInboundCompletionServedLocally() =>
        transactions.CountInboundCompletionServedLocally();

    internal SessionRegistrationForward BeginInboundCompletionRedirect() =>
        transactions.BeginInboundCompletionRedirect();

    public ValueTask<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken) =>
        transactions.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);

    public Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken) =>
        transactions.LocateAndGetTransactionWorkingSet(coordinatorKey, transactionId, cancellationToken);

    public Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken) =>
        transactions.LocateAndCloseTransaction(coordinatorKey, transactionId, cancellationToken);

    public (int PartitionId, long Generation) LocateRange(string key) =>
        routing.LocateRange(key);

    /// <summary>Routes a key and reports its routing mode and, for a key-range key, the descriptor it resolved
    /// through, in one classification pass (see <see cref="LocatorRouting.LocateRangeWithMode"/>).</summary>
    internal (int PartitionId, long Generation, bool IsKeyRange, Ranges.RangeDescriptor? Descriptor) LocateRangeWithMode(string key) =>
        routing.LocateRangeWithMode(key);

    /// <summary>True when <paramref name="key"/> lies in the quiesced part of a range whose data is being moved
    /// (see <see cref="LocatorRouting.IsKeyQuiesced"/>).</summary>
    internal bool IsKeyQuiesced(string key) =>
        routing.IsKeyQuiesced(key);

    public bool TryLocateRange(string key, out int partitionId) =>
        routing.TryLocateRange(key, out partitionId);

    internal int RouteKey(string key) =>
        routing.RouteKey(key);

    internal ValueTask<KeyValueResponseType> CheckLeaderTerm(string key, long term, CancellationToken cancellationToken) =>
        routing.CheckLeaderTerm(key, term, cancellationToken);
}
