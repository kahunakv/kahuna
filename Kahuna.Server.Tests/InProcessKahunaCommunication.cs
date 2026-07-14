
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Implements IKahunaCommunication by calling directly into an IKahuna instance.
/// Covers only the methods needed for C1/C2 server-side acceptance tests.
/// </summary>
internal sealed class InProcessKahunaCommunication : IKahunaCommunication
{
    private readonly IKahuna kahuna;

    public InProcessKahunaCommunication(IKahuna kahuna) => this.kahuna = kahuna;

    public async Task<(bool, byte[]?, long, HLCTimestamp, int)> TryGetKeyValue(
        string url, HLCTimestamp transactionId, string key, long revision,
        HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken,
        string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
            await kahuna.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);

        if (type == KeyValueResponseType.Get && entry is not null)
            return (true, entry.Value, entry.Revision, entry.LastModified, 0);

        return (false, null, 0, HLCTimestamp.Zero, 0);
    }

    public async Task<(bool, long, int)> TrySetKeyValue(
        string url, HLCTimestamp transactionId, string key, byte[]? value, int expiryTime,
        KeyValueFlags flags, KeyValueDurability durability, CancellationToken cancellationToken,
        string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        (KeyValueResponseType type, long revision, _) =
            await kahuna.LocateAndTrySetKeyValue(transactionId, key, value, null, -1, flags, expiryTime, durability, cancellationToken, 0, coordinatorKey, operationId);

        return (type == KeyValueResponseType.Set, revision, 0);
    }

    public async Task<KeyValueGetByRangePageResult> GetByRange(
        string url, HLCTimestamp transactionId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive,
        int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        KeyValueGetByRangeResult result = await kahuna.LocateAndGetByRange(
            transactionId, prefix, startKey, startInclusive, endKey, endInclusive,
            limit, readTimestamp, durability, cancellationToken);

        return new()
        {
            Items = result.Items.Select(t => new KeyValueGetByBucketItem
            {
                Key = t.Item1,
                Value = t.Item2.Value,
                Revision = t.Item2.Revision,
                LastModified = t.Item2.LastModified
            }).ToList(),
            NextCursor = result.NextCursor,
            HasMore = result.HasMore
        };
    }

    public async IAsyncEnumerable<KeyValueGetByBucketItem> ScanByRange(
        string url, HLCTimestamp transactionId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive,
        int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            transactionId, prefix, startKey, startInclusive, endKey, endInclusive,
            pageSize, readTimestamp, durability, cancellationToken))
        {
            yield return new KeyValueGetByBucketItem
            {
                Key = key,
                Value = entry.Value,
                Revision = entry.Revision,
                LastModified = entry.LastModified
            };
        }
    }

    public async Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryGetManyKeyValues(
        string url, HLCTimestamp transactionId,
        IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken)
    {
        List<(string key, long revision, KeyValueDurability durability)> keys =
            requestItems.Select(i => (i.Key ?? "", i.Revision, i.Durability)).ToList();

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results =
            await kahuna.LocateAndTryGetManyValues(transactionId, HLCTimestamp.Zero, keys, cancellationToken);

        return (results.Select(r => new KahunaGetManyKeyValuesResponseItem
        {
            Key = r.Item2,
            Type = r.Item1,
            Value = r.Item4?.Value,
            Revision = r.Item4?.Revision ?? 0,
            LastModified = r.Item4?.LastModified ?? HLCTimestamp.Zero,
            Durability = r.Item3
        }).ToList(), 0);
    }

    public async Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryExistsManyKeyValues(
        string url, HLCTimestamp transactionId,
        IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken)
    {
        List<(string key, long revision, KeyValueDurability durability)> keys =
            requestItems.Select(i => (i.Key ?? "", i.Revision, i.Durability)).ToList();

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results =
            await kahuna.LocateAndTryExistsManyValues(transactionId, HLCTimestamp.Zero, keys, cancellationToken);

        return (results.Select(r => new KahunaGetManyKeyValuesResponseItem
        {
            Key = r.Item2,
            Type = r.Item1,
            Revision = r.Item4?.Revision ?? 0,
            LastModified = r.Item4?.LastModified ?? HLCTimestamp.Zero,
            Durability = r.Item3
        }).ToList(), 0);
    }

    // ── remaining interface members — not needed for current tests ──────────────

    public Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<(bool, long)> TryExtendLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<KahunaLockInfo?> GetLock(string url, string resource, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<(List<KahunaSetKeyValueResponseItem>, int)> TrySetManyKeyValues(string url, IEnumerable<KahunaSetKeyValueRequestItem> requestItems, CancellationToken cancellationToken) => throw new NotImplementedException();
    public async Task<(List<KahunaDeleteKeyValueResponseItem>, int)> TryDeleteManyKeyValues(string url, IEnumerable<KahunaDeleteKeyValueRequestItem> requestItems, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        List<KahunaDeleteKeyValueResponseItem> responses =
            await kahuna.LocateAndTryDeleteManyKeyValue([.. requestItems], cancellationToken, coordinatorKey, operationId);
        return (responses, 0);
    }
    public async Task<(bool, long, int)> TryExistsKeyValue(
        string url, HLCTimestamp transactionId, string key, long revision,
        HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken,
        string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
            await kahuna.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);
        return type == KeyValueResponseType.Exists
            ? (true, entry?.Revision ?? 0, 0)
            : (false, 0, 0);
    }
    public Task<(bool, long, int)> TryCompareValueAndSetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) => throw new NotImplementedException();
    public Task<(bool, long, int)> TryCompareRevisionAndSetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, long compareRevision, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) => throw new NotImplementedException();
    public Task<(bool, long, int)> TryDeleteKeyValue(string url, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) => throw new NotImplementedException();
    public Task<(bool, long, int)> TryExtendKeyValue(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) => throw new NotImplementedException();
    public Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransactionScript(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken) => throw new NotImplementedException();
    public async Task<bool> TryAcquireExclusiveKeyValueLock(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        (KeyValueResponseType result, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            transactionId, key, expiresMs, durability, cancellationToken, coordinatorKey, operationId);
        return result == KeyValueResponseType.Locked;
    }
    public async Task<bool> TryAcquireExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KeyValueResponseType result = await kahuna.LocateAndTryAcquireExclusivePrefixLock(
            transactionId, prefixKey, expiresMs, durability, cancellationToken, coordinatorKey, operationId);
        return result == KeyValueResponseType.Locked;
    }
    public Task TryReleaseExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public async Task<bool> TryAcquireRangeKeyValueLock(
        string url, HLCTimestamp transactionId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive,
        int expiresMs, KeyValueDurability durability, RangeLockMode mode,
        CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        (KeyValueResponseType result, _) = await kahuna.LocateAndTryAcquireRangeLock(
            transactionId, prefix, startKey, startInclusive, endKey, endInclusive,
            expiresMs, durability, mode, cancellationToken, coordinatorKey, operationId);
        return result == KeyValueResponseType.Locked;
    }
    public Task TryReleaseExclusiveRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public async Task<List<KeyValueGetByBucketItem>> GetByBucket(
        string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        KeyValueGetByBucketResult result = await kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero, prefixKey, readTimestamp, durability, cancellationToken);
        return result.Items.Select(t => new KeyValueGetByBucketItem
        {
            Key = t.Item1, Value = t.Item2.Value, Revision = t.Item2.Revision, LastModified = t.Item2.LastModified
        }).ToList();
    }

    public async Task<List<KeyValueGetByBucketItem>> ScanAllByPrefix(
        string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        // In-process tests run against a single embedded node; use ScanByPrefix (local partition scan)
        // which honours readTimestamp and has the same result set when all keys live on one node.
        KeyValueGetByBucketResult result = await kahuna.ScanByPrefix(prefixKey, readTimestamp, durability);
        return result.Items.Select(t => new KeyValueGetByBucketItem
        {
            Key = t.Item1, Value = t.Item2.Value, Revision = t.Item2.Revision, LastModified = t.Item2.LastModified
        }).ToList();
    }
    public async Task<(string, HLCTimestamp transactionId)> StartTransactionSession(
        string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken)
    {
        KeyValueTransactionOptions opts = new()
        {
            CoordinatorKey     = uniqueId,
            Timeout            = txOptions.Timeout,
            Locking            = txOptions.Locking,
            AsyncRelease       = txOptions.AsyncRelease,
            AutoCommit         = txOptions.AutoCommit,
            ReadValidation     = txOptions.ReadValidation,
            DecisionDurability = txOptions.DecisionDurability,
            ReadTimestamp      = txOptions.ReadTimestamp
        };
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(opts, cancellationToken);
        if (type != KeyValueResponseType.Set)
            throw new KahunaException("Failed to start transaction: " + type, type);
        return (url, handle.TransactionId);
    }

    public async Task<(bool committed, string? recordAnchorKey)> CommitTransactionSession(
        string url, string uniqueId, HLCTimestamp transactionId, string? recordAnchorKey, CancellationToken cancellationToken)
    {
        TransactionHandle handle = new(transactionId, uniqueId, recordAnchorKey);

        // Commit returns the coordinator's canonical record anchor from the frozen finalize snapshot, so
        // there is no race between reading the anchor and freezing the working set. Mirror the gRPC transport
        // contract: true = committed, false = transient MustRetry (retryable), throw on a terminal failure so
        // the SDK can move to a terminal state rather than looping on Pending.
        (KeyValueResponseType type, string? returnedAnchor) = await kahuna.LocateAndCommitTransaction(handle, cancellationToken);
        if (type == KeyValueResponseType.Committed)
            return (true, returnedAnchor);
        if (type == KeyValueResponseType.MustRetry)
            return (false, returnedAnchor);
        throw new KahunaException("Failed to commit key/value transaction: " + type, type);
    }

    public async Task<bool> RollbackTransactionSession(
        string url, string uniqueId, HLCTimestamp transactionId, string? recordAnchorKey, CancellationToken cancellationToken)
    {
        TransactionHandle handle = new(transactionId, uniqueId, recordAnchorKey);
        KeyValueResponseType type = await kahuna.LocateAndRollbackTransaction(handle, cancellationToken);
        if (type == KeyValueResponseType.RolledBack)
            return true;
        if (type == KeyValueResponseType.MustRetry)
            return false;
        throw new KahunaException("Failed to rollback key/value transaction: " + type, type);
    }
    public Task<(SequenceResponseType, ReadOnlySequenceEntry?, int)> GetSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<(SequenceResponseType, long, int)> CreateSequence(string url, string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<(SequenceResponseType, SequenceAllocation, int)> NextSequenceValue(string url, string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<(SequenceResponseType, SequenceAllocation, int)> ReserveSequenceRange(string url, string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<(SequenceResponseType, int)> DeleteSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task<bool> RegisterKeyRange(string url, string keySpace, CancellationToken cancellationToken) =>
        kahuna.RegisterKeyRangeAsync(keySpace, cancellationToken);

    public Task<KahunaClusterMembershipResponse> GetClusterMembership(string url, CancellationToken cancellationToken) =>
        throw new NotImplementedException();

    public Task<KahunaBackupInfo> TakeFullBackup(string url, CancellationToken cancellationToken) =>
        kahuna.TakeFullBackupAsync(cancellationToken);

    public Task<KahunaBackupInfo> TakeIncrementalBackup(string url, Guid parentBackupId, CancellationToken cancellationToken) =>
        kahuna.TakeIncrementalBackupAsync(parentBackupId, cancellationToken);

    public Task<KahunaBackupInfo> TakeCoordinatedBackup(string url, CancellationToken cancellationToken) =>
        kahuna.TakeCoordinatedBackupAsync(cancellationToken);

    public async Task<List<KahunaBackupInfo>> ListBackups(string url, CancellationToken cancellationToken)
    {
        IReadOnlyList<KahunaBackupInfo> result = await kahuna.ListBackupsAsync(cancellationToken);
        return result.ToList();
    }

    public async Task<List<KahunaBackupInfo>> GetBackupChain(string url, Guid leafBackupId, CancellationToken cancellationToken)
    {
        IReadOnlyList<KahunaBackupInfo> result = await kahuna.GetBackupChainAsync(leafBackupId, cancellationToken);
        return result.ToList();
    }

    public Task<KahunaRestoreResponse> Restore(string url, Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken cancellationToken) =>
        kahuna.RestoreToAsync(leafBackupId, targetDir, targetTimeMs, cancellationToken);

    public Task<(KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry)> AcquireSnapshotHold(
        string url, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken) =>
        kahuna.LocateAndAcquireSnapshotHold(holderId, timestamp, leaseMs, cancellationToken);

    public Task<(KeyValueResponseType type, HLCTimestamp leaseExpiry)> RenewSnapshotHold(
        string url, string holdId, int leaseMs, CancellationToken cancellationToken) =>
        kahuna.LocateAndRenewSnapshotHold(holdId, leaseMs, cancellationToken);

    public Task<KeyValueResponseType> ReleaseSnapshotHold(
        string url, string holdId, CancellationToken cancellationToken) =>
        kahuna.LocateAndReleaseSnapshotHold(holdId, cancellationToken);

    public Task<(HLCTimestamp effectiveFloor, int liveHolds)> GetSnapshotFloor(
        string url, CancellationToken cancellationToken) =>
        kahuna.GetSnapshotFloor(cancellationToken);
}
