
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Client.Communication;

public interface IKahunaCommunication
{
     Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken);

     Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken);

     Task<(bool, long)> TryExtendLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken);

     Task<KahunaLockInfo?> GetLock(string url, string resource, LockDurability durability, CancellationToken cancellationToken);

     Task<(bool, long, int)> TrySetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, int expiryTime, KeyValueFlags flags, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);
     
     Task<(List<KahunaSetKeyValueResponseItem>, int)> TrySetManyKeyValues(string url, IEnumerable<KahunaSetKeyValueRequestItem> requestItems, CancellationToken cancellationToken);

     Task<(List<KahunaDeleteKeyValueResponseItem>, int)> TryDeleteManyKeyValues(string url, IEnumerable<KahunaDeleteKeyValueRequestItem> requestItems, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

     Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryGetManyKeyValues(string url, HLCTimestamp transactionId, IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken);

     Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryExistsManyKeyValues(string url, HLCTimestamp transactionId, IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken);

     Task<(bool, long, int)> TryCompareValueAndSetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

     Task<(bool, long, int)> TryCompareRevisionAndSetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, long compareRevision, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

     Task<(bool, byte[]?, long, HLCTimestamp, int)> TryGetKeyValue(string url, HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

     Task<(bool, long, int)> TryExistsKeyValue(string url, HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

     Task<(bool, long, int)> TryDeleteKeyValue(string url, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

     Task<(bool, long, int)> TryExtendKeyValue(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

     Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransactionScript(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken);

     Task<bool> TryAcquireExclusiveKeyValueLock(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<bool> TryAcquireExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

     Task TryReleaseExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<bool> TryAcquireRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken);

     Task TryReleaseExclusiveRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<KeyValueGetByRangePageResult> GetByRange(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

     IAsyncEnumerable<KeyValueGetByBucketItem> ScanByRange(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<List<KeyValueGetByBucketItem>> GetByBucket(string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<List<KeyValueGetByBucketItem>> ScanAllByPrefix(string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);
     
     Task<(string, HLCTimestamp transactionId)> StartTransactionSession(string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken);
     
     Task<(bool committed, string? recordAnchorKey)> CommitTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, CancellationToken cancellationToken);

     Task<bool> RollbackTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, CancellationToken cancellationToken);

     Task<(SequenceResponseType, ReadOnlySequenceEntry?, int)> GetSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken);

     Task<(SequenceResponseType, long, int)> CreateSequence(string url, string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken);

     Task<(SequenceResponseType, SequenceAllocation, int)> NextSequenceValue(string url, string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken);

     Task<(SequenceResponseType, SequenceAllocation, int)> ReserveSequenceRange(string url, string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken);

     Task<(SequenceResponseType, int)> DeleteSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken);

     Task<bool> RegisterKeyRange(string url, string keySpace, CancellationToken cancellationToken);

     Task<(KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry)> AcquireSnapshotHold(
         string url, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken);

     Task<(KeyValueResponseType type, HLCTimestamp leaseExpiry)> RenewSnapshotHold(
         string url, string holdId, int leaseMs, CancellationToken cancellationToken);

     Task<KeyValueResponseType> ReleaseSnapshotHold(
         string url, string holdId, CancellationToken cancellationToken);

     Task<(HLCTimestamp effectiveFloor, int liveHolds)> GetSnapshotFloor(
         string url, CancellationToken cancellationToken);

     Task<KahunaClusterMembershipResponse> GetClusterMembership(string url, CancellationToken cancellationToken);

     Task<KahunaBackupInfo> TakeFullBackup(string url, CancellationToken cancellationToken);
     Task<KahunaBackupInfo> TakeIncrementalBackup(string url, Guid parentBackupId, CancellationToken cancellationToken);
     Task<KahunaBackupInfo> TakeCoordinatedBackup(string url, CancellationToken cancellationToken);
     Task<List<KahunaBackupInfo>> ListBackups(string url, CancellationToken cancellationToken);
     Task<List<KahunaBackupInfo>> GetBackupChain(string url, Guid leafBackupId, CancellationToken cancellationToken);
     Task<KahunaRestoreResponse> Restore(string url, Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken cancellationToken);
}
