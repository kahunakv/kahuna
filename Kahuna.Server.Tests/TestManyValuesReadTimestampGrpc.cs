
using Google.Protobuf;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies the gRPC wire path for the batch read-timestamp: a readTimestamp encoded into the three
/// proto scalar fields survives a real serialize/parse round-trip and is decoded by the production
/// <see cref="KeyValuesService"/> handler and forwarded to the manager unchanged — never silently
/// dropped back to <see cref="HLCTimestamp.Zero"/>.
///
/// The in-process cluster tests (TestManyValuesReadTimestamp) exercise the in-memory transport, which
/// threads the HLCTimestamp directly and never touches the proto fields; these tests close the gap by
/// driving the actual proto encode/decode and the receiver handler without needing a live gRPC cluster.
///
/// Joined to the ClusterTests collection so it is serialized with the embedded-cluster tests: those are
/// timing-sensitive (Raft election, leadership), and running these in a parallel collection adds CPU
/// contention that can destabilize them under load.
/// </summary>
[Collection("ClusterTests")]
public class TestManyValuesReadTimestampGrpc
{
    // A readTimestamp whose physical component exceeds int.MaxValue and whose counter is large, so a
    // too-narrow proto field (e.g. int32 physical) or a transposed field would corrupt the round-trip.
    private static readonly HLCTimestamp SampleTs = new(3, 5_000_000_000L, 4_000_000_123u);

    private static KeyValuesService NewService(CapturingKahuna fake) => new(fake, NullLogger<IKahuna>.Instance);

    private static T WireRoundTrip<T>(T message, MessageParser<T> parser) where T : IMessage<T> =>
        parser.ParseFrom(message.ToByteArray());

    // ── GetMany ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetMany_ReadTimestamp_SurvivesWireAndReachesManager()
    {
        GrpcTryGetManyValuesRequest request = new()
        {
            ReadTimestampNode     = SampleTs.N,
            ReadTimestampPhysical = SampleTs.L,
            ReadTimestampCounter  = SampleTs.C
        };
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "k", Revision = -1 });

        GrpcTryGetManyValuesRequest parsed = WireRoundTrip(request, GrpcTryGetManyValuesRequest.Parser);

        CapturingKahuna fake = new();
        await NewService(fake).TryGetManyValuesInternal(parsed, null!);

        Assert.True(fake.GetReadTimestamp.HasValue);
        Assert.Equal(SampleTs, fake.GetReadTimestamp!.Value);
    }

    [Fact]
    public async Task GetMany_UnsetReadTimestamp_DecodesToZero()
    {
        // No ReadTimestamp fields set → they default to 0 on the wire → reconstruct HLCTimestamp.Zero.
        GrpcTryGetManyValuesRequest request = new();
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "k", Revision = -1 });

        GrpcTryGetManyValuesRequest parsed = WireRoundTrip(request, GrpcTryGetManyValuesRequest.Parser);

        CapturingKahuna fake = new();
        await NewService(fake).TryGetManyValuesInternal(parsed, null!);

        Assert.True(fake.GetReadTimestamp.HasValue);
        Assert.Equal(HLCTimestamp.Zero, fake.GetReadTimestamp!.Value);
    }

    // ── ExistsMany ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task ExistsMany_ReadTimestamp_SurvivesWireAndReachesManager()
    {
        GrpcTryExistsManyValuesRequest request = new()
        {
            ReadTimestampNode     = SampleTs.N,
            ReadTimestampPhysical = SampleTs.L,
            ReadTimestampCounter  = SampleTs.C
        };
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "k", Revision = -1 });

        GrpcTryExistsManyValuesRequest parsed = WireRoundTrip(request, GrpcTryExistsManyValuesRequest.Parser);

        CapturingKahuna fake = new();
        await NewService(fake).TryExistsManyValuesInternal(parsed, null!);

        Assert.True(fake.ExistsReadTimestamp.HasValue);
        Assert.Equal(SampleTs, fake.ExistsReadTimestamp!.Value);
    }

    [Fact]
    public async Task ExistsMany_UnsetReadTimestamp_DecodesToZero()
    {
        GrpcTryExistsManyValuesRequest request = new();
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "k", Revision = -1 });

        GrpcTryExistsManyValuesRequest parsed = WireRoundTrip(request, GrpcTryExistsManyValuesRequest.Parser);

        CapturingKahuna fake = new();
        await NewService(fake).TryExistsManyValuesInternal(parsed, null!);

        Assert.True(fake.ExistsReadTimestamp.HasValue);
        Assert.Equal(HLCTimestamp.Zero, fake.ExistsReadTimestamp!.Value);
    }

    /// <summary>
    /// Records the readTimestamp handed to the batch read entry points and returns an empty result,
    /// so the handler builds a valid (empty) response without any entry-serialization concerns.
    /// Every other member is unreachable in these tests.
    /// </summary>
    private sealed class CapturingKahuna : IKahuna
    {
        public HLCTimestamp? GetReadTimestamp { get; private set; }
        public HLCTimestamp? ExistsReadTimestamp { get; private set; }

        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys)
        {
            GetReadTimestamp = readTimestamp;
            return Task.FromResult(new List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>());
        }

        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys)
        {
            ExistsReadTimestamp = readTimestamp;
            return Task.FromResult(new List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>());
        }

        // ── Unreachable in these tests ────────────────────────────────────────────
        public Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(LockResponseType, ReadOnlyLockEntry?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability) => throw new NotImplementedException();
        public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability) => throw new NotImplementedException();
        public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability) => throw new NotImplementedException();
        public Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string resource, LockDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default) => throw new NotImplementedException();
        public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) => throw new NotImplementedException();
        public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, long routedGeneration = 0) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueResponseType> TryCheckWriteIntentValue(HLCTimestamp transactionId, string key, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndCommitTransaction(TransactionHandle handle, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, List<KeyValueTransactionReadKey> readKeys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, string? modifiedKey, string? pointLockKey, string? readKey, bool readExists, long readRevision, KeyValueDurability durability, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, CancellationToken cancellationToken) => throw new NotImplementedException();
        public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest) => throw new NotImplementedException();
        public void CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, string? modifiedKey, string? pointLockKey, string? readKey, bool readExists, long readRevision, KeyValueDurability durability, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<List<KeyValueRangeLock>> GetRangeLocks(string keySpace) => throw new NotImplementedException();
        public Task ImportRangeLocks(string keySpace, List<KeyValueRangeLock> locks) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, long routedGeneration = 0) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long)> TryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long)> TryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueTransactionResult> TryExecuteTransactionScript(byte[] script, string? hash, List<KeyValueParameter>? parameters) => throw new NotImplementedException();
        public Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp transactionId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options) => throw new NotImplementedException();
        public Task<KeyValueResponseType> CommitTransaction(TransactionHandle handle, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, List<KeyValueTransactionReadKey> readKeys) => throw new NotImplementedException();
        public Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys) => throw new NotImplementedException();
        public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(string name, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(SequenceResponseType, long)> LocateAndCreateSequence(string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<SequenceResponseType> LocateAndDeleteSequence(string name, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<bool> OnLogRestored(int partitionId, RaftLog log) => throw new NotImplementedException();
        public Task<bool> OnReplicationReceived(int partitionId, RaftLog log) => throw new NotImplementedException();
        public void OnReplicationError(int partitionId, RaftLog log) => throw new NotImplementedException();
        public Task<bool> OnLeaderChanged(int partitionId, string node) => throw new NotImplementedException();
        public Task FlushPersistenceAsync() => throw new NotImplementedException();
        public Task BootstrapFromPitrBackupAsync(string backupDir, Guid leafBackupId, HLCTimestamp targetTime, IWAL walAdapter, TimeSpan pitrWindow, TimeSpan baseSnapshotInterval) => throw new NotImplementedException();
        public void RegisterKeyRange(string keySpace) => throw new NotImplementedException();
        public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<bool> RemoveKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<int> TriggerAutoSplitAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task<int> TriggerAutoMergeAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public bool IsBackupConfigured => throw new NotImplementedException();
        public Task<KahunaBackupInfo> TakeFullBackupAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task<KahunaBackupInfo> TakeIncrementalBackupAsync(Guid parentBackupId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<KahunaBackupInfo>> ListBackupsAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<KahunaBackupInfo>> GetBackupChainAsync(Guid leafBackupId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<KahunaRestoreResponse> RestoreToAsync(Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> LocateAndAcquireSnapshotHold(string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct) => throw new NotImplementedException();
        public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> LocateAndRenewSnapshotHold(string holdId, int leaseMs, CancellationToken ct) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndReleaseSnapshotHold(string holdId, CancellationToken ct) => throw new NotImplementedException();
        public Task<(HLCTimestamp EffectiveFloor, int LiveHolds)> GetSnapshotFloor(CancellationToken ct) => throw new NotImplementedException();
    }
}
