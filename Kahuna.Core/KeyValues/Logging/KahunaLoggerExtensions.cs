
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Logging;

public static partial class KahunaLoggerExtensions
{
    [LoggerMessage(Level = LogLevel.Debug, Message = "KeyValueActor Message: {Actor} {Type} Key={Key} {Value} Expires={ExpiresMs} Flags={Flags} Revision={Revision} TxId={TransactionId} {Durability}")]
    public static partial void LogKeyValueActorEnter(this ILogger<IKahuna> logger, string actor, KeyValueRequestType type, string key, int? value, int expiresMs, KeyValueFlags flags, long revision, HLCTimestamp transactionId, KeyValueDurability durability);

    [LoggerMessage(Level = LogLevel.Debug, Message = "KeyValueActor Took: {Actor} {Type} Key={Key} Response={Response} Revision={Revision} Time={Elapsed}ms")]
    public static partial void LogKeyValueActorTook(this ILogger<IKahuna> logger, string actor, KeyValueRequestType type, string key, KeyValueResponseType? response, long? revision, long elapsed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Evicted {Count} key/value pairs (tombstone={Tombstone}, expired={Expired}, lru={Lru}, idle={Idle}, storeCount={StoreCount}, storeBytes={StoreBytes}, elapsedMs={ElapsedMs}, backlog={Backlog})")]
    public static partial void LogKeyValueEviction(
        this ILogger<IKahuna> logger,
        int count,
        int tombstone,
        int expired,
        int lru,
        int idle,
        int storeCount,
        long storeBytes,
        long elapsedMs,
        bool backlog
    );

    // ── KeyValueLocator redirect logs ──────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "SET-KEYVALUE Redirected {Key} to leader partition {Partition} at {Leader} Time={Elapsed}ms")]
    public static partial void LogSetKeyValueRedirected(this ILogger<IKahuna> logger, string key, int partition, string leader, long elapsed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "SET-MANY-KEYVALUE Redirect {Number} set key/value pairs to node {Leader}")]
    public static partial void LogSetManyKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "DELETE-MANY-KEYVALUE Redirect {Number} delete key/value pairs to node {Leader}")]
    public static partial void LogDeleteManyKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "DELETE-KEYVALUE Redirected {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogDeleteKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "EXTEND-KEYVALUE Redirected {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogExtendKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GET-KEYVALUE Redirected {KeyValueName} to leader partition {Partition} at {Leader} Time={Elapsed}ms")]
    public static partial void LogGetKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader, long elapsed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "EXISTS-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogExistsKeyValueRedirect(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "EXISTS-KEYVALUE Redirect {Number} batched exists probes to node {Leader}")]
    public static partial void LogExistsManyKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GET-KEYVALUE Redirect {Number} batched gets to node {Leader}")]
    public static partial void LogGetManyKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "CHECK-WRITE-INTENT Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogCheckWriteIntentRedirect(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ACQUIRE-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogAcquireLockKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ACQUIRE-PREFIX-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogAcquirePrefixLockKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ACQUIRE-LOCK-KEYVALUE Redirect {Number} lock acquisitions to node {Leader}")]
    public static partial void LogAcquireManyLocksKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RELEASE-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogReleaseLockKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RELEASE-PREFIX-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogReleasePrefixLockKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ACQUIRE-RANGE-LOCK-KEYVALUE Redirect {Prefix} P{Partition} → {Leader}")]
    public static partial void LogAcquireRangeLockKeyValueRedirected(this ILogger<IKahuna> logger, string prefix, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RELEASE-RANGE-LOCK-KEYVALUE Redirect {Prefix} P{Partition} → {Leader}")]
    public static partial void LogReleaseRangeLockKeyValueRedirected(this ILogger<IKahuna> logger, string prefix, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ACQUIRE-RANGE-LOCK {Prefix}: descriptor set changed during acquisition — MustRetry")]
    public static partial void LogAcquireRangeLockDescriptorChanged(this ILogger<IKahuna> logger, string prefix);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RELEASE-LOCK-KEYVALUE Redirect {Number} lock releases to node {Leader}")]
    public static partial void LogReleaseManyLocksKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "PREPARE-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogPrepareKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "PREPARE-KEYVALUE Redirect {Number} prepare mutations to node {Leader}")]
    public static partial void LogPrepareManyKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "COMMIT-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogCommitKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "COMMIT-KEYVALUE Redirect {Number} Commit mutations to node {Leader}")]
    public static partial void LogCommitManyKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ROLLBACK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogRollbackKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ROLLBACK-KEYVALUE Redirect {Number} Commit mutations to node {Leader}")]
    public static partial void LogRollbackManyKeyValueRedirect(this ILogger<IKahuna> logger, int number, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GETPREFIX-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogGetPrefixKeyValueRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GETRANGE-KEYVALUE Redirect {Prefix} to leader partition {Partition} at {Leader}")]
    public static partial void LogGetRangeKeyValueRedirected(this ILogger<IKahuna> logger, string prefix, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "START-TRANSACTION Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogStartTransactionRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "COMMIT-TRANSACTION Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogCommitTransactionRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ROLLBACK-TRANSACTION Redirect {KeyValueName} to leader partition {Partition} at {Leader}")]
    public static partial void LogRollbackTransactionRedirected(this ILogger<IKahuna> logger, string keyValueName, int partition, string leader);

    // ── KeyValuesManager ──────────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting {Workers} ephemeral key/value workers")]
    public static partial void LogStartingEphemeralWorkers(this ILogger<IKahuna> logger, int workers);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting {Workers} persistent key/value workers")]
    public static partial void LogStartingPersistentWorkers(this ILogger<IKahuna> logger, int workers);

    // ── Transaction coordinator exception logs ─────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "KahunaScriptException")]
    public static partial void LogKahunaScriptException(this ILogger<IKahuna> logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Debug, Message = "KahunaAbortedException")]
    public static partial void LogKahunaAbortedException(this ILogger<IKahuna> logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Debug, Message = "TaskCanceledException")]
    public static partial void LogTaskCanceledException(this ILogger<IKahuna> logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Debug, Message = "OperationCanceledException")]
    public static partial void LogOperationCanceledException(this ILogger<IKahuna> logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "TryExecuteTx")]
    public static partial void LogTryExecuteTxError(this ILogger<IKahuna> logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Started interactive transaction {TransactionId}")]
    public static partial void LogStartedInteractiveTransaction(this ILogger<IKahuna> logger, HLCTimestamp transactionId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Committed interactive transaction {TransactionId}")]
    public static partial void LogCommittedInteractiveTransaction(this ILogger<IKahuna> logger, HLCTimestamp transactionId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Rolled back interactive transaction {TransactionId}")]
    public static partial void LogRolledBackInteractiveTransaction(this ILogger<IKahuna> logger, HLCTimestamp transactionId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Write-skew guard aborted optimistic transaction {TransactionId}: concurrent writer on {Key}")]
    public static partial void LogWriteSkewGuardAborted(this ILogger<IKahuna> logger, HLCTimestamp transactionId, string key);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Read dependency changed for {Key} {Durability}: expected exists {ExpectedExists}, actual exists {ActualExists}")]
    public static partial void LogReadDependencyChanged(this ILogger<IKahuna> logger, string key, KeyValueDurability durability, bool expectedExists, bool actualExists);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Read dependency revision changed for {Key} {Durability}: expected revision {ExpectedRevision}, actual revision {ActualRevision}")]
    public static partial void LogReadDependencyRevisionChanged(this ILogger<IKahuna> logger, string key, KeyValueDurability durability, long expectedRevision, long actualRevision);

    // ── Handler logs ──────────────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "Assigned {Key} write intent to TxId={TransactionId}")]
    public static partial void LogAssignedWriteIntent(this ILogger<IKahuna> logger, string key, HLCTimestamp transactionId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Assigned {Key} write intent to TxId={TransactionId} (range lock)")]
    public static partial void LogAssignedWriteIntentRangeLock(this ILogger<IKahuna> logger, string key, HLCTimestamp transactionId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Successfully commmitted key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}")]
    public static partial void LogSuccessfullyCommittedKeyValue(this ILogger<IKahuna> logger, string key, int partition, long proposalIndex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Successfully proposed key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}")]
    public static partial void LogSuccessfullyProposedKeyValue(this ILogger<IKahuna> logger, string key, int partition, long proposalIndex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Successfully rolled back key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}")]
    public static partial void LogSuccessfullyRolledBackKeyValue(this ILogger<IKahuna> logger, string key, int partition, long proposalIndex);

    // ── Range logs ────────────────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeMergeChecker: performed {Count} merge(s).")]
    public static partial void LogRangeMergeCheckerPerformed(this ILogger<IKahuna> logger, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeSplitChecker: performed {Count} split(s).")]
    public static partial void LogRangeSplitCheckerPerformed(this ILogger<IKahuna> logger, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeMergeTrigger: retry-retired P{Id}")]
    public static partial void LogRangeMergeTriggerRetryRetired(this ILogger<IKahuna> logger, int id);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RangeMergeTrigger: retry RemovePartitionAsync({Id}) still failed: {Status}")]
    public static partial void LogRangeMergeTriggerRetryFailed(this ILogger<IKahuna> logger, int id, string status);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeMergeTrigger: merging {Space} [{LS},{LE})@P{LP} + [{RS},{RE})@P{RP}")]
    public static partial void LogRangeMergeTriggerMerging(this ILogger<IKahuna> logger, string space, string lS, string? lE, int lP, string? rS, string? rE, int rP);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RangeMergeTrigger: MergeAsync failed for {Space}: {Status}")]
    public static partial void LogRangeMergeTriggerFailed(this ILogger<IKahuna> logger, string space, string status);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RangeMergeTrigger: RemovePartitionAsync({Id}) failed for {Space}: {Status} — queued for retry")]
    public static partial void LogRangeMergeTriggerRemoveFailed(this ILogger<IKahuna> logger, int id, string space, string status);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeMergeTrigger: retired P{Id} for {Space}")]
    public static partial void LogRangeMergeTriggerRetired(this ILogger<IKahuna> logger, int id, string space);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeSplitTrigger: splitting {Space} [{Start},{End}) at {Key}")]
    public static partial void LogRangeSplitTriggerSplitting(this ILogger<IKahuna> logger, string space, string start, string end, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RangeSplitTrigger: CreatePartitionAsync({Id}) failed for {Space}")]
    public static partial void LogRangeSplitTriggerCreateFailed(this ILogger<IKahuna> logger, int id, string space);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeSplitTrigger: split {Space} at {Key} → P{Id}")]
    public static partial void LogRangeSplitTriggerSplit(this ILogger<IKahuna> logger, string space, string key, int id);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RangeSplitTrigger: SplitAsync failed for {Space} at {Key}: {Status}")]
    public static partial void LogRangeSplitTriggerSplitFailed(this ILogger<IKahuna> logger, string space, string key, string status);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RangeSplitTrigger: {Space} P{PartitionId} descriptor stale (gen advanced); skipping split")]
    public static partial void LogRangeSplitTriggerDescriptorStale(this ILogger<IKahuna> logger, string space, int partitionId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RangeSplitTrigger: failed to remove orphaned P{PartitionId} after SplitAsync failure")]
    public static partial void LogRangeSplitTriggerOrphanRemoveFailed(this ILogger<IKahuna> logger, int partitionId, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RangeSplitTrigger: CreatePartitionAsync({Id}) threw — likely lost leadership")]
    public static partial void LogRangeSplitTriggerCreateThrew(this ILogger<IKahuna> logger, Exception ex, int id);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RangeSplitTrigger: {Space} P{PartitionId} indivisible (imbalance={Imbalance:F3} >= max={Max:F3}); skipping")]
    public static partial void LogRangeSplitTriggerIndivisible(this ILogger<IKahuna> logger, string space, int partitionId, double imbalance, double max);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeMergeTrigger: {Space} skipping merge of P{LeftId}+P{RightId} — at least one partition is warm (ops ≥ split threshold); merge would re-trigger split")]
    public static partial void LogRangeMergeTriggerWarmSkipped(this ILogger<IKahuna> logger, string space, int leftId, int rightId);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeSplitTrigger: {Space} P{PartitionId} load-hot for {ElapsedMs:F0} ms (ops={Ops:F1}/s depth={Depth} commitWait={CommitWait:F1} ms) — triggering split")]
    public static partial void LogRangeSplitTriggerLoadHot(this ILogger<IKahuna> logger, string space, int partitionId, double elapsedMs, double ops, int depth, double commitWait);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RangeSplitTrigger: {Space} P{PartitionId} load-hot but no active peer nodes visible — skipping split (no relocation target; split would add Raft overhead with zero relief)")]
    public static partial void LogRangeSplitTriggerLoadNoReliefTarget(this ILogger<IKahuna> logger, string space, int partitionId);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeSplitLoadChecker: performed {Count} load-triggered split(s)")]
    public static partial void LogRangeSplitLoadCheckerPerformed(this ILogger<IKahuna> logger, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeMerger: merging {Space} [{LS},{LE})@P{LP} + [{RS},{RE})@P{RP}")]
    public static partial void LogRangeMergerMerging(this ILogger<IKahuna> logger, string space, string? lS, string? lE, int lP, string? rS, string? rE, int rP);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeMerger: merged {Space} -> [{A},{C})@P{P} gen={Gen} (retired P{Retired})")]
    public static partial void LogRangeMergerMerged(this ILogger<IKahuna> logger, string space, string? a, string? c, int p, long gen, int retired);

    [LoggerMessage(Level = LogLevel.Information, Message = "RangeSplitter: split {Space} at {Key} → [{Start},{Key}) @P{SourcePartition} + [{Key},{End}) @P{NewPartition} gen={Gen}")]
    public static partial void LogRangeSplitterSplit(this ILogger<IKahuna> logger, string space, string key, string? start, string? end, int sourcePartition, int newPartition, long gen);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded range-map snapshot from {Path} ({Count} descriptors)")]
    public static partial void LogRangeMapSnapshotLoaded(this ILogger<IKahuna> logger, string path, int count);

    // ── ScriptParser logs ─────────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "Retrieved script from cache {Hash}")]
    public static partial void LogScriptRetrievedFromCache(this ILogger<IKahuna> logger, string hash);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Added script to cache {Hash}")]
    public static partial void LogScriptAddedToCache(this ILogger<IKahuna> logger, string hash);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Removed {Key} from script cache")]
    public static partial void LogScriptRemovedFromCache(this ILogger<IKahuna> logger, string key);
}
