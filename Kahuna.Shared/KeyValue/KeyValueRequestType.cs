
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Specifies the type of operations that can be performed on key-value storage.
/// These operations include setting, extending, deleting, retrieving, and managing
/// keys and their associated values, as well as handling transactional and concurrency mechanisms.
///
/// <para><b>Every member carries an explicit value, and a value is never changed or reused.</b> The number
/// is persisted: it is the <c>Type</c> of a replicated key-value message in Raft logs, WAL segments and
/// backups, so a node that renumbers a member reads records an older node wrote as a different operation.
/// Inserting a member in the middle once shifted <see cref="MaterializeIntent"/> from 29 to 30, and a node
/// restarted on that build skipped every by-reference commit in its replay window as an unknown type.
/// Add a new member at the end with the next unused value; <c>TestKeyValueRequestTypeWireValues</c> pins
/// the numbers.</para>
///
/// <para><b>A new member must also get a record kind</b> in <c>KeyValueMessageDecoder.Classify</c>: not logged,
/// or the mutation kind every consumer of the key-value log applies it as. Producers can log only a mutation
/// kind, and <c>TestLoggedRecordTypeCoverage</c> fails until the member is classified.</para>
///
/// <para>Records written by the shifted build still carry 30 for a by-reference commit. Consumers of a logged
/// record read its type through <c>KeyValueMessageDecoder.RecordType</c>, which reads a logged 30 as
/// <see cref="MaterializeIntent"/>. That stays correct only while <see cref="DropLeaderState"/> is never
/// written into a log record.</para>
/// </summary>
public enum KeyValueRequestType
{
    TrySet = 0,
    TryExtend = 1,
    TryDelete = 2,
    TryGet = 3,
    TryExists = 4,
    TryAcquireExclusiveLock = 5,
    TryAcquireExclusivePrefixLock = 6,
    TryAcquireExclusiveRangeLock = 7,
    TryReleaseExclusiveLock = 8,
    TryReleaseExclusivePrefixLock = 9,
    TryReleaseExclusiveRangeLock = 10,
    TryPrepareMutations = 11,
    TryCommitMutations = 12,
    TryRollbackMutations = 13,
    ScanByPrefix = 14,
    ScanByPrefixFromDisk = 15,
    GetByBucket = 16,
    GetByRange = 17,
    CompleteProposal = 18,
    ReleaseProposal = 19,
    Collect = 20,
    TryCheckWriteIntent = 21,
    GetRangeLocks = 22,
    ImportRangeLocks = 23,
    GetSafeTimestamp = 24,
    ResumeRead = 25,
    InvalidateOrApply = 26,
    FlushAck = 27,

    /// <summary>
    /// Actor-internal maintenance message: removes every resident entry owned by the partition
    /// carried in the request, after this node stopped being one of its replicas. Never sent by
    /// clients and never serialized into the Raft log.
    /// </summary>
    EvictPartition = 28,

    /// <summary>
    /// A committed durable-transaction mutation replicated BY REFERENCE: the record names the prepared
    /// intent (transaction id, epoch, key) whose value every replica already holds, and carries no value
    /// bytes of its own. Consumers resolve the value from their own prepared-intent store and apply it
    /// exactly as the value-carrying form would.
    ///
    /// <para>Never sent by clients. The value 29 travels in Raft logs and WAL segments; it must stay 29.</para>
    /// </summary>
    MaterializeIntent = 29,

    /// <summary>
    /// Drops the belief-only state an actor holds for a partition this node stopped leading: staged
    /// transactional entries with their write intents, and exclusive prefix and range locks. Committed
    /// entries stay resident. Sent to every shard when Raft reports the leadership lost.
    ///
    /// <para>Never serialized into a log record: a logged 30 is a by-reference commit from the build that shifted
    /// <see cref="MaterializeIntent"/> to 30, and replay reads it as one.</para>
    /// </summary>
    DropLeaderState = 30,

    /// <summary>
    /// Prepare, commit-time range-lock check, and commit of one ephemeral mutation in a single actor turn.
    /// Sent only to the local actor that owns the key, by the coordinator of a transaction whose whole write
    /// set is that one key. Never replicated and never sent between nodes.
    /// </summary>
    TryFinalizeMutation = 31,

    /// <summary>
    /// Runs a unit of work inside one turn of the actor that owns a key, so the requests that work issues for
    /// the key are served without a mailbox hop each. Local to a node; never replicated, never sent between nodes.
    /// </summary>
    RunActorTurn = 32,

    /// <summary>
    /// Records the MVCC read pin of a transactional latest read that was answered off the actor from the key's
    /// published head record, so the transaction's later write or commit of the key is validated against the
    /// revision it read. Local to a node; never replicated, never sent between nodes.
    /// </summary>
    RecordReadPin = 33,

    /// <summary>
    /// Reads many point keys of one key space in one actor turn, optionally taking a Shared <c>[k,k]</c> range
    /// lock on each key just before reading it. Keys that are not resident are loaded from the backend as one
    /// batch and answered in one resume turn. Local to a node; never replicated.
    /// </summary>
    TryProbeMany = 34,

    /// <summary>
    /// Reads many bounded seeks of one key space in one actor turn, each answered as a single range page over its
    /// bounds and limit, optionally under a Shared range lock over its bounds taken just before the read. Disk pages
    /// of the seeks are read as one scheduler operation and merged in one resume turn. Local to a node; never
    /// replicated.
    /// </summary>
    TryProbeManyRanges = 35,
}