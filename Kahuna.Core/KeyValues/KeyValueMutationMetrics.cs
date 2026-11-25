using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The kinds of operation that a leadership confirmation admits to the state of a key actor without a proposal, and
/// the <c>op</c> tag of <see cref="KeyValueMutationMetrics"/>.
/// </summary>
internal enum KeyValueActorOperation
{
    /// <summary>A set, delete or extend of a transaction, alone or in a batch.</summary>
    StagedWrite,

    /// <summary>The grant of a point, prefix or range lock, and a probe that takes locks.</summary>
    LockGrant,

    /// <summary>The release of a point, prefix or range lock.</summary>
    LockRelease,

    /// <summary>A check of a commit: conflicting writers, held staged intents, and locks still in force.</summary>
    CommitCheck,

    /// <summary>A prepare, commit, rollback or finalize of an ephemeral mutation.</summary>
    Ticket
}

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the leadership confirmation that admits an operation to
/// the state of a key actor without a proposal (see <see cref="KeyValueActorOperation"/>). A point read has its own
/// series, <see cref="KeyValueReadMetrics"/>.
/// </summary>
internal static class KeyValueMutationMetrics
{
    /// <summary>
    /// The leadership confirmation of one operation on one partition, on the node that serves it, tagged by <c>op</c>
    /// and by <c>lease=hit|miss</c>. A hit is a lease that answered on the calling thread. A miss is a wait for the
    /// applied frontier of this node, or an executor round when the lease had lapsed. The tag set is fixed.
    /// </summary>
    internal static readonly Histogram<double> ConfirmMs =
        KeyValueReadMetrics.Meter.CreateHistogram<double>("kahuna.kv.mutation.confirm", unit: "ms", description: "Leadership confirmation of operations on key-actor state that replicate nothing, tagged by operation and by whether the lease answered it synchronously.");

    /// <summary>
    /// The operations on named keys that kept the wait for the applied frontier, because a log entry of this node that
    /// is not delivered yet can change one of their keys (see <see cref="Writes.UndeliveredKeys"/>), tagged by
    /// <c>op</c>. Counted once per operation and partition. A counted operation does not always wait: a key is marked
    /// from the dispatch of its entry, and the wait starts with the commit.
    /// </summary>
    internal static readonly Counter<long> UndeliveredKeyOperations =
        KeyValueReadMetrics.Meter.CreateCounter<long>("kahuna.kv.mutation.undelivered_key", description: "Operations on named keys that kept the wait for the applied frontier because an undelivered log entry can change one of their keys, tagged by operation.");

    private static readonly KeyValuePair<string, object?> LeaseHit = new("lease", "hit");
    private static readonly KeyValuePair<string, object?> LeaseMiss = new("lease", "miss");

    // One tag per KeyValueActorOperation value, indexed by the value, so a sample allocates nothing.
    private static readonly KeyValuePair<string, object?>[] Operations = BuildOperations();

    private static KeyValuePair<string, object?>[] BuildOperations()
    {
        KeyValueActorOperation[] values = Enum.GetValues<KeyValueActorOperation>();
        KeyValuePair<string, object?>[] tags = new KeyValuePair<string, object?>[values.Length];

        foreach (KeyValueActorOperation value in values)
            tags[(int)value] = new("op", OperationName(value));

        return tags;
    }

    /// <summary>The <c>op</c> tag value of an operation.</summary>
    internal static string OperationName(KeyValueActorOperation operation) => operation switch
    {
        KeyValueActorOperation.StagedWrite => "staged_write",
        KeyValueActorOperation.LockGrant => "lock_grant",
        KeyValueActorOperation.LockRelease => "lock_release",
        KeyValueActorOperation.CommitCheck => "commit_check",
        _ => "ticket"
    };

    /// <summary>One confirmation that began at <paramref name="start"/> and ended now.</summary>
    internal static void RecordConfirm(KeyValueActorOperation operation, long start, bool leaseHit) =>
        ConfirmMs.Record(KeyValueReadMetrics.ElapsedMs(start, Stopwatch.GetTimestamp()), Operations[(int)operation], leaseHit ? LeaseHit : LeaseMiss);

    /// <summary>One operation on named keys that kept the wait for the applied frontier.</summary>
    internal static void UndeliveredKey(KeyValueActorOperation operation) => UndeliveredKeyOperations.Add(1, Operations[(int)operation]);
}
