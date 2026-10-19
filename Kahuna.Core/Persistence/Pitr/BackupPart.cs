using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>Which half of the backup protocol a part belongs to.</summary>
public enum BackupPartKind
{
    /// <summary>An as-of checkpoint of the assigned partitions at the request's cut.</summary>
    Full = 0,

    /// <summary>The committed WAL of the assigned partitions after the parent chain's range ends.</summary>
    Incremental = 1,
}

/// <summary>
/// What a cluster-wide backup asks of one node: capture the listed partitions — the ones assigned to this
/// node because it hosts them — and stream the result back. Every part of one backup shares one cut (a
/// full; the cut is agreed through <see cref="IBackupPartSink.ProposeCutAsync"/>) or one parent chain (an
/// incremental), and one topology generation.
/// </summary>
/// <param name="Kind">Full or incremental.</param>
/// <param name="Partitions">The partitions this node captures. The node must host every one of them.</param>
/// <param name="ExpectedTopologyGeneration">The topology generation the coordinator captured. A node that sees
/// another one refuses the part, because its partitions would not belong to one layout.</param>
/// <param name="ParentChain">For an incremental: the resolved chain the part extends, full first. The part
/// continues each partition from the chain's high-water mark.</param>
public sealed record BackupPartRequest(
    BackupPartKind Kind,
    IReadOnlyList<int> Partitions,
    long ExpectedTopologyGeneration,
    IReadOnlyList<BackupManifest> ParentChain);

/// <summary>Why a part was refused, or <see cref="Ok"/>.</summary>
public enum BackupPartOutcome
{
    /// <summary>The part was captured and every artifact was streamed.</summary>
    Ok = 0,

    /// <summary>Backup is not configured on the node, so it has no capture machinery.</summary>
    NotConfigured = 1,

    /// <summary>The node no longer hosts one of the requested partitions (a replica move raced the backup).</summary>
    NotHosted = 2,

    /// <summary>The node sees another topology generation, or the topology moved during the capture.</summary>
    TopologyChanged = 3,

    /// <summary>A transaction at or below the cut was in flight on the node; a new attempt agrees on a new cut.</summary>
    CutUnverified = 4,

    /// <summary>An incremental part cannot continue the parent chain (compaction or topology); a full is required.</summary>
    NeedsFull = 5,

    /// <summary>Any other failure; the message says why.</summary>
    Failed = 6,

    /// <summary>
    /// The node had not applied every committed write of one of the requested partitions up to the cut within its
    /// wait, so its image would miss committed data. The message names the partitions. Another replica of those
    /// partitions has the writes applied; a new attempt captures them from it.
    /// </summary>
    NotCaughtUp = 7,
}

/// <summary>
/// The answer to a <see cref="BackupPartRequest"/>. On <see cref="BackupPartOutcome.Ok"/>,
/// <see cref="Manifest"/> describes the streamed artifacts exactly: its digests and sizes are what the
/// coordinator verifies the received bytes against.
/// </summary>
public sealed record BackupPartResult(
    BackupPartOutcome Outcome,
    string? Message,
    BackupManifest? Manifest)
{
    /// <summary>
    /// With <see cref="BackupPartOutcome.NotCaughtUp"/>: the requested partitions whose committed writes the node had
    /// not applied, so the coordinator can capture exactly those from another replica and keep the rest where they
    /// were. Empty from a node that does not report them; the coordinator then treats the whole share as behind.
    /// </summary>
    public IReadOnlyList<int> LaggingPartitions { get; init; } = [];

    internal static BackupPartResult Refused(BackupPartOutcome outcome, string message) =>
        new(outcome, message, null);

    internal static BackupPartResult Refused(BackupPartOutcome outcome, string message, IReadOnlyList<int> laggingPartitions) =>
        new(outcome, message, null) { LaggingPartitions = laggingPartitions };
}

/// <summary>
/// The composing node's side of one share. A full share first proposes what it sees for the cut
/// (<see cref="ProposeCutAsync"/>), then sends its artifacts one file at a time —
/// <see cref="BeginFileAsync"/>, any number of <see cref="WriteAsync"/>, then <see cref="EndFileAsync"/> —
/// and reports its capture (<see cref="CapturedAsync"/>).
/// The in-memory transport hands the coordinator's sink to the capturing node directly; the gRPC transport
/// replays a bidirectional stream into it.
/// </summary>
public interface IBackupPartSink
{
    /// <summary>
    /// Reports what this node sees for the cut of a full share — the lowest commit timestamp in flight on its
    /// partitions (zero when none) and the newest committed entry time of its partitions — and returns the cut
    /// every share captures at, once every share has reported. The composing node cannot see the partitions it
    /// does not host, so only every share's view together gives the cluster-wide cut: below everything in
    /// flight anywhere, and when nothing is in flight, at or above every committed write.
    /// </summary>
    ValueTask<HLCTimestamp> ProposeCutAsync(HLCTimestamp minInFlight, HLCTimestamp maxCommitted, CancellationToken ct);

    /// <summary>Starts the artifact at <paramref name="relativePath"/> (forward slashes, relative to the part).</summary>
    ValueTask BeginFileAsync(string relativePath, CancellationToken ct);

    /// <summary>Appends bytes to the artifact started last.</summary>
    ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct);

    /// <summary>Completes the artifact started last.</summary>
    ValueTask EndFileAsync(CancellationToken ct);

    /// <summary>
    /// Reports that a full share is captured and every artifact sent, and returns once every share of the
    /// backup is captured. The capturing node then makes its final cut check: only a check that runs after every
    /// capture can prove that no transaction at or below the cut has a write in one share and not in another.
    /// Throws <see cref="OperationCanceledException"/> when the backup was abandoned meanwhile.
    /// </summary>
    ValueTask CapturedAsync(BackupManifest manifest, CancellationToken ct);
}
