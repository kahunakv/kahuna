using System.Buffers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.Communication.Rest;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Cluster-wide backups under a replication factor smaller than the node count. No node hosts every
/// partition there, so a backup is composed from <i>shares</i>: each active partition is assigned to one node
/// that hosts it, each node captures its assigned partitions through the ordinary driver path, and the shares
/// stream to the node that composes them. That node merges the shares' checkpoints (or collects their
/// segments) and publishes one ordinary manifest that covers every partition, so restore, chain validation
/// and the bootstrap seed read it exactly like a backup one node took alone.
///
/// <para><b>Why the shares stream instead of meeting in the backup target.</b> Every node sweeps artifact sets
/// that have no manifest. On a target shared by several nodes, a share written there would be reclaimed by
/// another node's sweep while the composition still reads it. A share lives only in a private local directory
/// on the node that captured it and on the node that composes it, so no sweep can see it, and a cluster-wide
/// backup needs no shared target at all.</para>
///
/// <para><b>One cut for every share.</b> Each share of a full captures at the same cut, with the same
/// snapshot-hold, applied-index and cut-verification fences a single-node backup has, and refuses a cut at or
/// above a commit timestamp it sees in flight. The coordinator cannot see the intents of partitions it does not
/// host, so a share that finds such a transaction reports its timestamp and the next attempt cuts below it.
/// Every share is checked against the topology generation the coordinator captured, and the coordinator
/// re-checks its own before it publishes.</para>
/// </summary>
internal sealed partial class BackupService
{
    // The size of one streamed chunk: far below the inter-node transport's message limit.
    private const int ShareChunkBytes = 256 * 1024;

    // A share that saw another topology generation usually trails the committed map by a moment.
    private const int TopologyRetryDelayMs = 200;

    private const string SharePrefix = "kahuna_backup_share_";

    /// <summary>One node's share of a cluster-wide backup. <see cref="Node"/> is null when no node that
    /// hosts one of the partitions is known.</summary>
    private sealed record PartAssignment(string? Node, bool IsLocal, List<int> Partitions);

    /// <summary>A share whose artifacts are in the composition's private store under the manifest's id.</summary>
    private sealed record ReceivedShare(PartAssignment Assignment, BackupManifest Manifest);

    /// <summary>The partitions a full backup must reconstruct: the committed map minus ranges mid-move or merged away.</summary>
    private List<int> ActivePartitionIds()
    {
        List<int> ids = [];
        foreach (RaftPartitionRange partition in _raft.GetPartitionMap())
        {
            if (partition.State is RaftPartitionState.Draining or RaftPartitionState.Removed)
                continue;
            ids.Add(partition.PartitionId);
        }
        return ids;
    }

    /// <summary>
    /// Assigns each partition to one node that can capture it. This node captures a partition itself (no transfer)
    /// when it is one of the partition's voters and has it materialized (<see cref="CapturesLocally"/>); a partition
    /// it hosts only as a learner or as a replica marked for removal goes to a voter, like a partition it does not
    /// host: the leader hint when that node is a voter of the partition (it has applied everything committed), else
    /// the first voter. A replica in <paramref name="avoid"/> (one an earlier attempt could not reach, or that had
    /// not applied the partition's committed writes to the cut) is chosen only when no other voter is known. Returns
    /// null when this node captures every partition itself, so the caller captures alone.
    /// </summary>
    private List<PartAssignment>? AssignShares(
        IEnumerable<int> partitions, IReadOnlySet<(string Node, int PartitionId)>? avoid = null)
    {
        string self = _raft.GetLocalEndpoint();
        List<int> local = [];
        Dictionary<string, List<int>> remote = new(StringComparer.Ordinal);
        List<int> unassignable = [];

        foreach (int partitionId in partitions)
        {
            // A partition this node can capture but should avoid goes to another voter when one is known, and stays
            // here otherwise (under legacy full replication there is no replica set to choose from).
            bool capturesLocally = CapturesLocally(partitionId);
            string? node = capturesLocally && (avoid is null || !avoid.Contains((self, partitionId)))
                ? self
                : ChooseHostingNode(partitionId, self, avoid) ?? (capturesLocally ? self : null);

            if (node is null)
                unassignable.Add(partitionId);
            else if (string.Equals(node, self, StringComparison.Ordinal))
                local.Add(partitionId);
            else if (remote.TryGetValue(node, out List<int>? assigned))
                assigned.Add(partitionId);
            else
                remote[node] = [partitionId];
        }

        if (remote.Count == 0 && unassignable.Count == 0)
            return null;

        List<PartAssignment> shares = new(remote.Count + 2);
        if (local.Count > 0)
            shares.Add(new PartAssignment(self, IsLocal: true, local));
        foreach ((string node, List<int> assigned) in remote)
            shares.Add(new PartAssignment(node, IsLocal: false, assigned));
        if (unassignable.Count > 0)
            shares.Add(new PartAssignment(null, IsLocal: false, unassignable));
        return shares;
    }

    /// <summary>
    /// Whether this node captures <paramref name="partitionId"/> itself: it has the partition materialized and the
    /// committed replica set lists it as a voter, or the set is empty (legacy full replication, where every hosting
    /// node is a voter). A learner hosts the partition too, but it starts from nothing and can be hundreds of
    /// thousands of entries behind for minutes; a replica marked for removal is about to stop hosting it; a hosted
    /// partition absent from the set is mid-removal. A capture waits for the local apply to reach the cut, so none of
    /// them is a source it can wait for, while the partition's voters have the committed writes applied now.
    /// </summary>
    private bool CapturesLocally(int partitionId)
    {
        if (!_raft.HostsPartition(partitionId))
            return false;

        IReadOnlyList<RaftReplica> replicas = _raft.GetPartitionReplicas(partitionId);
        if (replicas.Count == 0)
            return true;

        string self = _raft.GetLocalEndpoint();
        foreach (RaftReplica replica in replicas)
        {
            if (string.Equals(replica.Endpoint, self, StringComparison.Ordinal))
                return replica.Role == RaftReplicaRole.Voter;
        }

        return false;
    }

    /// <summary>
    /// The node that captures a partition this node does not capture itself: the leader hint when it is a voter of
    /// the partition, else the first voter, with the replicas in <paramref name="avoid"/> as a last resort. This node
    /// is a candidate only when it hosts the partition: a voter the committed map lists but that has not materialized
    /// the partition yet cannot capture it.
    /// </summary>
    private string? ChooseHostingNode(int partitionId, string self, IReadOnlySet<(string Node, int PartitionId)>? avoid)
    {
        IReadOnlyList<RaftReplica> replicas = _raft.GetPartitionReplicas(partitionId);
        string? hint = _raft.GetPartitionLeaderHint(partitionId);
        string? firstVoter = null;
        string? avoidedVoter = null;

        foreach (RaftReplica replica in replicas)
        {
            if (replica.Role != RaftReplicaRole.Voter || string.IsNullOrEmpty(replica.Endpoint))
                continue;

            if (string.Equals(replica.Endpoint, self, StringComparison.Ordinal) && !_raft.HostsPartition(partitionId))
                continue;

            if (avoid is not null && avoid.Contains((replica.Endpoint, partitionId)))
            {
                avoidedVoter ??= replica.Endpoint;
                continue;
            }

            if (hint is not null && string.Equals(replica.Endpoint, hint, StringComparison.Ordinal))
                return hint;

            firstVoter ??= replica.Endpoint;
        }

        return firstVoter ?? avoidedVoter;
    }

    private static void EnsureAssignable(List<PartAssignment> shares, bool fullBackup)
    {
        foreach (PartAssignment share in shares)
        {
            if (share.Node is not null)
                continue;

            string message =
                $"No node is known to host partition(s) {string.Join(", ", share.Partitions)}, so their share of the " +
                "backup cannot be captured; nothing was published.";

            // For a full the partition map moved under the backup; for an incremental the chain's layout is gone.
            throw fullBackup
                ? new BackupDriverException(message) { TopologyChanged = true }
                : new BackupDriverException(message) { NeedsFullBackup = true, TopologyDivergedFromParent = true };
        }
    }

    // ── a share, on the node that captures it ──────────────────────────────────────────────────────

    /// <summary>
    /// Captures this node's share of a cluster-wide backup and streams its artifacts to
    /// <paramref name="sink"/>. The share goes through the ordinary driver path into a private local store
    /// that is deleted afterwards: nothing is written to this node's backup target or catalog, so the share
    /// takes no backup gate either (a coordinator holding its own gate while it waits for this one cannot then
    /// deadlock against a capture running here).
    /// </summary>
    internal async Task<BackupPartResult> CaptureBackupPartAsync(
        BackupPartRequest request, IBackupPartSink sink, CancellationToken ct)
    {
        foreach (int partitionId in request.Partitions)
        {
            if (!_raft.HostsPartition(partitionId))
                return BackupPartResult.Refused(BackupPartOutcome.NotHosted,
                    $"This node no longer hosts partition {partitionId}.");
        }

        if (_driver.CurrentTopologyGeneration() != request.ExpectedTopologyGeneration)
            return BackupPartResult.Refused(BackupPartOutcome.TopologyChanged,
                "This node sees another cluster topology generation than the coordinator.");

        HashSet<int> partitions = [.. request.Partitions];
        string root = CreateShareDirectory();
        PreparedIntentCommitObservation? observation = null;
        HLCTimestamp cut = HLCTimestamp.Zero;

        try
        {
            LocalDirectoryArtifactStore store = new(root);
            BackupCatalog catalog = new(new LocalDirectoryStorageTarget(root));
            BackupManifest manifest;

            if (request.Kind == BackupPartKind.Full)
            {
                if (_keyOwnership is null)
                    return BackupPartResult.Refused(BackupPartOutcome.Failed,
                        "This node cannot tell which partition owns a key, so it cannot capture a share.");

                // Opened before the cut is agreed, as a single-node coordinated backup opens it before it chooses
                // the cut, and kept open until the final check after every share is captured.
                observation = _beginCommitObservation?.Invoke();

                // This node's view of the cut: what is in flight on its partitions, and how far they committed.
                HLCTimestamp seenInFlight = await _queryMinInFlight().ConfigureAwait(false);
                cut = await sink.ProposeCutAsync(seenInFlight, NewestCommittedEntry(request.Partitions, ct), ct).ConfigureAwait(false);

                // A prepare that applied while the shares agreed can sit at or below the agreed cut; capturing at it
                // would take part of that transaction. Refused here, before any work.
                HLCTimestamp minInFlight = await _queryMinInFlight().ConfigureAwait(false);
                if (minInFlight != HLCTimestamp.Zero && minInFlight.CompareTo(cut) <= 0)
                    return BackupPartResult.Refused(BackupPartOutcome.CutUnverified,
                        $"A transaction with commit timestamp {minInFlight} is in flight at or below the cut {cut}.");

                PreparedIntentCommitObservation? observed = observation;
                BackupDriver.VerifyCutDelegate? verifyCut = observed is null
                    ? null
                    : (capturedCut, covered, verifyCt) => VerifyCoordinatedCutAsync(observed, capturedCut, covered, verifyCt);

                try
                {
                    manifest = await _driver.TakeFullBackupAsync(
                        store, catalog, cut, BuildOwnerIdentity(), verifyCut: verifyCut,
                        capturePartitions: partitions, retainKey: _keyOwnership(partitions), ct: ct).ConfigureAwait(false);
                }
                catch (BackupDriverException ex) when (ex.CutUnverified)
                {
                    return BackupPartResult.Refused(BackupPartOutcome.CutUnverified, ex.Message);
                }
            }
            else
            {
                // The driver resolves the chain it extends from a catalog; this private one holds exactly that chain.
                foreach (BackupManifest ancestor in request.ParentChain)
                    await catalog.PutAsync(ancestor, ct).ConfigureAwait(false);

                manifest = await _driver.TakeIncrementalBackupAsync(
                    request.ParentChain[^1].BackupId, store, catalog, snapshotT: null, _acquireRetentionHold,
                    BuildOwnerIdentity(), capturePartitions: partitions, ct: ct).ConfigureAwait(false);
            }

            if (manifest.TopologyGeneration is long captured && captured != request.ExpectedTopologyGeneration)
                return BackupPartResult.Refused(BackupPartOutcome.TopologyChanged,
                    "The cluster topology changed before this node's share was captured.");

            await StreamArtifactsAsync(store, manifest.BackupId, sink, ct).ConfigureAwait(false);

            if (request.Kind == BackupPartKind.Full)
            {
                // The check the driver made after this capture covers only the transactions decided before it. A
                // transaction with a write in another share's image was decided before that capture, which can be
                // later than this one: its prepare here may have applied only after this node checked. Every share
                // is captured once this returns, so a check now sees every prepare of every such transaction — live
                // in the observation, or settled before it opened and so in this image.
                await sink.CapturedAsync(manifest, ct).ConfigureAwait(false);

                if (observation is not null)
                {
                    try
                    {
                        if (!await VerifyCoordinatedCutAsync(observation, cut, request.Partitions, ct).ConfigureAwait(false))
                            return BackupPartResult.Refused(BackupPartOutcome.CutUnverified,
                                $"A transaction at or below the cut {cut} was in flight on this node while the backup's shares were captured.");
                    }
                    catch (BackupDriverException ex) when (ex.CutUnverified)
                    {
                        return BackupPartResult.Refused(BackupPartOutcome.CutUnverified, ex.Message);
                    }
                }
            }

            return new BackupPartResult(BackupPartOutcome.Ok, null, manifest);
        }
        catch (BackupDriverException ex) when (ex.TopologyChanged)
        {
            return BackupPartResult.Refused(BackupPartOutcome.TopologyChanged, ex.Message);
        }
        catch (BackupDriverException ex) when (ex.NeedsFullBackup)
        {
            return BackupPartResult.Refused(BackupPartOutcome.NeedsFull, ex.Message);
        }
        catch (BackupDriverException ex) when (ex.NotCaughtUp)
        {
            // The message names only partition ids and timestamps, so the coordinator can return it to its caller.
            return BackupPartResult.Refused(BackupPartOutcome.NotCaughtUp, ex.Message, ex.LaggingPartitions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The detail (paths, backend text) stays in this node's log; the coordinator gets the correlation id.
            string operationId = Guid.NewGuid().ToString("N")[..12];
            LogOperationFailure(operationId, ex);
            return BackupPartResult.Refused(BackupPartOutcome.Failed,
                $"the capture failed on that node (operation {operationId} in its log)");
        }
        finally
        {
            observation?.Dispose();
            TryDeleteDirectory(root);
        }
    }

    /// <summary>The newest committed entry time of <paramref name="partitionIds"/> in this node's WAL; zero when none.
    /// An entry's time is at or after the commit timestamp it carries, so a cut at this time holds every write
    /// these partitions committed.</summary>
    private HLCTimestamp NewestCommittedEntry(IReadOnlyList<int> partitionIds, CancellationToken ct)
    {
        HLCTimestamp newest = HLCTimestamp.Zero;
        foreach (int partitionId in partitionIds)
        {
            (_, HLCTimestamp time, _) = BackupDriver.FindLastCommitted(_raft.WalAdapter, partitionId, ct);
            if (time.CompareTo(newest) > 0)
                newest = time;
        }
        return newest;
    }

    private static async Task StreamArtifactsAsync(
        IBackupArtifactStore store, Guid backupId, IBackupPartSink sink, CancellationToken ct)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(ShareChunkBytes);
        try
        {
            foreach (BackupArtifactEntry entry in await store.ListAsync(backupId, ct).ConfigureAwait(false))
            {
                await sink.BeginFileAsync(entry.RelativePath, ct).ConfigureAwait(false);

                await using (Stream source = await store.OpenReadAsync(backupId, entry.RelativePath, ct: ct).ConfigureAwait(false))
                {
                    int read;
                    while ((read = await source.ReadAsync(buffer.AsMemory(0, ShareChunkBytes), ct).ConfigureAwait(false)) > 0)
                        await sink.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                }

                await sink.EndFileAsync(ct).ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// A private, owner-only directory for one share: under the configured backup scratch directory when there
    /// is one, else under the system temp directory. Never under the backup root, where another node's sweep
    /// could see it.
    /// </summary>
    private string CreateShareDirectory()
    {
        string path = Path.Combine(_partScratchRoot ?? Path.GetTempPath(), SharePrefix + Guid.NewGuid().ToString("N"));
        BackupFilePermissions.CreateDirectory(path);
        return path;
    }

    // ── the shares, on the node that composes them ─────────────────────────────────────────────────

    /// <summary>
    /// Captures every share in parallel into <paramref name="received"/>. The first refusal or failure cancels
    /// the others. Every failed share is reported, by its partitions and node.
    /// </summary>
    private async Task<List<ReceivedShare>> CaptureSharesAsync(
        List<PartAssignment> shares, Func<PartAssignment, BackupPartRequest> requestFor,
        LocalDirectoryArtifactStore received, CutRendezvous? cutRendezvous, CancellationToken ct)
    {
        using CancellationTokenSource abort = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // Full shares make their final cut check only once every share is captured (see IBackupPartSink).
        CaptureBarrier barrier = new(shares.Count);

        Task<ShareOutcome>[] captures = new Task<ShareOutcome>[shares.Count];
        for (int i = 0; i < shares.Count; i++)
            captures[i] = CaptureShareAsync(shares[i], requestFor(shares[i]), received, barrier, cutRendezvous, abort);

        ShareOutcome[] outcomes = await Task.WhenAll(captures).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();

        List<ReceivedShare> accepted = new(outcomes.Length);
        List<ShareOutcome> failed = [];
        foreach (ShareOutcome outcome in outcomes)
        {
            if (outcome.Result is { Outcome: BackupPartOutcome.Ok, Manifest: { } manifest })
            {
                manifest.BackupId = outcome.ReceiveId;
                accepted.Add(new ReceivedShare(outcome.Assignment, manifest));
            }
            // A share cancelled only because another one failed first is not a failure of its own.
            else if (outcome.Error is not OperationCanceledException)
                failed.Add(outcome);
        }

        if (failed.Count == 0 && accepted.Count == shares.Count)
            return accepted;

        throw ShareFailure(failed.Count > 0 ? failed : [.. outcomes.Where(o => o.Result?.Outcome != BackupPartOutcome.Ok)]);
    }

    private sealed record ShareOutcome(PartAssignment Assignment, Guid ReceiveId, BackupPartResult? Result, Exception? Error);

    private async Task<ShareOutcome> CaptureShareAsync(
        PartAssignment share, BackupPartRequest request, LocalDirectoryArtifactStore received,
        CaptureBarrier barrier, CutRendezvous? cutRendezvous, CancellationTokenSource abort)
    {
        Guid receiveId = Guid.NewGuid();
        await using ShareReceiver receiver = new(received, receiveId, barrier, cutRendezvous, abort.Token);

        try
        {
            BackupPartResult result;
            if (share.IsLocal)
                result = await CaptureBackupPartAsync(request, receiver, abort.Token).ConfigureAwait(false);
            else if (_interNode is null)
                result = BackupPartResult.Refused(BackupPartOutcome.Failed, "this node has no inter-node transport");
            else
                result = await _interNode.CaptureBackupPart(share.Node!, request, receiver, abort.Token).ConfigureAwait(false);

            if (result.Outcome != BackupPartOutcome.Ok)
                await abort.CancelAsync().ConfigureAwait(false);

            return new ShareOutcome(share, receiveId, result, null);
        }
        catch (Exception ex)
        {
            await abort.CancelAsync().ConfigureAwait(false);
            return new ShareOutcome(share, receiveId, null, ex);
        }
    }

    /// <summary>
    /// The one failure a set of failed shares amounts to. A cut or topology refusal is retryable with a new
    /// attempt, so it wins over a plain failure. A share whose node had not applied its partitions to the cut is
    /// retryable from another replica; it is reported as such when every failed share is of that kind, and as a
    /// plain failure otherwise. In every case the failed shares are named, and the replicas a retry should avoid
    /// (those of the nodes that could not be reached, and of the nodes that had not applied) ride along.
    /// </summary>
    private BackupDriverException ShareFailure(IReadOnlyList<ShareOutcome> failed)
    {
        bool cut = false, topology = false, needsFull = false;
        int notCaughtUp = 0;
        List<string> named = new(failed.Count);
        List<(string Node, int PartitionId)> replicasToAvoid = [];

        foreach (ShareOutcome outcome in failed)
        {
            string partitions = string.Join(", ", outcome.Assignment.Partitions);
            string node = outcome.Assignment.Node ?? "(none)";

            string reason;
            if (outcome.Result is { } result)
            {
                switch (result.Outcome)
                {
                    case BackupPartOutcome.CutUnverified:
                        cut = true;
                        break;
                    case BackupPartOutcome.TopologyChanged or BackupPartOutcome.NotHosted:
                        topology = true;
                        break;
                    case BackupPartOutcome.NeedsFull:
                        needsFull = true;
                        break;
                    case BackupPartOutcome.NotCaughtUp:
                        notCaughtUp++;
                        // Only the partitions the node named move; the rest of its share stays with it next time.
                        if (result.LaggingPartitions.Count > 0)
                            foreach (int partitionId in result.LaggingPartitions)
                                replicasToAvoid.Add((node, partitionId));
                        else
                            AvoidReplicasOf(replicasToAvoid, outcome.Assignment);
                        break;
                }

                reason = result.Outcome switch
                {
                    BackupPartOutcome.NotConfigured => "backup is not configured on that node",
                    BackupPartOutcome.Failed => result.Message ?? "the capture failed on that node",
                    BackupPartOutcome.NotCaughtUp => result.Message ?? "the node had not applied every committed write of its partitions to the cut",
                    _ => result.Outcome.ToString()
                };
            }
            else
            {
                string operationId = Guid.NewGuid().ToString("N")[..12];
                LogOperationFailure(operationId, outcome.Error!);
                reason = $"the node could not be reached or its share did not arrive (operation {operationId})";
                if (!outcome.Assignment.IsLocal && outcome.Assignment.Node is not null)
                    AvoidReplicasOf(replicasToAvoid, outcome.Assignment);
            }

            named.Add($"partition(s) {partitions} on node {node}: {reason}");
        }

        string message = "A share of the cluster-wide backup could not be captured, so nothing was published: " +
                         string.Join("; ", named) + ".";

        if (cut)
            return new BackupDriverException(message) { CutUnverified = true, ReplicasToAvoid = replicasToAvoid };
        if (topology)
            return new BackupDriverException(message) { TopologyChanged = true, ReplicasToAvoid = replicasToAvoid };
        if (needsFull)
            return new BackupDriverException(message) { NeedsFullBackup = true, ReplicasToAvoid = replicasToAvoid };
        if (notCaughtUp == failed.Count)
            return new BackupDriverException(message) { NotCaughtUp = true, ReplicasToAvoid = replicasToAvoid };
        return new BackupDriverException(message) { PartitionCaptureFailed = true, ReplicasToAvoid = replicasToAvoid };
    }

    /// <summary>Marks every replica of <paramref name="share"/> (its node, each of its partitions) as one a retry avoids.</summary>
    private static void AvoidReplicasOf(List<(string Node, int PartitionId)> replicasToAvoid, PartAssignment share)
    {
        foreach (int partitionId in share.Partitions)
            replicasToAvoid.Add((share.Node!, partitionId));
    }

    /// <summary>
    /// Refuses a share whose manifest does not describe exactly what it was asked for. The topology and cluster
    /// set are retryable mismatches; anything else means the node answered with something else entirely.
    /// </summary>
    private void ValidateShare(
        ReceivedShare share, BackupType kind, long expectedGeneration, HLCTimestamp? cut, Guid? parentId)
    {
        BackupManifest m = share.Manifest;
        string who = $"partition(s) {string.Join(", ", share.Assignment.Partitions)} on node {share.Assignment.Node}";

        if (m.TopologyGeneration is long generation && generation != expectedGeneration)
            throw new BackupDriverException(
                $"The share of {who} was captured at another cluster topology generation; nothing was published.")
                { TopologyChanged = true };

        bool coversAssignment = m.CoveredPartitions is { } covered
                                && covered.Count == share.Assignment.Partitions.Count
                                && covered.All(share.Assignment.Partitions.Contains);

        string? mismatch =
            m.Type != kind ? $"it is a {m.Type} backup" :
            m.StorageType is { } type && !string.Equals(type, _storageType, StringComparison.Ordinal)
                ? $"it holds '{type}' storage, not '{_storageType}'" :
            !coversAssignment ? "it does not cover exactly the partitions assigned to it" :
            cut is { } expectedCut && m.BaseCut != expectedCut ? "it was cut at another timestamp" :
            parentId is { } expectedParent && m.ParentBackupId != expectedParent ? "it extends another parent" :
            null;

        if (mismatch is not null)
            throw new BackupDriverException(
                $"A share of the cluster-wide backup could not be used, so nothing was published: {who}: {mismatch}.")
                { PartitionCaptureFailed = true };
    }

    /// <summary>Every share must have seen the same partition set, and their covered sets must add up to it.</summary>
    private static List<int> ClusterSetOf(IReadOnlyList<ReceivedShare> shares, IReadOnlyCollection<int> mustCover)
    {
        List<int> cluster = [.. (shares[0].Manifest.ClusterPartitions ?? []).Order()];

        HashSet<int> covered = [];
        foreach (ReceivedShare share in shares)
        {
            if (!(share.Manifest.ClusterPartitions ?? []).Order().SequenceEqual(cluster))
                throw new BackupDriverException(
                    "The shares of the cluster-wide backup saw different partition sets; nothing was published.")
                    { TopologyChanged = true };

            foreach (int partitionId in share.Manifest.CoveredPartitions!)
                if (!covered.Add(partitionId))
                    throw new BackupDriverException(
                        $"Partition {partitionId} is in two shares of the cluster-wide backup; nothing was published.")
                        { PartitionCaptureFailed = true };
        }

        List<int> missing = [.. mustCover.Where(p => !covered.Contains(p)).Order()];
        if (missing.Count > 0)
            throw new BackupDriverException(
                $"No share of the cluster-wide backup covers partition(s) {string.Join(", ", missing)}; nothing was published.")
                { TopologyChanged = true };

        return cluster;
    }

    // ── composed full ───────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Captures one share per hosting node at one cut the shares agree on, merges their checkpoints and intent
    /// frontiers into one image under a new backup id, and publishes it after the coordinator fence and a
    /// topology re-check. On any failure the new id's artifacts are removed and nothing is published.
    /// </summary>
    private async Task<BackupManifest> TakeComposedFullAsync(
        List<PartAssignment> shares, Func<CancellationToken, Task<bool>> stillCoordinator,
        CancellationToken ct)
    {
        EnsureAssignable(shares, fullBackup: true);

        long expectedGeneration = _driver.CurrentTopologyGeneration();
        List<int> active = [.. shares.SelectMany(s => s.Partitions)];

        string workRoot = CreateShareDirectory();
        Guid backupId = Guid.NewGuid();
        bool published = false;

        try
        {
            LocalDirectoryArtifactStore received = new(workRoot);
            CutRendezvous rendezvous = new(shares.Count);

            List<ReceivedShare> parts = await CaptureSharesAsync(
                shares,
                share => new BackupPartRequest(BackupPartKind.Full, share.Partitions, expectedGeneration, []),
                received, rendezvous, ct).ConfigureAwait(false);

            HLCTimestamp snapshotT = rendezvous.Cut;

            foreach (ReceivedShare part in parts)
            {
                ValidateShare(part, BackupType.Full, expectedGeneration, snapshotT, parentId: null);
                await BackupArtifactVerifier.VerifyAsync(part.Manifest, received, ct).ConfigureAwait(false);
            }

            List<int> cluster = ClusterSetOf(parts, active);

            // The local share first: its files already carry this node's storage revision, the one the
            // published image is named with.
            parts.Sort(static (a, b) => b.Assignment.IsLocal.CompareTo(a.Assignment.IsLocal));

            Dictionary<string, string> checksums;
            Dictionary<string, long> sizes;
            await using (IBackupCheckpointStagingArea staging =
                         await _artifacts.BeginCheckpointAsync(backupId, ct).ConfigureAwait(false))
            {
                await received.MaterializeAsync(
                    parts[0].Manifest.BackupId, LocalDirectoryArtifactStore.CheckpointDirectoryName, staging.LocalPath,
                    ct: ct).ConfigureAwait(false);

                if (!parts[0].Assignment.IsLocal)
                    CheckpointComposer.AdoptRevision(_storageType, _storageRevision, staging.LocalPath);

                for (int i = 1; i < parts.Count; i++)
                    CheckpointComposer.MergeInto(
                        _storageType, _storageRevision, staging.LocalPath,
                        Path.Combine(workRoot, parts[i].Manifest.BackupId.ToString("N"), LocalDirectoryArtifactStore.CheckpointDirectoryName),
                        ct);

                (checksums, sizes) = BackupArtifactVerifier.HashDirectory(staging.LocalPath, "checkpoint/", ct);
                BackupFilePermissions.RestrictTree(staging.LocalPath);
                await staging.CommitAsync(ct).ConfigureAwait(false);
            }

            List<IntentFrontier> frontiers = [];
            foreach (ReceivedShare part in parts)
            {
                if (!part.Manifest.Checksums.ContainsKey(IntentFrontier.ArtifactName))
                    continue;

                await using Stream stream = await received.OpenReadAsync(
                    part.Manifest.BackupId, IntentFrontier.ArtifactName, ct: ct).ConfigureAwait(false);
                frontiers.Add(IntentFrontier.Deserialize(stream));
            }

            IntentFrontier frontier = IntentFrontier.Merge(frontiers);
            if (!frontier.IsEmpty)
            {
                (string digest, long length) = await BackupDriver.WriteIntentFrontierAsync(_artifacts, backupId, frontier, ct).ConfigureAwait(false);
                checksums[IntentFrontier.ArtifactName] = digest;
                sizes[IntentFrontier.ArtifactName] = length;
            }

            List<PartitionBackupRange> ranges = [.. parts.SelectMany(p => p.Manifest.PartitionRanges).OrderBy(r => r.PartitionId)];

            BackupManifest manifest = BackupManifest.CreateFull(ranges);
            manifest.BackupId = backupId;
            manifest.Checksums = checksums;
            manifest.Sizes = sizes;
            manifest.ClusterPartitions = cluster;
            manifest.CoveredPartitions = [.. parts.SelectMany(p => p.Manifest.CoveredPartitions!).Order()];
            manifest.SetBaseCut(snapshotT);
            manifest.ApplyOwnerIdentity(BuildOwnerIdentity());
            manifest.TopologyGeneration = expectedGeneration;
            manifest.SetClusterSnapshotTime(snapshotT);

            await BackupArtifactVerifier.VerifyAsync(manifest, _artifacts, ct).ConfigureAwait(false);

            await EnsurePublishableAsync(expectedGeneration, stillCoordinator, ct).ConfigureAwait(false);

            ManifestSigner?.Invoke(manifest);
            await _catalog.PutAsync(manifest, ct).ConfigureAwait(false);
            published = true;

            if (_logger is not null && _logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "Cluster-wide backup {BackupId} composed {Shares} share(s) covering {Partitions} partition(s) at cut {Cut}.",
                    backupId, parts.Count, manifest.CoveredPartitions.Count, snapshotT);

            return manifest;
        }
        finally
        {
            if (!published)
                await TryDeleteArtifactsAsync(backupId).ConfigureAwait(false);

            TryDeleteDirectory(workRoot);
        }
    }

    private async Task EnsurePublishableAsync(
        long expectedGeneration, Func<CancellationToken, Task<bool>>? stillCoordinator, CancellationToken ct)
    {
        if (_driver.CurrentTopologyGeneration() != expectedGeneration)
            throw new BackupDriverException(
                "The cluster topology (partition map or membership) changed during the cluster-wide backup; the " +
                "shares are not one consistent snapshot, so the backup was not published.")
                { TopologyChanged = true };

        if (stillCoordinator is not null && !await stillCoordinator(ct).ConfigureAwait(false))
            throw new BackupDriverException(
                "Meta-partition leadership was lost during the coordinated backup; this node is no longer " +
                "the backup coordinator, so the backup was not published.")
                { RetryableLeadershipLoss = true };
    }

    private async Task TryDeleteArtifactsAsync(Guid backupId)
    {
        try { await _artifacts.DeleteAllAsync(backupId).ConfigureAwait(false); }
        catch { /* leftovers with no manifest are reclaimed by the orphan sweep */ }
    }

    // ── composed incremental ────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The incremental of a chain that covers partitions this node does not host, composed from one share per
    /// hosting node. When the chain cannot be extended (compaction or topology), the substitute is a full that
    /// covers the same partitions — the coordinated one, which only the backup coordinator may take.
    /// </summary>
    private async Task<KahunaBackupInfo> TakeComposedIncrementalOrSubstituteAsync(
        IReadOnlyList<BackupManifest> parentChain, List<PartAssignment> shares, long start, CancellationToken ct)
    {
        KahunaBackupInfo dto;
        try
        {
            BackupManifest manifest = await TakeComposedIncrementalAsync(parentChain, shares, ct).ConfigureAwait(false);
            RecordBackupSuccess(manifest, start);
            dto = ToDto(manifest);
            dto.RequestedKind = BackupType.Incremental.ToString();
        }
        catch (BackupDriverException ex) when (ex.NeedsFullBackup)
        {
            string cause = ex.TopologyDivergedFromParent
                ? "the cluster topology (partition map or membership) changed since the parent chain was taken"
                : "the WAL compaction floor advanced past the parent backup's range on a node that captures part of it";

            if (!await _raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
                throw new KahunaBackupException(KahunaBackupOutcome.NotBackupCoordinator,
                    $"An incremental could not be produced because {cause}. The parent chain spans several nodes, so " +
                    "the replacement full backup must be a coordinated one; retry against the meta-partition leader.");

            _logger?.LogWarning(
                "Incremental backup from {ParentId} not possible: {Cause}. Falling back to a coordinated full backup. Reason: {Message}",
                parentChain[^1].BackupId, cause, ex.Message);

            BackupManifest full = await TakeCoordinatedLockedAsync(ct).ConfigureAwait(false);
            RecordBackupSuccess(full, start);
            dto = ToDto(full);
            dto.RequestedKind = BackupType.Incremental.ToString();
            dto.SubstitutionReason =
                $"An incremental could not be produced because {cause}; a coordinated full backup was taken instead. {ex.Message}";
        }

        await TryRunGcLockedAsync(ct).ConfigureAwait(false);
        return dto;
    }

    private async Task<BackupManifest> TakeComposedIncrementalAsync(
        IReadOnlyList<BackupManifest> parentChain, List<PartAssignment> shares, CancellationToken ct)
    {
        long expectedGeneration = _driver.CurrentTopologyGeneration();

        // Checked here before any share runs: every share would refuse anyway, one by one.
        foreach (BackupManifest ancestor in parentChain)
        {
            if (ancestor.TopologyGeneration is long recorded && recorded != expectedGeneration)
                throw new BackupDriverException(
                    $"The cluster topology changed since backup {ancestor.BackupId:N} was taken; an incremental " +
                    "cannot extend that chain, so a new full backup is required.")
                    { NeedsFullBackup = true, TopologyDivergedFromParent = true };
        }

        EnsureAssignable(shares, fullBackup: false);

        BackupManifest parent = parentChain[^1];
        List<int> mustCover = [.. shares.SelectMany(s => s.Partitions)];

        string workRoot = CreateShareDirectory();
        Guid backupId = Guid.NewGuid();
        bool published = false;

        try
        {
            LocalDirectoryArtifactStore received = new(workRoot);

            List<ReceivedShare> parts = await CaptureSharesAsync(
                shares,
                share => new BackupPartRequest(BackupPartKind.Incremental, share.Partitions, expectedGeneration, parentChain),
                received, cutRendezvous: null, ct).ConfigureAwait(false);

            foreach (ReceivedShare part in parts)
            {
                ValidateShare(part, BackupType.Incremental, expectedGeneration, cut: null, parentId: parent.BackupId);
                await BackupArtifactVerifier.VerifyAsync(part.Manifest, received, ct).ConfigureAwait(false);
            }

            List<int> cluster = ClusterSetOf(parts, mustCover);

            // Segments are per-partition files, so the shares' artifacts are disjoint: each is copied as it is.
            Dictionary<string, string> checksums = [];
            Dictionary<string, long> sizes = [];
            foreach (ReceivedShare part in parts)
            {
                foreach ((string path, string digest) in part.Manifest.Checksums)
                {
                    if (!checksums.TryAdd(path, digest))
                        throw new BackupDriverException(
                            $"Two shares of the cluster-wide incremental hold the artifact '{path}'; nothing was published.")
                            { PartitionCaptureFailed = true };

                    sizes[path] = part.Manifest.Sizes[path];
                    await CopyArtifactAsync(received, part.Manifest.BackupId, backupId, path, ct).ConfigureAwait(false);
                }
            }

            List<PartitionBackupRange> ranges = [.. parts.SelectMany(p => p.Manifest.PartitionRanges).OrderBy(r => r.PartitionId)];

            BackupManifest manifest = BackupManifest.CreateIncremental(parent.BackupId, ranges);
            manifest.BackupId = backupId;
            manifest.Checksums = checksums;
            manifest.Sizes = sizes;
            manifest.ClusterPartitions = cluster;
            manifest.CoveredPartitions = [.. parts.SelectMany(p => p.Manifest.CoveredPartitions!).Order()];
            manifest.ApplyOwnerIdentity(BuildOwnerIdentity());
            manifest.TopologyGeneration = expectedGeneration;

            await BackupArtifactVerifier.VerifyAsync(manifest, _artifacts, ct).ConfigureAwait(false);

            await EnsurePublishableAsync(expectedGeneration, stillCoordinator: null, ct).ConfigureAwait(false);

            ManifestSigner?.Invoke(manifest);
            await _catalog.PutAsync(manifest, ct).ConfigureAwait(false);
            published = true;
            return manifest;
        }
        finally
        {
            if (!published)
                await TryDeleteArtifactsAsync(backupId).ConfigureAwait(false);

            TryDeleteDirectory(workRoot);
        }
    }

    private async Task CopyArtifactAsync(
        IBackupArtifactStore source, Guid sourceId, Guid targetId, string relativePath, CancellationToken ct)
    {
        await using Stream input = await source.OpenReadAsync(sourceId, relativePath, ct: ct).ConfigureAwait(false);
        await using IBackupArtifactWriter writer = await _artifacts.OpenWriteAsync(targetId, relativePath, ct).ConfigureAwait(false);
        await input.CopyToAsync(writer.Stream, ShareChunkBytes, ct).ConfigureAwait(false);
        await writer.CompleteAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Collects every full share's view of the cut and answers all of them with one cut, chosen by the rule a
    /// single node applies to everything it hosts: just below the lowest commit timestamp in flight anywhere,
    /// or, when nothing is in flight, the newest committed entry anywhere. A share that fails before it proposes
    /// abandons the backup, which cancels every wait.
    /// </summary>
    private sealed class CutRendezvous(int parties)
    {
        private readonly Lock gate = new();

        private readonly TaskCompletionSource<HLCTimestamp> agreed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int remaining = parties;

        private HLCTimestamp minInFlight = HLCTimestamp.Zero;

        private HLCTimestamp maxCommitted = HLCTimestamp.Zero;

        /// <summary>The agreed cut; valid once every share has proposed.</summary>
        public HLCTimestamp Cut => agreed.Task.IsCompletedSuccessfully
            ? agreed.Task.Result
            : throw new InvalidOperationException("The shares have not agreed on a cut.");

        public async ValueTask<HLCTimestamp> ProposeAsync(HLCTimestamp seenInFlight, HLCTimestamp committed, CancellationToken ct)
        {
            lock (gate)
            {
                if (seenInFlight != HLCTimestamp.Zero && (minInFlight == HLCTimestamp.Zero || seenInFlight.CompareTo(minInFlight) < 0))
                    minInFlight = seenInFlight;
                if (committed.CompareTo(maxCommitted) > 0)
                    maxCommitted = committed;

                if (--remaining == 0)
                {
                    agreed.TrySetResult(minInFlight != HLCTimestamp.Zero ? SnapshotCoordinator.Predecessor(minInFlight) : maxCommitted);
                }
            }

            return await agreed.Task.WaitAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Releases every full share's final cut check at once, when the last share reports its capture. A share
    /// that fails before it arrives abandons the backup, which cancels every wait.
    /// </summary>
    private sealed class CaptureBarrier(int parties)
    {
        private int remaining = parties;

        private readonly TaskCompletionSource all = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask ArriveAndWaitAsync(CancellationToken ct)
        {
            if (Interlocked.Decrement(ref remaining) == 0)
                all.TrySetResult();

            await all.Task.WaitAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Writes a streamed share into the composition's private store, one artifact at a time. Each artifact is
    /// published only when it completes, so a share cut off mid-file leaves nothing that looks whole; the
    /// digests in the share's manifest are then checked against what arrived.
    /// </summary>
    private sealed class ShareReceiver(
        LocalDirectoryArtifactStore store, Guid receiveId, CaptureBarrier barrier, CutRendezvous? cutRendezvous,
        CancellationToken abandoned)
        : IBackupPartSink, IAsyncDisposable
    {
        private IBackupArtifactWriter? current;

        private int captured;

        private int proposed;

        public async ValueTask<HLCTimestamp> ProposeCutAsync(HLCTimestamp minInFlight, HLCTimestamp maxCommitted, CancellationToken ct)
        {
            if (cutRendezvous is null || Interlocked.Exchange(ref proposed, 1) != 0)
                throw new BackupArtifactException("A share proposed a cut where none is agreed, or proposed twice.");

            using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(ct, abandoned);
            return await cutRendezvous.ProposeAsync(minInFlight, maxCommitted, linked.Token).ConfigureAwait(false);
        }

        public async ValueTask CapturedAsync(BackupManifest manifest, CancellationToken ct)
        {
            if (current is not null || Interlocked.Exchange(ref captured, 1) != 0)
                throw new BackupArtifactException("A share reported its capture twice, or inside an artifact.");

            using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(ct, abandoned);
            await barrier.ArriveAndWaitAsync(linked.Token).ConfigureAwait(false);
        }

        public async ValueTask BeginFileAsync(string relativePath, CancellationToken ct)
        {
            if (current is not null)
                throw new BackupArtifactException("A share started an artifact before it completed the previous one.");

            current = await store.OpenWriteAsync(receiveId, relativePath, ct).ConfigureAwait(false);
        }

        public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
        {
            if (current is null)
                throw new BackupArtifactException("A share sent bytes outside an artifact.");

            await current.Stream.WriteAsync(data, ct).ConfigureAwait(false);
        }

        public async ValueTask EndFileAsync(CancellationToken ct)
        {
            IBackupArtifactWriter writer = current
                ?? throw new BackupArtifactException("A share completed an artifact it never started.");
            current = null;

            try { await writer.CompleteAsync(ct).ConfigureAwait(false); }
            finally { await writer.DisposeAsync().ConfigureAwait(false); }
        }

        public async ValueTask DisposeAsync()
        {
            if (current is not null)
            {
                await current.DisposeAsync().ConfigureAwait(false);
                current = null;
            }
        }
    }
}
