using System.Collections.Concurrent;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Every durable-2PC write leaves the node through here: routing an entry to its partition leader,
/// submitting it through the write scheduler (fenced on a range lock where the caller supplies one),
/// bundling an anchor's init+prepare into a single atomic batch, applying the committed entries, and
/// looking a transaction record back up.
///
/// The bundle calls return two independent signals — whether the batch committed, and whether the
/// prepare inside it was acknowledged — because a committed batch whose prepare was not acknowledged
/// must fall back to the unbundled path rather than be treated as a finished prepare.
/// </summary>
internal sealed class DurableReplicationGateway
{
    private readonly KeyValuesRuntime runtime;

    private readonly KeyValueReplicator replicator;

    internal DurableReplicationGateway(KeyValuesRuntime runtime, KeyValueReplicator replicator)
    {
        this.runtime = runtime;
        this.replicator = replicator;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private IInterNodeCommunication interNodeCommunication => runtime.InterNodeCommunication;

    private KeyValueLocator locator => runtime.Locator;

    private PartitionWriteAggregator writeAggregator => runtime.WriteAggregator;

    private TransactionRecordStore transactionRecordStore => runtime.TransactionRecordStore;

    private PreparedIntentStore preparedIntentStore => runtime.PreparedIntentStore;

    private DurableApplyResultLedger durableApplyResults => runtime.DurableApplyResults;

    internal (int PartitionId, long Generation) LocateDurablePartition(string key) => locator.LocateRange(key);

    /// <summary>
    /// Replicates a durable-intent 2PC delta through the shared partition write scheduler so it coalesces with
    /// concurrent transactions' records to the same partition into one <c>ReplicateEntries</c> proposal. Returns
    /// true once the batch carrying it committed; false on scheduler backpressure (the finalizer maps that to a
    /// retryable outcome). The caller applies the committed delta to its local store, matching the direct-write
    /// path where the producer applies on completion.
    /// </summary>
    // Durable-operation kinds carried across the inter-node forwarding to the partition leader.
    private const int DurableOpReplicate = 0;
    private const int DurableOpCommit = 1;
    private const int DurableOpRollback = 2;

    /// <summary>
    /// Resolves whether a durable operation for <paramref name="partitionId"/> must run on a remote leader.
    /// Returns null when this node is the partition leader (or standalone) — run locally; otherwise the remote
    /// leader endpoint to forward to. The durable path proposes via the local scheduler and clears the staged write
    /// intent locally, both of which require the local node to be the partition leader, so on a cluster a coordinator
    /// that is not the leader forwards the operation to the leader (where records also coalesce across coordinators).
    /// </summary>
    private async Task<string?> ResolveDurableLeader(int partitionId, CancellationToken cancellationToken)
    {
        // Durable-path routing is always a sub-operation the serving node initiates on its own
        // behalf (a 2PC leg to a participant leader, an anchor-record lookup for a foreign intent),
        // never a re-forward of the request being served — so the forwarded-request loop guard must
        // not apply. Without this, any durable work reached through forwarding (a forwarded read
        // meeting an unsettled foreign intent, a forwarded commit fanning out its prepares) could
        // not route to partitions this node does not host.
        using ForwardedRequestScope.SuppressScope _ = ForwardedRequestScope.Suppress();

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
            return null;

        // For a partition this node does not host, the resolver answers a replica target (the
        // receiver redirects to its own leader if needed). A null target means the operation
        // cannot be routed from here at all — surface the typed retryable condition rather than
        // running the durable op locally, which requires hosting the partition.
        string? leader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is null)
            throw new PartitionNotHostedException(partitionId);

        return leader == raft.GetLocalEndpoint() ? null : leader;
    }

    internal async Task<bool> ReplicateDurableThroughScheduler(int partitionId, string logType, byte[] data, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken, bool projectRecordLocally = true)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
        {
            bool ok = await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpReplicate, logType, data, cancellationToken).ConfigureAwait(false);

            // The authoritative apply happened on the remote leader (its scheduler is the single ordered owner).
            // Keep a local projection of the canonical record only, so this node's own decision read-back and
            // lost-session consult resolve locally until the anchor-routed record lookup replaces it. The record
            // identity is this transaction's own, so the projection never misrepresents another transaction;
            // prepared intents are not projected (a remote conflict is already reflected in a false result here).
            //
            // The projection is only sound for a delta whose sender is the transition's sole author — the
            // coordinator projecting its own decision. A recovery-driven presumed abort must pass
            // projectRecordLocally: false — its abort can LOSE at the anchor to a commit that already won, and
            // projecting the losing abort into a store that holds no record for the transaction would mint a
            // local abort tombstone that diverges from the canonical commit, misleads this node's scan-decision
            // lookups, and hides the committed value.
            if (ok && projectRecordLocally && logType == ReplicationTypes.TransactionRecord)
                transactionRecordStore.Replicate(partitionId, new RaftLog { LogType = logType, LogData = data });

            return ok;
        }

        return await ReplicateDurableLocal(partitionId, logType, data, admissionClass, fenceKey: null, fenceGeneration: 0, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Like <see cref="ReplicateDurableThroughScheduler"/> but re-fences the local submission at dispatch against
    /// the range descriptor <paramref name="fenceKey"/> resolved to at freeze (<paramref name="fenceGeneration"/>):
    /// a split/merge between freeze and dispatch releases it retryably instead of appending to a retired partition.
    /// The re-fence applies on the local aggregator path; a forward to a remote leader carries no fence (the
    /// remote's freeze-to-dispatch race is a follow-up).
    /// </summary>
    internal async Task<bool> ReplicateDurableThroughSchedulerFenced(int partitionId, string logType, byte[] data, string fenceKey, long fenceGeneration, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken, bool projectRecordLocally = true)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
        {
            bool ok = await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpReplicate, logType, data, cancellationToken).ConfigureAwait(false);

            // Same projection contract as the unfenced variant: sound only for a delta whose sender is the
            // transition's sole author. A terminal DECISION forwarded to a remote leader must pass
            // projectRecordLocally: false — the remote apply can reject it in favour of a decision that
            // already won (a routed presumed abort racing a commit, or the reverse), and `ok` reports only
            // that the batch replicated, not that the transition applied. Projecting the losing delta mints
            // a local record that permanently diverges from the canonical one (the canonical winner's later
            // replica apply is rejected by the terminal-transition rules), and every local consumer of that
            // store — the scan-decision overlay, a settle pass, a decision read-back — then acts on the
            // wrong outcome.
            if (ok && projectRecordLocally && logType == ReplicationTypes.TransactionRecord)
                transactionRecordStore.Replicate(partitionId, new RaftLog { LogType = logType, LogData = data });

            return ok;
        }

        return await ReplicateDurableLocal(partitionId, logType, data, admissionClass, fenceKey, fenceGeneration, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> ReplicateDurableLocal(int partitionId, string logType, byte[] data, Writes.WriteAdmissionClass admissionClass, string? fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Writes.DurableProposalSubmission submission = new(
            partitionId,
            [new RaftProposalEntry(logType, data, AutoCommit: true, ExpectedGeneration: 0)],
            completion,
            admissionClass,
            ApplyDurableEntriesOnCommit,
            fenceKey,
            fenceGeneration
        );

        if (!writeAggregator.TryEnqueue(submission))
            return false;

        using CancellationTokenRegistration _ = cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetResult(false), completion);
        return await submission.Committed.ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates the anchor partition's <c>[TransactionRecord init, PreparedIntent prepare]</c> as one atomic
    /// ordered proposal, removing a pre-decision Raft barrier: the record initialization no longer costs a separate
    /// round trip before the anchor partition's prepare. Fenced against the anchor descriptor <paramref name="fenceKey"/>
    /// at <paramref name="fenceGeneration"/> like the standalone pre-decision path.
    /// <para>Returns two independent signals the caller cannot fold into one: <c>BatchCommitted</c> — the proposal
    /// reached Raft, so the record is durably initialized (Undecided); and <c>PrepareAcknowledged</c> — the anchor
    /// prepare took ownership of its key. A committed batch with a rejected prepare must drive a truthful abort
    /// (the record exists), whereas a batch that never committed is a clean retry with nothing durable.</para>
    /// </summary>
    internal async Task<(bool BatchCommitted, bool PrepareAcknowledged)> ReplicateDurableBundleThroughSchedulerFenced(
        int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
        {
            // The durable-operation wire carries a single (logType, data) per call, so the two-entry atomic bundle
            // cannot cross to a remote leader as one proposal. Forward the record init and the anchor prepare as the
            // two sequential ops they were before this optimization — the bundle win is the local-leader path (the
            // embedded single-node target); a remote atomic bundle needs a wire change and is a follow-up.
            bool initOk = await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpReplicate, ReplicationTypes.TransactionRecord, recordInitDelta, cancellationToken).ConfigureAwait(false);
            if (!initOk)
                return (false, false);

            transactionRecordStore.Replicate(partitionId, new RaftLog { LogType = ReplicationTypes.TransactionRecord, LogData = recordInitDelta });

            bool prepareOk = await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpReplicate, ReplicationTypes.PreparedIntent, anchorPrepareDelta, cancellationToken).ConfigureAwait(false);
            return (true, prepareOk);
        }

        return await ReplicateDurableBundleLocal(partitionId, recordInitDelta, anchorPrepareDelta, fenceKey, fenceGeneration, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// One-phase commit bundle: proposes the transaction's record init, its single (anchor-partition) prepare,
    /// and its commit decision as ONE atomic durable batch — a single barrier instead of the 2PC's
    /// init+prepare barrier followed by the decision barrier. Local-leader only: returns <see langword="null"/>
    /// when the anchor partition is led by another node, and the caller falls back to the standard 2PC flow
    /// (the durable-operation wire carries one delta per call, so the atomic bundle cannot cross nodes).
    /// The safety argument for deciding in the same batch as the prepare lives at the call site
    /// (<see cref="Transactions.DurableTransactionFinalizer"/>): the caller pre-checks that no foreign durable
    /// intent holds any of the transaction's keys, and in-memory write intents exclude new conflicting
    /// prepares from being proposed behind it.
    /// </summary>
    internal async Task<(bool BatchCommitted, bool PrepareAcknowledged)?> ReplicateDurableOnePhaseBundleThroughSchedulerFenced(
        int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, byte[] decisionDelta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
            return null;

        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        bool batchCommitted = false;

        Writes.DurableProposalSubmission submission = new(
            partitionId,
            [
                new RaftProposalEntry(ReplicationTypes.TransactionRecord, recordInitDelta, AutoCommit: true, ExpectedGeneration: 0),
                new RaftProposalEntry(ReplicationTypes.PreparedIntent, anchorPrepareDelta, AutoCommit: true, ExpectedGeneration: 0),
                new RaftProposalEntry(ReplicationTypes.TransactionRecord, decisionDelta, AutoCommit: true, ExpectedGeneration: 0)
            ],
            completion,
            // Ordinary admission, matching the 2PC record-init/prepare stage: nothing is prepared yet, so a
            // capacity rejection here is a clean retry — the Terminal reserve stays dedicated to finishing
            // transactions that already hold durable intents.
            Writes.WriteAdmissionClass.Ordinary,
            (batchPartitionId, entries, entryLogIndices) =>
            {
                batchCommitted = true;
                return ApplyDurableEntriesOnCommit(batchPartitionId, entries, entryLogIndices);
            },
            fenceKey,
            fenceGeneration);

        if (!writeAggregator.TryEnqueue(submission))
            return (false, false);

        using CancellationTokenRegistration _ = cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetResult(false), completion);
        bool prepareAcknowledged = await submission.Committed.ConfigureAwait(false);
        return (batchCommitted, prepareAcknowledged);
    }

    private async Task<(bool BatchCommitted, bool PrepareAcknowledged)> ReplicateDurableBundleLocal(
        int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Set only from the scheduler's ordered completion, which runs iff the batch committed; its write
        // happens-before the awaiter through the completion's TrySetResult, so a true read means the record init
        // (the first bundled entry) is durably applied. The batch's autocommit round is a single proposal, so both
        // entries share one fate on the commit dimension — this flag is the "did the record land" signal the folded
        // Committed bool cannot express on its own.
        bool batchCommitted = false;

        Writes.DurableProposalSubmission submission = new(
            partitionId,
            [
                new RaftProposalEntry(ReplicationTypes.TransactionRecord, recordInitDelta, AutoCommit: true, ExpectedGeneration: 0),
                new RaftProposalEntry(ReplicationTypes.PreparedIntent, anchorPrepareDelta, AutoCommit: true, ExpectedGeneration: 0)
            ],
            completion,
            Writes.WriteAdmissionClass.Ordinary,
            (batchPartitionId, entries, entryLogIndices) =>
            {
                batchCommitted = true;
                return ApplyDurableEntriesOnCommit(batchPartitionId, entries, entryLogIndices);
            },
            fenceKey,
            fenceGeneration);

        if (!writeAggregator.TryEnqueue(submission))
            return (false, false);

        using CancellationTokenRegistration _ = cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetResult(false), completion);
        bool prepareAcknowledged = await submission.Committed.ConfigureAwait(false);
        return (batchCommitted, prepareAcknowledged);
    }

    // The scheduler's ordered per-partition completion applies each durable record/intent delta to its store, in
    // Raft-commit order — the single authoritative apply owner on the leader. Key/value materialization records are
    // applied by their own leader path (ApplyDurableCommit / the replicator), not here. Returns whether every
    // PREPARE in the bundle took ownership of its key so a rejected prepare fails the producer's replicate.
    internal bool ApplyDurableEntriesOnCommit(int partitionId, IReadOnlyList<RaftProposalEntry> entries, IReadOnlyList<long>? entryLogIndices)
    {
        bool preparesAcknowledged = true;

        for (int i = 0; i < entries.Count; i++)
        {
            RaftProposalEntry entry = entries[i];

            if (entry.Type != ReplicationTypes.TransactionRecord && entry.Type != ReplicationTypes.PreparedIntent)
                continue;

            // Raft's commit path applies committed entries to the consumer before this completion can run, so the
            // apply of this very log entry has normally already happened and left its result. Reusing it keeps this
            // path's contract (the record is applied before the producer is resolved) without a second parse of the
            // same delta.
            long logIndex = entryLogIndices is not null && i < entryLogIndices.Count ? entryLogIndices[i] : 0;
            if (durableApplyResults.TryConsume(partitionId, logIndex, out bool recorded))
            {
                if (entry.Type == ReplicationTypes.PreparedIntent)
                    preparesAcknowledged &= recorded;

                continue;
            }

            // No recorded result — this completion overtook the consumer apply, so apply it here and leave the
            // outcome for the consumer to reuse when Raft delivers the same entry, mirroring the consumer-first
            // direction. The store applies by record/intent identity; the partition argument is unused here.
            RaftLog log = new() { LogType = entry.Type, LogData = entry.Data };

            bool applied;
            if (entry.Type == ReplicationTypes.TransactionRecord)
                applied = transactionRecordStore.Replicate(0, log);
            else
            {
                applied = preparedIntentStore.ApplyDeltaAckPrepares(log);
                preparesAcknowledged &= applied;
            }

            durableApplyResults.RecordApplied(partitionId, logIndex, applied);
        }

        return preparesAcknowledged;
    }

    /// <summary>
    /// Runs a durable operation that this node received via inter-node forwarding because it is the partition
    /// leader: replicate a delta through the local scheduler, or apply a committed/aborted intent's resolution on
    /// the local (leader) KV state. The intent crosses the wire serialized.
    /// </summary>
    internal async Task<bool> DurableOperationLocal(int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken)
    {
        // The sender may have routed on a replica guess (no gossiped leader hint yet): every durable
        // kind below requires the partition leader (propose via the local scheduler, apply on leader
        // state). This node hosts the partition, so its own leader resolution is accurate — redirect
        // once to the actual leader instead of failing the operation against a follower, which the
        // sender would only retry against the same guessed replica. Bounded: the redirect target is
        // the current leader, which serves locally; a mid-flight leadership move just redirects again.
        if (!await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
        {
            string? actualLeader;
            try
            {
                actualLeader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
            }
            catch (RaftException)
            {
                // Leadership undecided (election in progress): fail the forwarded op; the origin retries.
                return false;
            }

            if (actualLeader is not null && actualLeader != raft.GetLocalEndpoint())
                return await interNodeCommunication.DurableOperation(actualLeader, partitionId, kind, logType, payload, cancellationToken).ConfigureAwait(false);

            if (actualLeader is null)
                return false;
        }

        switch (kind)
        {
            case DurableOpReplicate:
                // A forwarded durable op is applied on the leader it was routed to; the fence ran on the origin.
                // The admission class is not carried on the durable-operation wire, so admit a forwarded op as
                // Terminal: cross-node settlement of an already-prepared transaction must never be starved by
                // local ordinary-write saturation. (Wiring the origin's class across the wire is a follow-up.)
                return await ReplicateDurableLocal(partitionId, logType, payload, Writes.WriteAdmissionClass.Terminal, fenceKey: null, fenceGeneration: 0, cancellationToken).ConfigureAwait(false);

            case DurableOpCommit:
                return await replicator.ApplyDurableCommit(partitionId, PreparedIntentStore.DeserializeIntents(payload)[0]).ConfigureAwait(false);

            case DurableOpRollback:
                return await replicator.ApplyDurableRollback(partitionId, PreparedIntentStore.DeserializeIntents(payload)[0]).ConfigureAwait(false);

            default:
                return false;
        }
    }

    /// <summary>The soft cap on the positive-terminal record-lookup cache; a terminal (Commit/Abort) record is
    /// immutable so a hit is always valid, but the cache stops growing past this to bound memory.</summary>
    private const int DurableRecordLookupCacheMax = 8192;

    /// <summary>Positive-terminal cache for routed record lookups: only immutable terminal records are cached (an
    /// Undecided/absent outcome can still transition and must never be cached across a later decision).</summary>
    private readonly ConcurrentDictionary<(HLCTimestamp, long), TransactionRecord> durableRecordLookupCache = new();

    /// <summary>
    /// Serves a canonical transaction-record lookup that was routed to this node because it is the record's anchor
    /// partition leader: the local record store is authoritative here. Returns the serialized record, or null when
    /// none exists. The record crosses the wire serialized so the public transport contract never exposes the
    /// internal record type.
    /// </summary>
    internal async Task<byte[]?> LookupTransactionRecordLocal(int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
    {
        // Only the anchor partition's leader is authoritative; a lagging follower's "absent" answer
        // would be indistinguishable from "no record exists". If the sender's leader guess landed
        // here on a follower, redirect once — this node hosts the partition, so its own resolution
        // is accurate.
        if (!await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
        {
            string? actualLeader;
            try
            {
                actualLeader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
            }
            catch (RaftException)
            {
                throw new PartitionNotHostedException(partitionId);
            }

            if (actualLeader is not null && actualLeader != raft.GetLocalEndpoint())
                return await interNodeCommunication
                    .LookupTransactionRecord(actualLeader, partitionId, transactionId, epoch, anchorKey, cancellationToken)
                    .ConfigureAwait(false);

            if (actualLeader is null)
                throw new PartitionNotHostedException(partitionId);
        }

        TransactionRecord? record = transactionRecordStore.Get(transactionId, epoch);
        return record is null ? null : TransactionRecordStore.SerializeRecords([record]);
    }

    /// <summary>
    /// Linearizable canonical-record lookup routed by the record's anchor key: resolves the anchor partition, and
    /// if this node does not lead it, forwards to the leader (whose store is authoritative) instead of consulting a
    /// node-local projection that would otherwise force a retry-until-settlement. Terminal outcomes are positively
    /// cached (immutable); Undecided/absent outcomes are never cached.
    /// </summary>
    internal async Task<TransactionRecord?> LookupDurableRecordRouted(HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
    {
        if (durableRecordLookupCache.TryGetValue((transactionId, epoch), out TransactionRecord? cached))
            return cached;

        int partitionId = locator.LocateRange(anchorKey).PartitionId;
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);

        TransactionRecord? record;
        if (leader is null)
        {
            record = transactionRecordStore.Get(transactionId, epoch);
        }
        else
        {
            byte[]? serialized = await interNodeCommunication
                .LookupTransactionRecord(leader, partitionId, transactionId, epoch, anchorKey, cancellationToken)
                .ConfigureAwait(false);

            record = serialized is null ? null : TransactionRecordStore.DeserializeRecords(serialized) is [TransactionRecord r, ..] ? r : null;
        }

        if (record is { IsTerminal: true } && durableRecordLookupCache.Count < DurableRecordLookupCacheMax)
            durableRecordLookupCache[(transactionId, epoch)] = record;

        return record;
    }

    /// <summary>
    /// Applies a durable-intent resolution's committed value on the leader: clears the committing transaction's
    /// staged write intent + MVCC snapshot and applies the value to the base entry. The leader applies direct
    /// writes through CompleteProposal, not the replication callback, so a durable-committed value would never
    /// land in the leader's store without this. Idempotent.
    /// </summary>
    internal async Task<bool> ApplyDurableCommit(int partitionId, Transactions.Data.PreparedIntent intent, CancellationToken cancellationToken)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
            return await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpCommit, "", PreparedIntentStore.SerializeIntents([intent]), cancellationToken).ConfigureAwait(false);

        return await replicator.ApplyDurableCommit(partitionId, intent).ConfigureAwait(false);
    }

    /// <summary>
    /// Clears an aborted durable transaction's staged write intent + MVCC snapshot on the owning actor (the durable
    /// analog of ApplyConfirmedRollback), so the key is not blocked until the intent lease expires. Routed to the
    /// partition leader on a cluster (that is where the staged write intent lives).
    /// </summary>
    internal async Task<bool> ApplyDurableRollback(int partitionId, Transactions.Data.PreparedIntent intent, CancellationToken cancellationToken)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
            return await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpRollback, "", PreparedIntentStore.SerializeIntents([intent]), cancellationToken).ConfigureAwait(false);

        return await replicator.ApplyDurableRollback(partitionId, intent).ConfigureAwait(false);
    }
}
