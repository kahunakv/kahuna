
using System.Collections.Concurrent;
using System.Diagnostics;
using DotNext;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Replication;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Communication.Grpc;
using TransactionHandle = Kahuna.Shared.KeyValue.TransactionHandle;
using Kahuna.Server.KeyValues.Handlers;

namespace Kahuna.Server.KeyValues.Transactions;

// Durable-intent finalize: replication, admission, and outcome application.
internal sealed partial class TransactionCoordinator
{

    // Rolling p99s of durable-finalize latency (total and per intent). They size each transaction's
    // decision-deadline margin from real finalize cost instead of a fixed guess: too low aborts slow but live
    // coordinators, too high delays recovery of dead ones. Latency is local elapsed time, so it uses a monotonic
    // stopwatch; the deadline itself is an HLC offset.
    private readonly FinalizeLatencyEstimator finalizeLatency = new();

    private DurableTransactionFinalizer? durableFinalizer;

    /// <summary>Test access to the lazily built finalizer, for installing its interleaving hooks.</summary>
    internal DurableTransactionFinalizer DurableFinalizerForTests => DurableFinalizer;

    /// <summary>Test-only: skips the coordinator's range-quiesce admission gate so a prepare into a quiesced
    /// range reaches its partition leader, where the dispatch fence must refuse it on its own.</summary>
    internal bool TestSkipRangeQuiesceAdmissionGate;

    // Deferred-settlement background resolutions in flight. Dispose drains them (bounded) and cancels stragglers.
    // Best-effort: recovery finishes any lost run.
    private readonly CancellationTokenSource deferredResolutionCts = new();

    private readonly ConcurrentDictionary<Task, byte> deferredResolutions = new();

    // Resolution (materialize committed values, settle intents) runs off the commit critical path when
    // DurableDeferredSettlement is on: the finalizer returns once the decision record is durable and settles in
    // the background. A read or write that meets a committed-but-unsettled foreign intent resolves it through the
    // durable-intent visibility path (local record, or routed to the anchor leader), so no stale value is served.
    // Recovery completes a lost background run. A null scheduler settles inline (synchronous).
    private DurableTransactionFinalizer DurableFinalizer => durableFinalizer ??= new DurableTransactionFinalizer(
        manager.DurableTransactionRecordStore, manager.DurablePreparedIntentStore, ReplicateDurableAsync,
        resolutionScheduler: configuration.DurableDeferredSettlement ? ScheduleDeferredResolution : null,
        ApplyDurableCommitLocally, ApplyDurableRollbackLocally,
        // Fresh attempt HLC at the decision, so a slow prepare can trip the deadline. Latency is recorded only up to
        // the decision, so post-decision settlement never inflates the next finalize's deadline.
        attemptClock: () => raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()),
        recordDecisionLatencyMs: finalizeLatency.Record,
        // Re-fence pre-decision record/prepare submissions at dispatch: a split/merge since freeze releases them
        // retryably instead of appending to a retired partition.
        replicateFenced: ReplicateDurableFencedAsync,
        // Bundle the anchor's record init with its prepare in one proposal, removing a pre-decision Raft barrier
        // from the one-partition critical path.
        replicateAnchorBundle: ReplicateDurableAnchorBundleAsync,
        maxMaterializationBatchItems: Math.Min(
            configuration.KeyValueWriteMaxBatchItems,
            configuration.KeyValueWriteMaxQueuedItemsPerPartition),
        maxMaterializationBatchBytes: Math.Min(
            configuration.KeyValueWriteMaxBatchBytes,
            configuration.KeyValueWriteMaxQueuedBytesPerPartition),
        // Prepare-conflict helping: a prepare blocked by a decided-but-unsettled foreign intent settles it inline
        // (idempotent recovery resolution) and re-prepares at once instead of backing off. Breaks the
        // settlement-lag convoy under load.
        resolveDecidedBlockers: manager.TryResolveDecidedDurableBlockersAsync,
        // One-phase commit: a single-participant transaction decides in one durable barrier ([init + prepare +
        // decision] as one atomic batch). Proposed locally when this node leads the anchor, else forwarded whole to
        // the anchor leader (one extra hop, still one durable round).
        replicateOnePhaseBundle: manager.ReplicateDurableOnePhaseBundleThroughSchedulerFenced,
        // Write-side compare-and-set before anything durable: a staged base that moved while the in-memory
        // write-intent lease lapsed aborts truthfully instead of discarding the other writer's commit.
        validateStagedBases: ValidateStagedBasesAsync,
        // Claim staged intents before the 2PC prepares. This ends the window where a snapshot read pushes the
        // commit above itself, and reports the floor the commit must be minted above.
        claimStagedIntents: ClaimStagedIntentsAsync,
        // Re-resolve each intent's data partition at resolution time. A split/merge after the decision moves key
        // ownership; materializing into the frozen partition would hide the committed value from the new range.
        resolveCurrentPartition: key => manager.LocateDurablePartition(key).PartitionId,
        // A prepare the leader released against a range quiesce is a clean retry from the first classification,
        // not helped and backed off against a window no other transaction holds.
        isKeyQuiesced: manager.IsKeyQuiesced,
        // The decision replicates WITHOUT the sender-side record projection. A decision forwarded to a remote
        // anchor leader can lose to one that already won there (e.g. a routed presumed abort). Projecting the
        // losing delta would permanently diverge this node's record store from the canonical record and
        // mis-direct scans and resolution.
        replicateDecision: (partitionId, delta, fences, cancellationToken) =>
            manager.ReplicateDurableThroughSchedulerFenced(
                partitionId, ReplicationTypes.TransactionRecord, delta, fences,
                Writes.WriteAdmissionClass.Terminal, Writes.WriteSubmissionStage.Decision, cancellationToken, projectRecordLocally: false),
        // The decision winner and resolution direction read the CANONICAL record: locally when this node leads
        // the anchor partition, else routed to the anchor leader.
        lookupRecordRouted: manager.LookupDurableRecordRouted,
        // Pre-decision replica fence check. The prepare ack folds only the leader's staged-base verdict, and a
        // leader with frozen or freshly restored fence memory admits prepares healthy replicas refuse. Reading
        // replica verdicts before the decision turns that racing stale-base veto into an ordered conflict abort.
        // Skipped in a single-process group: all replicas share this process's stores.
        confirmReplicaFence: configuration.SingleProcessRaftGroup ? null : manager.ConfirmReplicaFenceForCommitAsync,
        // Materialize by reference when the cluster is known to apply it: the record names the prepared intent
        // every replica already holds instead of copying the committed value through the log again.
        materializeByReference: configuration.DurableMaterializeByReference,
        // Install committed values at the settle's own apply instead of through materialization records, when
        // every node in the cluster is known to apply the materializing settle.
        materializeOnResolve: configuration.DurableMaterializeOnResolve,
        // One node-wide bound on leader-local applies, shared with the recovery paths that resolve on this node.
        localApplyGate: manager.DurableLocalApplyGate,
        // Typed decision: replicate the terminal transition and read the canonical winner in one round (locally
        // when this node leads the anchor, else from the anchor leader's answer), saving a separate lookup.
        // Supersedes replicateDecision + lookupRecordRouted for the decision; both stay wired for older remote
        // nodes' read-back and for the resolution direction.
        decide: manager.ReplicateDurableDecisionThroughSchedulerFenced,
        // A live finalize in a productive wait (settlement lag of a decided predecessor, a validation that used
        // most of the margin) renews the record's decision-deadline lease instead of being presumed aborted.
        // Bounded per attempt; 0 disables renewal for a mixed-version rollout.
        maxDeadlineExtensions: configuration.DurableDecisionDeadlineMaxExtensions,
        logger: logger,
        // Every prepare carries the commit admission lag bound, so each replica refuses a commit timestamp that
        // reaches its log too late (or too far ahead) and a watch can promise a watermark.
        commitAdmissionLagMs: configuration.CommitAdmissionLagMs);

    /// <summary>Runs a durable transaction's post-decision resolution off the commit critical path. Exceptions are
    /// logged and swallowed: the decision is durable and recovery finishes a lost run. The task is tracked so
    /// <see cref="Dispose"/> can drain it.</summary>
    private void ScheduleDeferredResolution(Func<CancellationToken, Task> resolution)
    {
        Task task = Task.Run(async () =>
        {
            try
            {
                await resolution(deferredResolutionCts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Deferred durable resolution failed; recovery will complete it");
            }
        });

        deferredResolutions.TryAdd(task, 0);
        _ = task.ContinueWith(static (t, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(t, out _),
            deferredResolutions, TaskScheduler.Default);
    }


    // Durable 2PC records replicate through the shared partition write scheduler, so concurrent transactions'
    // records to one partition coalesce into one ReplicateEntries proposal. The finalizer applies committed
    // record/intent deltas to its local stores, and committed values to the leader's KV state via
    // ApplyDurableCommitLocally.
    private Task<bool> ReplicateDurableAsync(int partitionId, string logType, byte[] data, Writes.WriteAdmissionClass admissionClass, Writes.WriteSubmissionStage stage, CancellationToken cancellationToken) =>
        manager.ReplicateDurableThroughScheduler(partitionId, logType, data, admissionClass, stage, cancellationToken);

    // Fenced variant for pre-decision record/prepare: at dispatch (local aggregator path), re-checks each fenced key
    // against its frozen generation, so a topology change since freeze releases it retryably.
    private Task<bool> ReplicateDurableFencedAsync(int partitionId, string logType, byte[] data, IReadOnlyList<Writes.RangeFence> fences, Writes.WriteAdmissionClass admissionClass, Writes.WriteSubmissionStage stage, CancellationToken cancellationToken) =>
        manager.ReplicateDurableThroughSchedulerFenced(partitionId, logType, data, fences, admissionClass, stage, cancellationToken);

    // Bundles the anchor's [record init, prepare] into one fenced proposal, removing a pre-decision barrier.
    // Returns record-durable and prepare-acknowledged separately, so the finalizer can tell a clean retry (nothing
    // durable) from a rejected prepare with a durable record (drive a truthful abort).
    private Task<(bool BatchCommitted, bool PrepareAcknowledged)> ReplicateDurableAnchorBundleAsync(int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, IReadOnlyList<Writes.RangeFence> fences, CancellationToken cancellationToken) =>
        manager.ReplicateDurableBundleThroughSchedulerFenced(partitionId, recordInitDelta, anchorPrepareDelta, fences, cancellationToken);

    // Durable-path commit apply: clears the staged write intent/MVCC and applies the committed value on the
    // participant partition's leader (routed there on a cluster). Called by the finalizer's resolution.
    private Task<bool> ApplyDurableCommitLocally(int partitionId, PreparedIntent intent) =>
        manager.ApplyDurableCommit(partitionId, intent, CancellationToken.None);

    // Clears an aborted transaction's staged write intent/MVCC on the leader (the durable analog of
    // ApplyConfirmedRollback), invoked by the finalizer's resolution when the decision is abort.
    private Task<bool> ApplyDurableRollbackLocally(int partitionId, PreparedIntent intent) =>
        manager.ApplyDurableRollback(partitionId, intent, CancellationToken.None);

    /// <summary>
    /// Derives this finalize's decision-deadline margin (ms past the commit timestamp) as
    /// <c>clamp(multiplier × max(observed-finalize-p99, observed-per-intent-p99 × intentCount), floor, ceiling)</c>.
    /// A healthy commit gets headroom above recent finalizes <i>and</i> above its own manifest's cost; the ceiling
    /// caps how long a dead coordinator's undecided record blocks recovery. The floor applies before the first
    /// sample. The margin is recorded as a metric. See <see cref="FinalizeLatencyEstimator"/> for why manifest
    /// size counts.
    /// </summary>
    private long DeriveDecisionDeadlineMarginMs(int intentCount)
    {
        long margin = finalizeLatency.DeadlineMarginMs(
            intentCount,
            configuration.DurableDecisionDeadlineMultiplier,
            configuration.DurableDecisionDeadlineFloorMs,
            configuration.DurableDecisionDeadlineCeilingMs);

        DurableTransactionMetrics.DecisionDeadlineMarginMs.Record(margin);
        return margin;
    }

    /// <summary>
    /// Builds the frozen finalize input from the transaction's PERSISTENT modified keys (the crash-atomic subset),
    /// grouping intents by current data partition. In a mixed transaction the ephemeral keys commit or roll back in
    /// memory, keyed on this decision. Returns false (fall back to the ticket path) when there is no persistent
    /// key, no anchor, or a persistent key has no staged value. A pending unresolved finalize is re-driven as is.
    /// </summary>
    private bool TryBuildDurableFinalizeInput(TransactionContext context,
        IReadOnlyCollection<(string Key, KeyValueDurability Durability)> persistentKeys,
        out DurableFinalizeInput? input, out HLCTimestamp opId)
    {
        input = null;
        opId = HLCTimestamp.Zero;

        // A prior attempt proposed its canonical record without a record-backed outcome. Its identity (commit
        // timestamp, decision deadline, manifest hash) is frozen in the record CAS, so a retry re-drives the same
        // input under a fresh operation id, as the reaper's and rollback's fences do. Rebuilding would mint a new
        // identity the record rejects on every transition, leaving the transaction retryable forever.
        if (context.UnresolvedDurableFinalize is { } unresolved)
        {
            input = unresolved;
            opId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            return true;
        }

        if (persistentKeys.Count == 0 || context.RecordAnchorKey is null)
            return false;

        // Staged values accumulate per key across every mutation command (ModifiedResult holds only the last). A
        // modified key missing here (an extend, a TTL set, any mutation that cannot be staged losslessly) makes
        // TryBuild fall back to the ticket path.
        Dictionary<string, StagedValue>? staged = context.StagedMutations;
        if (staged is null || staged.Count == 0)
            return false;

        HLCTimestamp txId = context.TransactionId;

        const long epoch = DurableFinalizeEpoch;

        // The commit timestamp is the revision every participant materializes. It must exceed each participant's
        // stamp on the staged write, not just the transaction id: a participant that served a snapshot read at T
        // before the write was staged advanced its clock past T, and a lagging coordinator could mint at or below
        // T. It must also exceed every commit floor the commit probe reported: a snapshot read served over the
        // staged write answered the committed head at that floor, and the commit must not land inside it.
        HLCTimestamp commitTimestamp = MintDurableCommitTimestamp(raft.HybridLogicalClock, raft.GetLocalNodeId(), txId, staged, context.ClaimedCommitFloor);
        HLCTimestamp decisionDeadline = new(commitTimestamp.N, commitTimestamp.L + DeriveDecisionDeadlineMarginMs(persistentKeys.Count), commitTimestamp.C);

        // The builder reads the session's staged values and written-base observations in place. Safe: this freeze
        // runs synchronously under the finalize owner after accepted operations drained, so neither dictionary
        // mutates, and the built input owns its collections. Each read-then-written key's pre-write observation is
        // the committed base its mutation depends on; it is frozen into the intent for the staged-base CAS.
        if (!DurableFinalizeInputBuilder.TryBuild(
                txId, epoch, context.CoordinatorKey ?? context.RecordAnchorKey, context.RecordAnchorKey,
                commitTimestamp, decisionDeadline, persistentKeys, staged,
                manager.LocateDurablePartition, out input, context.WrittenBaseObservations, context.Origin))
            return false;

        opId = commitTimestamp;
        return true;
    }

    /// <summary>
    /// How many times one finalize may re-freeze its commit timestamp above a snapshot read's commit floor. The
    /// claim that reports the floor also ends the push window, so one re-freeze suffices; the bound guards the loop.
    /// </summary>
    private const int MaxCommitFloorRefreezes = 2;

    // Finalizes the persistent subset of a transaction and returns its terminal decision, so a mixed transaction's
    // caller can commit or roll back the ephemeral subset. Also sets context.Result. persistentKeys (the subset the
    // input was frozen from) lets the input be re-frozen above a commit floor the finalizer reports.
    private async Task<DurableFinalizeResult> DurableFinalize(
        TransactionContext context,
        IReadOnlyCollection<(string Key, KeyValueDurability Durability)> persistentKeys,
        DurableFinalizeInput input,
        HLCTimestamp opId,
        CancellationToken cancellationToken)
    {
        // Whether this attempt froze the input itself. A re-driven input keeps the identity an earlier attempt
        // proposed under; only a freshly frozen one may be frozen again (see the commit-floor re-freeze below).
        bool frozenHere = context.UnresolvedDurableFinalize is null;

        context.DurableCommitTimestamp = input.CommitTimestamp;

        // A retry of a frozen finalize past its decision deadline cannot commit: the record's deadline gate rejects
        // any commit whose attempt HLC passed it, and HLCs only advance. Re-driving the prepares would re-init the
        // record on the (possibly new) anchor leader and lose again, in a loop. Conclude now through the reaper's
        // and rollback's record-CAS fence: a presumed abort that yields to a commit the ordered log applied first.
        if (context.UnresolvedDurableFinalize is not null && opId > input.DecisionDeadline)
        {
            (DurableFinalizeOutcome fenced, bool fenceRan) = await FenceUnresolvedDurableFinalize(input, opId, cancellationToken).ConfigureAwait(false);

            DurableTransactionMetrics.RetryPastDeadlineConcluded(fenced.Result);

            logger.LogWarning(
                "Durable commit retry of transaction {TransactionId} arrived past its frozen decision deadline {Deadline} (attempt {Attempt}); concluded through the record fence as {Outcome}",
                input.TransactionId, input.DecisionDeadline, opId, fenced.Result);

            // Only a fence that ran is a deadline-gate conclusion; a throw classified from the record is not.
            return ApplyDurableOutcome(context, fenceRan ? fenced with { LateCommitRejected = true } : fenced);
        }

        // Admission gate 0: range quiesce. Refuse when a key or the record anchor lies in a range that is moving to
        // another partition; a prepare appended during the move's catch-up copy is stranded on the source. The
        // mover's range lock is leader-local and lost on leader change, so this checks the replicated quiesce
        // flag. The binding check is the leader's dispatch fence; this gate keeps the prepare off the wire and
        // names the refusal. A retry proceeds once the move ends. Re-driven attempts are refused too: the move
        // waits for their intents to decide, so they are delayed, never deadlocked.
        if (!TestSkipRangeQuiesceAdmissionGate && FindQuiescedKey(input) is { } quiescedKey)
        {
            DurableTransactionMetrics.RangeQuiesceRefusals.Add(1);
            context.Result = new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.MustRetry,
                Reason = $"Key {quiescedKey} lies in a key range whose data is being moved to another partition; retry",
                Refusal = CommitRefusalClass.RangeQuiesced
            };
            return DurableFinalizeResult.MustRetry;
        }

        // Admission gate 1 — resident prepared-intent count/bytes. Refuse if this transaction would push resident
        // prepared-intent state past its node bound, so slow settlement cannot grow it without limit. Read-only
        // (nothing to unwind), so it runs before the slot reserve.
        if (!AdmitPreparedIntents(input))
        {
            DurableTransactionMetrics.AdmissionRejections.Add(1);
            context.Result = new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.MustRetry,
                Reason = "Durable prepared-intent capacity reached; retry",
                Refusal = CommitRefusalClass.AdmissionCapacity
            };
            return DurableFinalizeResult.MustRetry;
        }

        // Admission gate 2 — reserve one outstanding-durable slot before prepare. At capacity, refuse with MustRetry
        // (nothing prepared or installed), so a burst gets backpressure instead of unbounded intents and records.
        if (!TryReserveDurableSlot())
        {
            DurableTransactionMetrics.AdmissionRejections.Add(1);
            context.Result = new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.MustRetry,
                Reason = "Durable admission at capacity; retry",
                Refusal = CommitRefusalClass.AdmissionCapacity
            };
            return DurableFinalizeResult.MustRetry;
        }

        // Commit validation (validateReadSet below) runs after prepare, so it sees a range lock taken after the
        // write was staged. The probe catches an in-flight writer on a read key and a foreign range lock on a
        // written key; the revision check catches a read a concurrent commit made stale. Both must pass. One-phase
        // validates here before proposing; its 2PC fallback validates again after the prepares are durable.
        //
        // The probe runs first: it answers at once for an undecided foreign intent, while the revision check
        // waits for it. Two transactions with intents on each other's read keys would otherwise wait on each other
        // until the read deadline; probing first makes both abort at once.
        //
        // From here the attempt replicates, and a timed-out proposal can still commit after a partition heals.
        // Record the frozen input so a finalizer that gives up (reaper, rollback) fences through the record CAS
        // before it reports an abort. Cleared only on a record-backed terminal outcome.
        context.UnresolvedDurableFinalize = input;

        DurableFinalizeOutcome outcome;
        try
        {
            for (int refreeze = 0; ; refreeze++)
            {
                OnePhaseEligibility eligibility = ComputeOnePhaseEligibility(context, input);

                try
                {
                    outcome = await DurableFinalizer.FinalizeAsync(
                        input,
                        validateReadSet: async ct =>
                        {
                            DurableTransactionMetrics.FinalizeReadSetKeys.Record(context.ReadKeys?.Count ?? 0);

                            if (await CheckCommitConflicts(context, ct).ConfigureAwait(false)
                                && await ValidateReadSet(context, ct).ConfigureAwait(false))
                                return TransactionAbortClass.None;

                            // The failed check named its cause on the result. A lost lock is recorded as such, so a
                            // leader change is not called a conflict. A probe that proved nothing is not an abort: the
                            // finalizer decides nothing and the attempt is retried.
                            return context.Result switch
                            {
                                { Type: KeyValueResponseType.MustRetry } => TransactionAbortClass.RetryableFailure,
                                { Type: KeyValueResponseType.Aborted, ExclusionLost: true } => TransactionAbortClass.LostExclusion,
                                _ => TransactionAbortClass.Conflict
                            };
                        },
                        opId,
                        cancellationToken,
                        readSetExclusion: eligibility.ReadSetExclusion,
                        applyTimeValidation: eligibility.ApplyTimeValidation,
                        bundledReadDependencies: eligibility.OnPartitionReadDependencies,
                        bundledExclusionTerm: () => AnchorExclusionTerm(context, input.AnchorPartitionId),
                        claimedCommitFloor: () => context.ClaimedCommitFloor).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // A finalize exception decides nothing. Classify from the canonical record: a durable decision is
                    // the truth (Committed or Abort); otherwise the outcome is unknown and retryable. This stops a
                    // post-decision failure from telling the caller to replay a transaction that committed.
                    outcome = ClassifyFromCanonicalRecord(input);
                }

                if (outcome.CommitFloor.IsNull())
                    break;

                // A snapshot read was served the committed head over one of our staged writes at or above the frozen
                // commit timestamp (after the freeze, before the commit probe claimed the intent). The finalizer
                // refused before its first proposal, so nothing durable carries the frozen identity; re-freeze
                // strictly above the floor. The claim ended the push window, so the next attempt cannot meet a higher
                // floor; the bound only guards the loop. A re-driven input keeps its identity: its claim preceded an
                // earlier proposal, so this refusal should not arise; if it does, answer MustRetry rather than mint
                // an identity the record rejects.
                if (!frozenHere || refreeze >= MaxCommitFloorRefreezes)
                {
                    logger.LogWarning(
                        "Durable commit of transaction {TransactionId} met a snapshot read floor {CommitFloor} at or above its frozen commit timestamp {CommitTimestamp} and was not frozen again (re-driven {ReDriven}, re-freezes {Refreezes}); answering MustRetry",
                        input.TransactionId, outcome.CommitFloor, input.CommitTimestamp, !frozenHere, refreeze);
                    break;
                }

                context.RaiseClaimedCommitFloor(outcome.CommitFloor);
                context.UnresolvedDurableFinalize = null;

                if (!TryBuildDurableFinalizeInput(context, persistentKeys, out DurableFinalizeInput? refrozen, out HLCTimestamp refrozenOpId))
                    break;

                DurableTransactionMetrics.CommitFloorRefrozen();

                input = refrozen!;
                opId = refrozenOpId;
                context.DurableCommitTimestamp = input.CommitTimestamp;
                context.UnresolvedDurableFinalize = input;
            }
        }
        finally
        {
            // The attempt has ended (decision installed or given up); free the slot for the next transaction.
            ReleaseDurableSlot();
        }

        return ApplyDurableOutcome(context, outcome);
    }

    /// <summary>
    /// The first key of <paramref name="input"/> in a range quiesced for a data move, per this node's applied range
    /// map, or null. Checks the record anchor first (its record init would be stranded like a prepare), then every
    /// key to prepare.
    /// </summary>
    private string? FindQuiescedKey(DurableFinalizeInput input)
    {
        if (manager.IsKeyQuiesced(input.RecordAnchorKey))
            return input.RecordAnchorKey;

        foreach (DurablePartitionPrepare partition in input.Partitions)
        {
            foreach (PreparedIntent intent in partition.Intents)
            {
                if (manager.IsKeyQuiesced(intent.Key))
                    return intent.Key;
            }
        }

        return null;
    }

    /// <summary>
    /// Records a durable finalize attempt's outcome on the session — the unresolved-finalize fence obligation,
    /// the per-transaction deadline-gate accounting, and the client-facing result — and returns its result.
    /// </summary>
    private DurableFinalizeResult ApplyDurableOutcome(TransactionContext context, DurableFinalizeOutcome outcome)
    {
        // A terminal outcome is record-backed, so no stalled proposal can contradict it; only MustRetry keeps the
        // fence obligation. An unresolved attempt that extended the record's decision deadline carries it onto the
        // frozen input, so a same-identity retry checks its clock against the deadline the record holds.
        if (outcome.Result is DurableFinalizeResult.Committed or DurableFinalizeResult.Aborted)
            context.UnresolvedDurableFinalize = null;
        else if (context.UnresolvedDurableFinalize is { } unresolved && outcome.DecisionDeadline > unresolved.DecisionDeadline)
            context.UnresolvedDurableFinalize = unresolved with { DecisionDeadline = outcome.DecisionDeadline };

        // Per-transaction view of the deadline gate. The finalizer counts each rejected attempt, and each client
        // retry is an attempt. Count the transaction once, on first rejection, to show how many transactions the
        // gate cost.
        if (outcome.LateCommitRejected && !context.LateCommitRejected)
        {
            context.LateCommitRejected = true;
            DurableTransactionMetrics.LateCommitRejectedTransaction();
        }

        context.Result = outcome.Result switch
        {
            // On commit, keep the last statement's result as the ticket path does (its CommitMutations sets none on
            // success), so an auto-commit script ending in a read returns that read, not a synthetic Set. A bare
            // commit with no prior result reports Set.
            //
            // Never keep a failure: a discarded earlier attempt may have written one. The one-phase path's
            // pre-propose validation sets a conflict Aborted when its write-skew probe finds a concurrent intent,
            // then falls back to 2PC, whose re-validation can pass and commit. Reporting that stale Aborted for a
            // committed transaction is an aborted-read anomaly for every reader that sees its writes.
            DurableFinalizeResult.Committed =>
                context.Result is { Type: not (KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry) }
                    ? context.Result
                    : new KeyValueTransactionResult { Type = KeyValueResponseType.Set, Reason = null },
            // A lost exclusion keeps the refusal's own reason (which partition changed leader, or which key lost
            // its staging) when this attempt's validation wrote it; a class read back from a record another
            // attempt decided has no such text and reports the class.
            DurableFinalizeResult.Aborted when outcome.AbortClass == TransactionAbortClass.LostExclusion => new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.Aborted,
                Reason = context.Result is { Type: KeyValueResponseType.Aborted, ExclusionLost: true, Reason: { } lostReason }
                    ? lostReason
                    : outcome.Reason ?? "Transaction aborted: a leader change dropped a lock or a staging the transaction relied on",
                ExclusionLost = true,
                Refusal = context.Result is { Type: KeyValueResponseType.Aborted, ExclusionLost: true, Refusal: not CommitRefusalClass.None } lostNamed
                    ? lostNamed.Refusal
                    : outcome.Refusal != CommitRefusalClass.None ? outcome.Refusal : CommitRefusalClass.LostLock
            },
            // The validation that failed named its arm on the result (a read-set conflict, a lost staged intent);
            // the finalizer names every arm it decided itself (a moved base, an exhausted prepare loop, the
            // deadline gate); a class read back from a record another attempt decided maps from its abort class.
            DurableFinalizeResult.Aborted => new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.Aborted,
                Reason = outcome.AbortClass == TransactionAbortClass.Conflict && context.Result is { Type: KeyValueResponseType.Aborted, Reason: { } conflictReason }
                    ? conflictReason
                    : outcome.Reason ?? (outcome.AbortClass == TransactionAbortClass.Conflict ? "Transaction conflict" : $"Transaction aborted: {outcome.AbortClass}"),
                Refusal = outcome.AbortClass == TransactionAbortClass.Conflict && context.Result is { Type: KeyValueResponseType.Aborted, Refusal: not CommitRefusalClass.None } conflictNamed
                    ? conflictNamed.Refusal
                    : outcome.Refusal != CommitRefusalClass.None ? outcome.Refusal : CommitRefusals.FromAbortClass(outcome.AbortClass)
            },
            _ => new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.MustRetry,
                Reason = outcome.Reason ?? "Durable finalize could not complete; retry",
                Refusal = outcome.Refusal != CommitRefusalClass.None ? outcome.Refusal : CommitRefusalClass.Unresolved
            }
        };

        return outcome.Result;
    }

    /// <summary>
    /// Fences a durable finalize attempt its caller abandons without a same-identity retry, and reports what the
    /// fence found on <see cref="TransactionContext.Result"/>.
    ///
    /// <para>A MustRetry finalize can leave the record initialized and some prepared intents installed. E.g. a
    /// one-phase bundle refused on one key (held by another transaction's committed-but-unsettled intent) has
    /// already installed its other keys, expecting a same-identity retry to reuse them through 2PC. A script
    /// never retries under the same identity: it re-runs as a new transaction, so those intents are abandoned.
    /// Left alone they stay undecided until presumed-abort recovery reaches the decision deadline, and every
    /// writer of those keys, the re-run included, gets MustRetry meanwhile. The fence installs a durable Abort
    /// through the record CAS and rolls the intents back at once, freeing the keys.</para>
    ///
    /// <para>The fence also learns the truth about a stalled proposal: if its commit applied first, the CAS
    /// rejects the abort and the transaction is reported Committed, not MustRetry, since retrying a committed
    /// script would apply it twice. If the fence cannot be installed (replication unavailable), MustRetry stands
    /// and recovery resolves the record at its deadline.</para>
    /// </summary>
    internal async Task FenceAbandonedFinalize(TransactionContext context)
    {
        if (context.UnresolvedDurableFinalize is not { } unresolved)
            return;

        // A throwing fence is classified from the canonical record, so a commit that already applied is reported
        // Committed and the caller never re-runs a committed script.
        (DurableFinalizeOutcome fenced, _) = await FenceUnresolvedDurableFinalize(
            unresolved, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()), CancellationToken.None);

        if (fenced.Result == DurableFinalizeResult.MustRetry)
            return;

        context.UnresolvedDurableFinalize = null;

        if (fenced.Result == DurableFinalizeResult.Committed)
            context.Result = new() { Type = KeyValueResponseType.Set, Reason = null };
    }

    /// <summary>Reserves one outstanding-durable admission slot, or returns false at
    /// <c>DurableDecisionOutstandingMax</c>. A non-positive cap disables the gate. The CAS loop keeps the count at
    /// or below the cap under concurrent admissions.</summary>
    private bool TryReserveDurableSlot()
    {
        int max = configuration.DurableDecisionOutstandingMax;
        if (max <= 0)
            return true;

        while (true)
        {
            int current = Volatile.Read(ref outstandingDurable);
            if (current >= max)
                return false;
            if (Interlocked.CompareExchange(ref outstandingDurable, current + 1, current) == current)
                return true;
        }
    }

    /// <summary>Releases a slot reserved by <see cref="TryReserveDurableSlot"/>. A no-op when the gate is
    /// disabled, so a disabled-then-enabled reconfiguration cannot underflow the counter.</summary>
    private void ReleaseDurableSlot()
    {
        if (configuration.DurableDecisionOutstandingMax <= 0)
            return;

        Interlocked.Decrement(ref outstandingDurable);
    }

    /// <summary>Current count of durable transactions being driven through finalize (test/observability).</summary>
    internal int OutstandingDurableCount => Volatile.Read(ref outstandingDurable);

    /// <summary>Whether this transaction's prepared intents fit the node's resident prepared-intent count and byte
    /// bounds. A non-positive cap disables that dimension. Reads totals without reserving, so concurrent admissions
    /// can overshoot slightly; acceptable for a soft bound whose transaction count the outstanding gate already
    /// caps.</summary>
    private bool AdmitPreparedIntents(DurableFinalizeInput input)
    {
        int maxCount = configuration.DurablePreparedIntentMaxCount;
        long maxBytes = configuration.DurablePreparedIntentMaxBytes;
        if (maxCount <= 0 && maxBytes <= 0)
            return true;

        int txCount = 0;
        long txBytes = 0;
        foreach (DurablePartitionPrepare partition in input.Partitions)
            foreach (PreparedIntent intent in partition.Intents)
            {
                txCount++;
                txBytes += intent.Value?.Length ?? 0;
            }

        PreparedIntentStore store = manager.DurablePreparedIntentStore;
        if (maxCount > 0 && store.Count + txCount > maxCount)
            return false;
        if (maxBytes > 0 && store.TotalBytes + txBytes > maxBytes)
            return false;

        return true;
    }

    /// <summary>
    /// Maps the canonical record to a finalize outcome after a finalize threw. Durable Commit is Committed; durable
    /// Abort is Aborted, whatever its class. An undecided or nonresident record is MustRetry, never a fabricated
    /// abort. A remote anchor's record is read from this node's local projection; if absent, MustRetry stands until
    /// recovery or the anchor-routed lookup resolves it.
    /// </summary>
    private DurableFinalizeOutcome ClassifyFromCanonicalRecord(DurableFinalizeInput input)
    {
        TransactionRecord? record = manager.DurableTransactionRecordStore.Get(input.TransactionId, input.Epoch);

        return record?.Decision switch
        {
            TransactionDecision.Commit => new DurableFinalizeOutcome(DurableFinalizeResult.Committed, TransactionAbortClass.None),
            TransactionDecision.Abort => new DurableFinalizeOutcome(DurableFinalizeResult.Aborted, record.AbortClass,
                Refusal: CommitRefusals.FromAbortClass(record.AbortClass), Reason: $"Transaction aborted: {record.AbortClass}"),
            _ => new DurableFinalizeOutcome(DurableFinalizeResult.MustRetry, TransactionAbortClass.RetryableFailure,
                Refusal: CommitRefusalClass.Unresolved, Reason: "The finalize threw before a record-backed outcome")
        };
    }
}
