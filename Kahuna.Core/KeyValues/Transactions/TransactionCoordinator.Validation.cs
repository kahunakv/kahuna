
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

// One-phase eligibility, read-set validation, staged-base validation, and staged-intent claims.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// Whether the read-set must be validated for write skew before prepare. Optimistic locking always validates:
    /// without it there is no isolation, and lost updates and write skew go through. An explicit
    /// <see cref="ReadValidation.TrackAndValidate"/> policy also requests it, whatever the locking mode.
    /// </summary>
    private static bool RequiresReadSetValidation(TransactionContext context)
    {
        return context.ReadValidation == ReadValidation.TrackAndValidate
            || context.Locking == KeyValueTransactionLocking.Optimistic;
    }

    /// <summary>The one-phase bundle's eligibility inputs for one finalize, computed by
    /// <see cref="ComputeOnePhaseEligibility"/>.</summary>
    /// <param name="ReadSetExclusion">A validated dependency the bundle cannot re-check at apply time. It keeps
    /// the bundle closed (2PC runs) and is recorded as the gate verdict. Null admits the read set.</param>
    /// <param name="ApplyTimeValidation">The bundled commit is validated at apply time against the partition's
    /// replicated committed-head ledger.</param>
    /// <param name="OnPartitionReadDependencies">Read-only point dependencies on the anchor partition, carried
    /// into the bundled commit for the apply-time check. Null when none, when the check is off, or when the read
    /// set is excluded (2PC then validates the whole read set from the context).</param>
    private readonly record struct OnePhaseEligibility(
        OnePhaseGateOutcome? ReadSetExclusion,
        bool ApplyTimeValidation,
        IReadOnlyList<BundledReadDependency>? OnPartitionReadDependencies);

    /// <summary>
    /// The leadership term the transaction's in-memory exclusions on the anchor partition were confirmed under:
    /// the term the commit probe confirmed its staged intents under, else the term its locks there were granted
    /// under; 0 if neither. The one-phase bundle is fenced to this term: refused before append if the anchor is
    /// led under another term, and again at apply with apply-time validation. The staged intents and locks live
    /// only in that leader's memory, and until the bundle applies they keep snapshot reads waiting for the write.
    /// Read after the commit probe passes, since the probe reports the staged intents' term; not at eligibility.
    /// </summary>
    private static long AnchorExclusionTerm(TransactionContext context, int anchorPartitionId)
    {
        long stagedIntentTerm = context.StagedIntentTermOf(anchorPartitionId);
        if (stagedIntentTerm != 0)
            return stagedIntentTerm;

        if (context.SnapshotLockGrantTerms() is { } lockGrants)
        {
            foreach (LockGrantTerm grant in lockGrants)
            {
                if (grant.PartitionId == anchorPartitionId)
                    return grant.Term;
            }
        }

        return 0;
    }

    /// <summary>
    /// Decides which validated dependencies keep the one-phase bundle closed and which ride into the bundled
    /// commit for its apply-time check.
    ///
    /// <para><b>Without apply-time validation</b> (<see cref="KahunaConfiguration.OnePhaseApplyTimeValidation"/>
    /// off), apply-time re-checks cover only written keys. So a read set beyond the written keys closes the
    /// bundle: a stalled bundle surfacing after a leader change would decide on a stale read validation. A
    /// validated base (a key read then written) closes it too. On the 2PC path, a competitor committing the same
    /// base between the pre-propose probe and the prepare is caught by the staged-base fence at prepare apply.
    /// But the bundle's decision shares the prepare's atomic batch, so a refused acknowledgement cannot withhold it;
    /// its only guard is the pre-propose re-validation, unguarded across the validate→apply gap. Both rules apply
    /// only in multi-process groups, where a stalled bundle can apply arbitrarily late. In a single-process group
    /// (<see cref="KahunaConfiguration.SingleProcessRaftGroup"/>) no remote replica can resurrect a proposal, and a
    /// committed bundle replays on restore before any later conflicting write. The gap is a sub-millisecond
    /// in-process window, accepted for the embedded fast path.</para>
    ///
    /// <para><b>With apply-time validation</b>, the bundled commit proves its read set at apply, in log order,
    /// against the partition's committed-head ledger: every co-bundled validated base, and every read-only point
    /// dependency routed to the anchor partition (carried in the command). Those no longer close the bundle.
    /// Still closing it is any dependency with no deterministic apply-time check: a prefix or range lock (a
    /// predicate, not a key), a read-only key on another partition (no cross-partition state at apply), or a
    /// read-only non-persistent key (its writes never feed the ledger). Routing uses the same locate that froze
    /// the intents, so a read and a write of one key agree on the partition. A hash-routed read's partition comes
    /// from its key space's placement (<see cref="Shared.Routing.HashPlacement"/>): a consumer that reads an index
    /// entry and writes a row keeps the bundle open by putting the index key space in the row's placement group.</para>
    ///
    /// <para>The returned exclusion is the first found: predicate first, then read keys in context order. It is
    /// recorded as the gate verdict so an operator sees what closed the bundle. Once excluded, no on-partition
    /// dependency is collected; 2PC validates the whole read set from the context.</para>
    /// </summary>
    private OnePhaseEligibility ComputeOnePhaseEligibility(TransactionContext context, DurableFinalizeInput input)
    {
        // Only a multi-process group closes the bundle on these shapes. A single-process group accepts the
        // in-process residual (see the summary) and still carries what it can check.
        bool closesBundle = !configuration.SingleProcessRaftGroup;

        // Locks live only in the granting leader's memory. A leader change between validation and propose would
        // let a new leader, which never saw the locks, accept the bundle. With apply-time validation the bundled
        // commit carries the anchor's exclusion term (see AnchorExclusionTerm), and every replica refuses it
        // unless a leader of that term proposed it. Without it, nothing at apply can tell, so a multi-process
        // group sends lock-holding transactions through 2PC, whose validation runs after prepares are durable.
        // Staged write intents are covered in both modes by the anchor leader's pre-append fence on that term.
        List<LockGrantTerm>? lockGrants = context.SnapshotLockGrantTerms();

        if (!configuration.OnePhaseApplyTimeValidation)
        {
            OnePhaseGateOutcome? exclusion = null;

            if (closesBundle)
            {
                if (HasReadDependenciesBeyondWrites(context))
                    exclusion = OnePhaseGateOutcome.ReadSetBeyondWrites;
                else if (context.WrittenBaseObservations is { Count: > 0 })
                    exclusion = OnePhaseGateOutcome.ValidatedBase;
                else if (lockGrants is not null)
                    exclusion = OnePhaseGateOutcome.HeldLock;
            }

            return new OnePhaseEligibility(exclusion, ApplyTimeValidation: false, OnPartitionReadDependencies: null);
        }

        if (!RequiresReadSetValidation(context))
            return new OnePhaseEligibility(ReadSetExclusion: null, ApplyTimeValidation: true, OnPartitionReadDependencies: null);

        if (closesBundle && (context.PrefixLocksAcquired is { Count: > 0 } || context.RangeLocksAcquired is { Count: > 0 }))
            return new OnePhaseEligibility(OnePhaseGateOutcome.PredicateRead, ApplyTimeValidation: true, OnPartitionReadDependencies: null);

        List<BundledReadDependency>? onPartitionReads = null;

        if (context.ReadKeys is { Count: > 0 })
        {
            foreach (KeyValueTransactionReadKey readKey in context.ReadKeys.Values)
            {
                if (string.IsNullOrEmpty(readKey.Key))
                    continue;

                if (context.ModifiedKeys is not null && context.ModifiedKeys.Contains((readKey.Key, readKey.Durability)))
                    continue;

                if (readKey.Durability != KeyValueDurability.Persistent)
                {
                    if (closesBundle)
                        return new OnePhaseEligibility(OnePhaseGateOutcome.NonPersistentRead, ApplyTimeValidation: true, OnPartitionReadDependencies: null);

                    continue;
                }

                if (manager.LocateDurablePartition(readKey.Key).PartitionId != input.AnchorPartitionId)
                {
                    if (closesBundle)
                        return new OnePhaseEligibility(OnePhaseGateOutcome.OffPartitionRead, ApplyTimeValidation: true, OnPartitionReadDependencies: null);

                    continue;
                }

                (onPartitionReads ??= []).Add(new BundledReadDependency(readKey.Key, readKey.Revision, readKey.Exists));
            }
        }

        return new OnePhaseEligibility(ReadSetExclusion: null, ApplyTimeValidation: true, OnPartitionReadDependencies: onPartitionReads);
    }

    /// <summary>
    /// Whether the validated read footprint reaches beyond the written keys: a point read of an unmodified key,
    /// or any prefix/range lock (a scan). Written keys with a validated base are re-fenced at prepare apply (the
    /// staged-base fence refuses the prepare when the base moved). A read-only dependency is protected only by
    /// commit-time validation, so without apply-time validation of the bundle, finalize must not pick a protocol
    /// that lets the decision apply after that validation goes stale. False when the read set is not validated.
    /// </summary>
    private static bool HasReadDependenciesBeyondWrites(TransactionContext context)
    {
        if (!RequiresReadSetValidation(context))
            return false;

        if (context.PrefixLocksAcquired is { Count: > 0 } || context.RangeLocksAcquired is { Count: > 0 })
            return true;

        if (context.ReadKeys is null || context.ReadKeys.Count == 0)
            return false;

        foreach (KeyValueTransactionReadKey readKey in context.ReadKeys.Values)
        {
            if (string.IsNullOrEmpty(readKey.Key))
                continue;

            if (context.ModifiedKeys is not null && context.ModifiedKeys.Contains((readKey.Key, readKey.Durability)))
                continue;

            return true;
        }

        return false;
    }

    /// <summary>
    /// Validates optimistic read dependencies against the current committed state before prepare.
    /// </summary>
    private async Task<bool> ValidateReadSet(TransactionContext context, CancellationToken cancellationToken)
    {
        if (!RequiresReadSetValidation(context))
            return true;

        // The same key was observed at two base states: the read set contradicts itself and cannot validate. Abort.
        if (context.ReadObservationConflict)
        {
            context.Result = new()
            {
                Type = KeyValueResponseType.Aborted,
                Reason = "Read observation instability: a key was observed at two different base revisions",
                Refusal = CommitRefusalClass.Conflict
            };

            return false;
        }

        if (context.ReadKeys is null || context.ReadKeys.Count == 0)
            return true;

        List<KeyValueTransactionReadKey> toValidate = [];

        foreach (KeyValueTransactionReadKey readKey in context.ReadKeys.Values)
        {
            if (string.IsNullOrEmpty(readKey.Key))
                continue;

            if (context.ModifiedKeys is not null && context.ModifiedKeys.Contains((readKey.Key, readKey.Durability)))
                continue;

            toValidate.Add(readKey);
        }

        if (toValidate.Count == 0)
            return true;

        List<(string key, long revision, KeyValueDurability durability)> probes = new(toValidate.Count);

        for (int i = 0; i < toValidate.Count; i++)
            probes.Add((toValidate[i].Key!, -1, toValidate[i].Durability));

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> results =
            await manager.LocateAndTryExistsManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, probes, cancellationToken);

        Dictionary<(string key, KeyValueDurability durability), (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry)> currentByKey = new(results.Count);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) result in results)
            currentByKey[(result.key, result.durability)] = (result.type, result.entry);

        foreach (KeyValueTransactionReadKey readKey in toValidate)
        {
            if (!currentByKey.TryGetValue((readKey.Key!, readKey.Durability), out (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) current))
            {
                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = $"Read dependency validation failed for {readKey.Key}: {KeyValueResponseType.Errored}",
                    Refusal = CommitRefusalClass.Conflict
                };

                logger.LogWarning(
                    "Read dependency validation failed for {Key} {Durability}: {Response}",
                    readKey.Key,
                    readKey.Durability,
                    KeyValueResponseType.Errored
                );

                return false;
            }

            ReadValidationFailure? failure = ValidateReadKey(readKey, current.type, current.entry);

            if (failure is null)
                continue;

            // A re-read that proved nothing (the leader asked for a retry) is not a conflict: refuse as MustRetry
            // and let the retry validate again.
            context.Result = new()
            {
                Type = failure.Value.Retryable ? KeyValueResponseType.MustRetry : KeyValueResponseType.Aborted,
                Reason = failure.Value.AbortReason
            };

            failure.Value.Log();

            return false;
        }

        return true;
    }

    /// <summary>
    /// Validates a single optimistic read dependency against the current committed state.
    /// </summary>
    private ReadValidationFailure? ValidateReadKey(
        KeyValueTransactionReadKey readKey,
        KeyValueResponseType response,
        ReadOnlyKeyValueEntry? current
    )
    {
        if (response is KeyValueResponseType.MustRetry or KeyValueResponseType.Errored or KeyValueResponseType.Aborted or KeyValueResponseType.WaitingForReplication)
            return new(
                $"Read dependency validation failed for {readKey.Key}: {response}",
                () => logger.LogWarning(
                    "Read dependency validation failed for {Key} {Durability}: {Response}",
                    readKey.Key,
                    readKey.Durability,
                    response
                ),
                Retryable: response is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
            );

        bool existsNow = response == KeyValueResponseType.Exists && current is not null;

        if (readKey.Exists != existsNow)
            return new(
                $"Read dependency changed for {readKey.Key}",
                () => logger.LogReadDependencyChanged(readKey.Key!, readKey.Durability, readKey.Exists, existsNow)
            );

        if (readKey.Exists && current!.Revision != readKey.Revision)
            return new(
                $"Read dependency revision changed for {readKey.Key}",
                () => logger.LogReadDependencyRevisionChanged(readKey.Key!, readKey.Durability, readKey.Revision, current.Revision)
            );

        return null;
    }

    /// <summary>
    /// A read dependency that did not validate. <paramref name="Retryable"/> marks a re-read that proved nothing
    /// (leader asked for a retry, or the value was in flight), not an observed change.
    /// </summary>
    private readonly record struct ReadValidationFailure(string AbortReason, Action Log, bool Retryable = false);

    /// <summary>
    /// The write-side compare-and-set, run by the finalizer before any durable proposal. Checks each frozen
    /// intent's validated base against the key's committed state. If the base moved after staging (the
    /// write-intent lease lapsed mid-finalize and another transaction committed that base first), this aborts
    /// instead of silently discarding the other writer's commit. First, every revision-carrying intent (blind
    /// writes included) is checked against this node's committed-head memory: a head at or above the staged
    /// revision means another transaction committed that revision, a conflict proven without I/O. Each
    /// validated-base key then resolves in three tiers:
    /// <list type="bullet">
    /// <item>A live durable intent of this transaction (a retry of a prepared, undecided attempt): the
    /// single-live-intent rule froze the base beneath it, and the check that admitted the prepare already ran.
    /// Valid.</item>
    /// <item>A foreign durable intent: its record decides. A committed winner whose revision is not this
    /// transaction's validated base is a moved-base conflict, even before it materializes into MVCC. Ordinary
    /// reads cannot answer this: they wait behind the intent or serve this transaction's own staged view. An
    /// aborted holder falls through to the committed read; an undecided one is a retryable unknown.</item>
    /// <item>No intent: the key's actor compares the committed head's revision and existence to the base via the
    /// staged-base conflict probe. Unlike an ordinary read, the transaction's own prefix lock over the key's
    /// bucket does not refuse it.</item>
    /// </list>
    /// </summary>
    private async Task<StagedBaseValidation> ValidateStagedBasesAsync(DurableFinalizeInput input, CancellationToken cancellationToken)
    {
        List<KeyValueConflictProbe> probes = [];
        List<PreparedIntent> toProbe = [];

        foreach (DurablePartitionPrepare partition in input.Partitions)
        {
            foreach (PreparedIntent staged in partition.Intents)
            {
                // Every revision-carrying write, blind ones included, must stage a revision above the committed
                // head. A head already there is another transaction's commit of that revision: this writer staged
                // under an exclusion lost to a leader change (its lock and intent lived in the deposed leader's
                // memory), and committing would write a second record at that revision. Local committed-head
                // memory answers without I/O, but it is a replica's view, so this is only an early check. The
                // prepare fence and the bundled commit gate apply the same rule at the apply position.
                if (!staged.NoRevision
                    && manager.DurablePreparedIntentStore.TryGetCommittedHead(staged.Key, out long headRevision, out _)
                    && headRevision >= staged.Revision)
                {
                    DurableTransactionMetrics.StagedRevisionCollisions.Add(1);
                    logger.LogWarning(
                        "Staged revision {Revision} for {Key} is already committed (committed head is revision {HeadRevision}); the write was staged under an exclusion a leader change took away",
                        staged.Revision, staged.Key, headRevision);
                    return StagedBaseValidation.Conflict;
                }

                // A blind write has no validated base: it is last-writer-wins. A foreign holder on its key is
                // for the prepare apply to resolve.
                if (!staged.HasValidatedBase)
                    continue;

                PreparedIntent? holder = manager.DurablePreparedIntentStore.Get(staged.Key);

                if (holder is not null)
                {
                    if (holder.TransactionId == input.TransactionId && holder.Epoch == input.Epoch)
                        continue;

                    TransactionRecord? holderRecord;
                    try
                    {
                        holderRecord = await manager.LookupDurableRecordRouted(
                            holder.TransactionId, holder.Epoch, holder.RecordAnchorKey, cancellationToken);
                    }
                    catch (PartitionNotHostedException)
                    {
                        // The holder's decision is unreachable: a retryable unknown. Never fall back to the
                        // intent's possibly-stale local resolution.
                        return StagedBaseValidation.Unknown;
                    }

                    // The routed record is authoritative. If it is gone (purged after settlement), use the
                    // resolution stamped on the intent.
                    TransactionDecision holderDecision = holderRecord?.Decision ?? holder.Resolution switch
                    {
                        PreparedIntentResolution.Committed => TransactionDecision.Commit,
                        PreparedIntentResolution.Aborted => TransactionDecision.Abort,
                        _ => TransactionDecision.Undecided
                    };

                    if (holderDecision == TransactionDecision.Commit)
                    {
                        // The holder's mutation is the key's next committed state, materialized or not. It is a
                        // valid base only if it is the base this transaction validated against. That is the usual
                        // case under deferred settlement: the observed base was materialized by this holder, whose
                        // intent awaits removal. An existing base matches by revision; an absent base matches by
                        // what the holder left behind, since an absent observation records no revision.
                        if (CommittedHolderIsTheValidatedBase(holder, staged, input.TransactionId))
                            continue;

                        logger.LogWarning(
                            "Staged base for {Key} was overtaken by committed transaction {HolderTransactionId} at revision {HolderRevision} state {HolderState} (validated base was {BaseRevision} {BaseState})",
                            staged.Key, holder.TransactionId, holder.Revision, holder.State, staged.BaseRevision, staged.BaseState);
                        return StagedBaseValidation.Conflict;
                    }

                    if (holderDecision == TransactionDecision.Undecided)
                        return StagedBaseValidation.Unknown;

                    // Aborted holder: its value never materializes; the committed state beneath it decides.
                }

                // The frozen base is the transaction's own exact pre-write observation. The probe carries its
                // revision, or -1 if the key was absent; the actor compares that to its committed head.
                probes.Add(new(staged.Key, KeyValueDurability.Persistent, KeyValueConflictChecks.StagedBase,
                    staged.BaseState == KeyValueState.Set ? staged.BaseRevision : -1));
                toProbe.Add(staged);
            }
        }

        if (probes.Count == 0)
            return StagedBaseValidation.Valid;

        // The compare runs in the key's actor as a conflict check, not a read. A live prefix lock over the
        // bucket refuses ordinary reads, and a pessimistic transaction that scanned a bucket and writes a member
        // holds that lock itself, so its finalize would spin on MustRetry until timeout. A transactional read
        // serves this transaction's staged view, not the committed base.
        //
        // The unconfirmed variant skips the read-index round for locally-led keys. This check only decides
        // whether to drive the durable decision proposal, and that proposal's replication fences a deposed
        // leader: a stale answer can cause a failed proposal or a conservative abort, never a durably wrong
        // outcome. Read the variant's contract before using it elsewhere.
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
            await manager.LocateAndTryCheckManyWriteIntentsUnconfirmed(input.TransactionId, probes, cancellationToken);

        Dictionary<string, KeyValueResponseType> byKey = new(results.Count);
        foreach ((KeyValueResponseType type, string key, KeyValueDurability _) in results)
            byKey[key] = type;

        foreach (PreparedIntent staged in toProbe)
        {
            if (!byKey.TryGetValue(staged.Key, out KeyValueResponseType current))
                return StagedBaseValidation.Unknown;

            switch (current)
            {
                // Compare passed: existence and revision still match the base.
                case KeyValueResponseType.DoesNotExist:
                    continue;

                // Compare failed: existence flipped, or the committed head moved off the base revision.
                case KeyValueResponseType.NotSet:
                    logger.LogWarning(
                        "Staged base changed for {Key}: validated against exists={BaseExisted} rev={BaseRevision}, the committed state no longer matches",
                        staged.Key, staged.BaseState == KeyValueState.Set, staged.BaseRevision);
                    return StagedBaseValidation.Conflict;

                // A refused, gated, redirected or failed probe decides nothing.
                default:
                    return StagedBaseValidation.Unknown;
            }
        }

        return StagedBaseValidation.Valid;
    }

    /// <summary>
    /// Whether a committed foreign intent still on a key is the base the staged write validated against, so the
    /// write may commit over it. The base records a revision only when the key existed; an absent observation
    /// does not say which mutation left the key absent. So an existing base matches by revision (and the holder
    /// must be a value, not a tombstone at that revision). An absent base matches by effect: a committed delete,
    /// or a committed value that expired before this transaction began, so every read it made saw the key
    /// absent. Any other committed holder overtook the base.
    /// </summary>
    private static bool CommittedHolderIsTheValidatedBase(PreparedIntent holder, PreparedIntent staged, HLCTimestamp transactionId)
    {
        if (staged.BaseState == KeyValueState.Set)
            return holder.State == KeyValueState.Set && holder.Revision == staged.BaseRevision;

        if (holder.State == KeyValueState.Deleted)
            return true;

        return holder.Expires != HLCTimestamp.Zero && holder.Expires.CompareTo(transactionId) < 0;
    }

    /// <summary>
    /// Claims every staged intent of a two-phase finalize before its first proposal, and returns the highest
    /// commit floor a snapshot read left on them (<see cref="DurableTransactionFinalizer.ClaimStagedIntentsDelegate"/>).
    /// One confirmed probe per node owning written keys asks each key's leader whether the staged write is still
    /// covered by its intent (<see cref="KeyValueConflictChecks.OwnStagedIntent"/>) and what floor the intent
    /// carries (<see cref="KeyValueConflictChecks.CommitFloor"/>). A held answer claims the intent, so no snapshot
    /// read pushes it afterwards. A lost one names the key; post-prepare validation then refuses the commit and
    /// attributes the loss (a lost lock over a lost staging) with its usual counters. An answer that proved
    /// nothing (leadership unconfirmed, a gated key, no answer) is unknown. The post-prepare probe asks again and
    /// finds the intents already claimed.
    /// </summary>
    private async Task<StagedIntentClaim> ClaimStagedIntentsAsync(DurableFinalizeInput input, CancellationToken cancellationToken)
    {
        List<KeyValueConflictProbe> probes = new(input.Manifest.Count);

        foreach (DurablePartitionPrepare partition in input.Partitions)
        {
            foreach (PreparedIntent intent in partition.Intents)
                probes.Add(new(intent.Key, KeyValueDurability.Persistent, KeyValueConflictChecks.OwnStagedIntent | KeyValueConflictChecks.CommitFloor));
        }

        if (probes.Count == 0)
            return new StagedIntentClaim(StagedIntentClaimOutcome.Held, HLCTimestamp.Zero, null);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results;
        HLCTimestamp commitFloor;

        using (LockGrantScope.Begin(out LockGrantCapture capture))
        {
            results = await manager.LocateAndTryCheckManyWriteIntents(input.TransactionId, probes, cancellationToken).ConfigureAwait(false);
            commitFloor = capture.CommitFloor;
        }

        // Two answers for one key differ only if one is not clean, and that one counts; see ProbeCommitConflicts.
        Dictionary<string, KeyValueResponseType> byKey = new(results.Count, StringComparer.Ordinal);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability _) in results)
        {
            if (byKey.TryGetValue(key, out KeyValueResponseType existing) && ProbeAnswerRank(existing) >= ProbeAnswerRank(type))
                continue;

            byKey[key] = type;
        }

        bool unknown = false;

        foreach (KeyValueConflictProbe probe in probes)
        {
            if (!byKey.TryGetValue(probe.Key, out KeyValueResponseType type))
            {
                unknown = true;
                continue;
            }

            switch (type)
            {
                case KeyValueResponseType.DoesNotExist:
                    continue;

                case KeyValueResponseType.Unlocked:
                    return new StagedIntentClaim(StagedIntentClaimOutcome.Lost, commitFloor, probe.Key);

                default:
                    unknown = true;
                    break;
            }
        }

        if (unknown)
        {
            DurableTransactionMetrics.CommitProbeUnproven();
            return new StagedIntentClaim(StagedIntentClaimOutcome.Unknown, commitFloor, null);
        }

        return new StagedIntentClaim(StagedIntentClaimOutcome.Held, commitFloor, null);
    }
}
