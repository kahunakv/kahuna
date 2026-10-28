
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

// Commit conflict checks, lost-lock detection, and probes.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// The commit barrier's two checks, run side by side: every granted lock is still in force
    /// (<see cref="FindLostLock"/>), and no forbidden conflict exists at commit
    /// (<see cref="ProbeCommitConflicts"/>). False sets Aborted, or MustRetry when the probe proved nothing.
    /// </summary>
    private async Task<bool> CheckCommitConflicts(TransactionContext context, CancellationToken cancellationToken)
    {
        // Both checks ask the same leaders and neither needs the other's answer. On the two-phase flow the lock
        // proof runs here, after the prepares are durable: from then on replicated intents protect the written
        // keys, so the locks only had to hold until now. The answers stay separate because a lock's routing key
        // can be a written key, and merged answers would be indistinguishable.
        Task<LostLock?> lostLock = FindLostLock(context, cancellationToken);
        Task<bool> conflictFree = ProbeCommitConflicts(context, cancellationToken);

        await Task.WhenAll(lostLock, conflictFree).ConfigureAwait(false);

        // A lost lock wins over a conflict found beside it: it is the cause, and the conflict (e.g. a foreign
        // range lock on a written key) is usually its consequence.
        if (await lostLock.ConfigureAwait(false) is { } lost)
        {
            RefuseLostLock(context, lost);
            return false;
        }

        return await conflictFree.ConfigureAwait(false);
    }

    /// <summary>A lock the transaction was granted that can no longer be proven held, and how that surfaced.</summary>
    private readonly record struct LostLock(LostLockDetection Detection, string Reason);

    /// <summary>How many times the lock proof asks again when a leader answers that it cannot say right now.</summary>
    private const int LockProofAttempts = 3;

    /// <summary>
    /// Proves that every lock the transaction was granted is still in force, or sets the Aborted result and
    /// returns false. See <see cref="FindLostLock"/>.
    /// </summary>
    private async Task<bool> LocksStillInForce(TransactionContext context, CancellationToken cancellationToken)
    {
        if (await FindLostLock(context, cancellationToken).ConfigureAwait(false) is not { } lost)
            return true;

        RefuseLostLock(context, lost);
        return false;
    }

    /// <summary>
    /// Refuses the commit of a transaction that lost a lock. Nothing it staged was committed, so the client
    /// restarts it. Counted once per transaction, however many finalize stages reach this verdict. Marked as a
    /// lost exclusion, not a conflict: an interactive session gets a terminal Aborted (its reads under the lost
    /// lock cannot be repeated); a script gets MustRetry, since a new run takes new locks and reads again.
    /// </summary>
    private void RefuseLostLock(TransactionContext context, LostLock lost)
    {
        if (!context.LostLockRefused)
        {
            context.LostLockRefused = true;
            DurableTransactionMetrics.LostLockAborted(lost.Detection);
            logger.LogWarning("Refusing to commit transaction {TransactionId}: {Reason}", context.TransactionId, lost.Reason);
        }

        context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Lost lock: " + lost.Reason, ExclusionLost = true, Refusal = CommitRefusalClass.LostLock };
    }

    /// <summary>
    /// Looks for a granted lock that may no longer be held. Returns null when every lock is proven in force (or
    /// none reported its leadership).
    ///
    /// <para>Point, prefix and range locks are in-memory state of the granting partition leader. A leader change
    /// drops them silently: the coordinator still lists the lock, the new leader answers a later renewal or
    /// upgrade as a fresh grant, and may grant the same keys to another transaction. Each grant reported its
    /// Raft term (<see cref="LockGrantTerm"/>). The proof is that the partition's <b>confirmed</b> leader is
    /// still in that term: a term has one leader, and a node that stops leading can only lead again in a later
    /// term, so the same term means the same leader, uninterrupted, with the lock still in memory.</para>
    ///
    /// <para>The proof fails on: a different term (leadership changed); a key that now routes to another
    /// partition (the range moved, and the new partition's term says nothing about the old grant); no confirmed
    /// answer after a few attempts (likely an election, which changes the term anyway); a lapsed lease (same
    /// leader, but it stopped honoring one of this transaction's range locks because its time ran out, see
    /// <see cref="LapsedRangeLockRegistry"/>; a later renewal re-creates the lock but does not undo that).
    /// Failing closed costs a retryable abort; failing open would commit on an exclusion nobody holds.</para>
    /// </summary>
    private async Task<LostLock?> FindLostLock(TransactionContext context, CancellationToken cancellationToken)
    {
        if (context.LockGrantTermChange is { } termChange)
            return new LostLock(LostLockDetection.Regrant, termChange);

        if (context.SnapshotLockGrantTerms() is not { } grants)
            return null;

        List<KeyValueConflictProbe> probes = new(grants.Count);

        foreach (LockGrantTerm grant in grants)
        {
            int routedPartition;
            try
            {
                routedPartition = manager.LocateDurablePartition(grant.RoutingKey).PartitionId;
            }
            catch (KahunaServerException)
            {
                // No range descriptor covers the key any more.
                routedPartition = -1;
            }

            if (routedPartition != grant.PartitionId)
                return new LostLock(
                    LostLockDetection.RangeMoved,
                    $"a lock was granted on partition {grant.PartitionId} in term {grant.Term}, and its key now routes to partition {routedPartition}");

            probes.Add(new(grant.RoutingKey, KeyValueDurability.Persistent, KeyValueConflictChecks.LeaderTerm, grant.Term));
        }

        Dictionary<string, KeyValueResponseType> byKey = new(probes.Count, StringComparer.Ordinal);

        for (int attempt = 1; ; attempt++)
        {
            // No confirmation round in the locator: the leader-term check already confirms the answering node's
            // leadership (KeyValueLocator.CheckLeaderTerm). A second round would run after it instead of beside
            // the conflict probe, costing the commit a durable round under load.
            List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
                await manager.LocateAndTryCheckManyWriteIntentsUnconfirmed(context.TransactionId, probes, cancellationToken).ConfigureAwait(false);

            byKey.Clear();
            foreach ((KeyValueResponseType type, string key, KeyValueDurability _) in results)
                byKey[key] = type;

            bool undecided = false;

            foreach (LockGrantTerm grant in grants)
            {
                if (!byKey.TryGetValue(grant.RoutingKey, out KeyValueResponseType type))
                    type = KeyValueResponseType.Errored;

                switch (type)
                {
                    // The confirmed leader is still in the term of the grant.
                    case KeyValueResponseType.DoesNotExist:
                        continue;

                    case KeyValueResponseType.Aborted:
                        return new LostLock(
                            LostLockDetection.CommitProbe,
                            $"partition {grant.PartitionId} is no longer led under term {grant.Term}, in which it granted this transaction a lock; the lock was dropped by the leader change");

                    case KeyValueResponseType.Unlocked:
                        return new LostLock(
                            LostLockDetection.LeaseLapsed,
                            $"the leader of partition {grant.PartitionId} stopped honoring a range lock of this transaction: its lease ran out before the transaction released it");

                    default:
                        undecided = true;
                        break;
                }
            }

            if (!undecided)
                return null;

            if (attempt >= LockProofAttempts)
                return new LostLock(
                    LostLockDetection.Unconfirmed,
                    "no partition leader confirmed that it still leads under the term in which it granted this transaction a lock");

            await Task.Delay(attempt * 10, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Probes, in one batched call, for the two conflicts that must not exist at commit:
    ///
    /// <list type="bullet">
    /// <item>a concurrent writer on a <b>read set</b> key — the write-skew guard. Only optimistic (or explicitly
    /// read-validating) transactions need it; exclusive key locks serialize the rest;</item>
    /// <item>a foreign range lock covering a <b>write set</b> key, acquired after the write was staged, so the
    /// write-time fence could not see it. Shared and Exclusive acquires are refused over a live foreign write
    /// intent, so this is their backstop. A WriteFence steps around intents by design (a quiesce must not wait
    /// for in-flight writers) and relies on this check. Not gated by locking mode: a fence steps around held key
    /// locks too, so pessimistic transactions are equally exposed.</item>
    /// </list>
    ///
    /// The sets are disjoint (a key both read and written is validated as a write), so a flagged answer is
    /// attributed by its set.
    ///
    /// Only the clean answer (<see cref="KeyValueResponseType.DoesNotExist"/>) passes a key. A found conflict or
    /// a lost own intent refuses as <c>Aborted</c>. An answer that proved nothing —
    /// <see cref="KeyValueResponseType.MustRetry"/> from a leader that could not confirm leadership or from a
    /// gated partition, or an answer this node does not know — refuses as <c>MustRetry</c>: nothing was found
    /// or decided.
    /// </summary>
    private async Task<bool> ProbeCommitConflicts(TransactionContext context, CancellationToken cancellationToken)
    {
        List<KeyValueConflictProbe> probeKeys = [];

        // Read keys ask only about concurrent write intents: a range lock covering a key this transaction
        // merely read is not a conflict for it.
        if (RequiresReadSetValidation(context) && context.ReadKeys is { Count: > 0 })
        {
            foreach (KeyValueTransactionReadKey readKey in context.ReadKeys.Values)
            {
                if (string.IsNullOrEmpty(readKey.Key))
                    continue;

                if (context.ModifiedKeys is not null && context.ModifiedKeys.Contains((readKey.Key, readKey.Durability)))
                    continue;

                probeKeys.Add(new(readKey.Key!, readKey.Durability, KeyValueConflictChecks.WriteIntent));
            }
        }

        // Write keys ask about foreign range locks and their own staged write intent. The commit timestamp is
        // frozen before any prepare lands; until then the in-memory staged intent is all a snapshot read meeting
        // the write consults. The read leaves its commit floor on the intent while the commit can still move, and
        // waits behind it once claimed. An intent that lapsed, was taken, or was dropped by a leader change may
        // have let a read return the old value at a snapshot this commit lands inside, with no floor recorded, so
        // the commit is refused. On the two-phase flows this probe runs after the prepares are durable
        // (ClaimStagedIntentsAsync claims before them), and the prepared intents then make reads wait. The
        // one-phase bundle validates before proposing and cannot be refused once appended, so the probe closes
        // the gap to its apply two ways: a held intent is claimed for the commit and stops lapsing by lease, and
        // the confirming leader reports its term, to which the bundle is fenced at the anchor leader.
        //
        // Write keys do not ask about foreign write intents: those are the durable prepare's conflict to resolve
        // (it retries and helps the blocker settle); flagging them here would turn retryable contention into a
        // hard abort. Nor about staged bases: the intent store's staged-base fence catches a moved base at the
        // prepare's apply position and drives the abort. Asking here would add a per-key entry load inside the
        // post-prepare barrier, while durable intents freeze the hottest keys, for an answer apply already gives.
        if (context.ModifiedKeys is not null)
        {
            foreach ((string key, KeyValueDurability durability) in context.ModifiedKeys)
            {
                if (string.IsNullOrEmpty(key))
                    continue;

                // The floor comes with the claim: a snapshot read served over the staged write before the claim
                // left the highest timestamp it answered at, and the commit is minted above it.
                probeKeys.Add(new(key, durability, KeyValueConflictChecks.ForeignRangeLock | KeyValueConflictChecks.OwnStagedIntent | KeyValueConflictChecks.CommitFloor));
            }
        }

        if (probeKeys.Count == 0)
            return true;

        // One probe per node owning part of the set, not one per key, which would cost a round trip per remote
        // key. The capture collects, per partition, the term under which a leader confirmed this transaction's
        // staged intents held; the one-phase bundle is fenced to it (see AnchorExclusionTerm). It also collects
        // the highest commit floor over those intents, which the finalizer compares with the frozen commit
        // timestamp before proposing.
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results;

        using (LockGrantScope.Begin(out LockGrantCapture stagedIntentTerms))
        {
            results = await manager.LocateAndTryCheckManyWriteIntents(context.TransactionId, probeKeys, cancellationToken);
            context.RecordStagedIntentTerms(stagedIntentTerms.Take());
            context.RaiseClaimedCommitFloor(stagedIntentTerms.CommitFloor);
        }

        Dictionary<(string, KeyValueDurability), KeyValueResponseType> byKey = new(results.Count);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in results)
        {
            // Two answers for one key differ only if one is not clean, and that one matters: never let a cleaner
            // second answer overwrite it. Rank: found conflict (Aborted, or NotSet, the staged-base fence's
            // "compare failed") > lost own intent > proved nothing > clean.
            if (byKey.TryGetValue((key, durability), out KeyValueResponseType existing)
                && ProbeAnswerRank(existing) >= ProbeAnswerRank(type))
                continue;

            byKey[(key, durability)] = type;
        }

        foreach ((string key, KeyValueDurability durability, KeyValueConflictChecks checks, _) in probeKeys)
        {
            // Tell the set by the question asked, not by list position: only read keys ask about write intents.
            bool isReadKey = checks == KeyValueConflictChecks.WriteIntent;

            // A probed key with no answer means the probe did not cover the set: a broken contract, not a
            // transient outcome. Reading it as clean would silently disable its guard, so it aborts like a
            // detected conflict.
            if (!byKey.TryGetValue((key, durability), out KeyValueResponseType type))
            {
                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = isReadKey
                        ? $"Write intent probe returned no result for read key {key}"
                        : $"Range lock probe returned no result for written key {key}",
                    Refusal = CommitRefusalClass.Conflict
                };

                logger.LogWriteSkewGuardAborted(context.TransactionId, key);

                return false;
            }

            // Defensive: NotSet is the staged-base probe's "compare failed". This coordinator no longer asks for
            // that check (the apply-time fence owns it), but a mixed-version peer may still answer it, and a
            // failed compare must abort, never read as clean.
            if (type == KeyValueResponseType.NotSet)
            {
                DurableTransactionMetrics.StagedBasePostPrepareAborts.Add(1);

                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = $"Committed base for written key {key} changed after validation",
                    Refusal = CommitRefusalClass.StagedBaseMoved
                };

                logger.LogWarning(
                    "Staged base for {Key} moved between validation and prepare of transaction {TransactionId}; aborting to prevent a lost update",
                    key, context.TransactionId);

                return false;
            }

            if (type == KeyValueResponseType.Unlocked && !isReadKey)
            {
                DurableTransactionMetrics.LostStagedIntentAborts.Add(1);

                // The transaction's own exclusion over the key is gone (lease lapsed or leader change), not a
                // conflict caused by another transaction. Marked as a lost exclusion so a script re-runs
                // instead of reporting an abort.
                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = $"Write intent of the staged write on key {key} was lost before commit",
                    ExclusionLost = true,
                    Refusal = CommitRefusalClass.LostStagedIntent
                };

                logger.LogWarning(
                    "Refusing to commit transaction {TransactionId}: the write intent its staged write on {Key} relied on lapsed or was dropped",
                    context.TransactionId, key);

                return false;
            }

            if (type == KeyValueResponseType.DoesNotExist)
                continue;

            // Any other answer proved nothing: the leader could not confirm leadership for the key's partition
            // (read-index failed, or the partition is gated as incomplete), no leader was known, or a newer peer
            // answered something this node does not know. Reading it as clean would silently disable every guard
            // asked about (write-skew on a read key; range-lock fence and staged-intent hold on a written key),
            // and the one-phase bundle would propose with no term to fence to. Nothing was found or decided, so
            // refuse with MustRetry, not Aborted: the retry probes again and finds the intent held or gone.
            if (type != KeyValueResponseType.Aborted)
            {
                DurableTransactionMetrics.CommitProbeUnproven();

                context.Result = new()
                {
                    Type = KeyValueResponseType.MustRetry,
                    Reason = isReadKey
                        ? $"Commit probe could not be completed for read key {key}: the leader answered {type}"
                        : $"Commit probe could not be completed for written key {key}: the leader answered {type}",
                    Refusal = CommitRefusalClass.Unresolved
                };

                logger.LogWarning(
                    "Commit probe of transaction {TransactionId} proved nothing about key {Key} (the leader answered {Answer}); refusing with MustRetry",
                    context.TransactionId, key, type);

                return false;
            }

            if (!isReadKey)
                DurableTransactionMetrics.RangeLockFenceAborts.Add(1);

            context.Result = new()
            {
                Type = KeyValueResponseType.Aborted,
                Reason = isReadKey
                    ? $"Concurrent write intent detected on read key {key}"
                    : $"Foreign range lock covers written key {key}",
                Refusal = CommitRefusalClass.Conflict
            };

            logger.LogWriteSkewGuardAborted(context.TransactionId, key);

            return false;
        }

        return true;
    }

    /// <summary>
    /// Weight of a commit probe answer when two cover one key (see <see cref="ProbeCommitConflicts"/>): found
    /// conflict, then lost own intent, then proved nothing, then clean.
    /// </summary>
    private static int ProbeAnswerRank(KeyValueResponseType type) => type switch
    {
        KeyValueResponseType.Aborted or KeyValueResponseType.NotSet => 3,
        KeyValueResponseType.Unlocked => 2,
        KeyValueResponseType.DoesNotExist => 0,
        _ => 1
    };

    /// <summary>
    /// The finalize pin for a yielding transaction. Before any prepare it claims every write intent the
    /// transaction still holds (one probe per owning node), so a foreground writer cannot take one over before
    /// the durable prepare. A key already taken over answers <c>Aborted</c>, and the transaction aborts instead
    /// of committing over the winner. Hard invariant: a yielding transaction never commits a write to a key it
    /// lost.
    ///
    /// <para>Pinned keys are its point locks and modified keys: every key a yielding intent may live on. A
    /// yielding transaction cannot hold prefix or range locks (those acquires are refused), so this is exactly
    /// its point-key intents.</para>
    /// </summary>
    private async Task<bool> PinYieldingIntents(TransactionContext context, CancellationToken cancellationToken)
    {
        HashSet<(string, KeyValueDurability)> keys = [];
        if (context.LocksAcquired is not null)
            foreach ((string, KeyValueDurability) k in context.LocksAcquired)
                keys.Add(k);
        if (context.ModifiedKeys is not null)
            foreach ((string, KeyValueDurability) k in context.ModifiedKeys)
                keys.Add(k);

        if (keys.Count == 0)
            return true;

        List<KeyValueConflictProbe> probes = new(keys.Count);
        foreach ((string key, KeyValueDurability durability) in keys)
        {
            if (!string.IsNullOrEmpty(key))
                probes.Add(new(key, durability, KeyValueConflictChecks.PinOwnIntent));
        }

        if (probes.Count == 0)
            return true;

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
            await manager.LocateAndTryCheckManyWriteIntents(context.TransactionId, probes, cancellationToken);

        Dictionary<(string, KeyValueDurability), KeyValueResponseType> byKey = new(results.Count);
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in results)
        {
            // An Aborted answer means a takeover was detected: never let a cleaner second answer overwrite it.
            if (byKey.TryGetValue((key, durability), out KeyValueResponseType existing) && existing == KeyValueResponseType.Aborted)
                continue;
            byKey[(key, durability)] = type;
        }

        foreach (KeyValueConflictProbe probe in probes)
        {
            // An unanswered key means the pin did not cover the set: a broken contract, not a clean answer.
            // Treat it as a loss so the invariant is never silently disabled.
            if (!byKey.TryGetValue((probe.Key, probe.Durability), out KeyValueResponseType type) || type == KeyValueResponseType.Aborted)
            {
                DurableTransactionMetrics.RecordYieldAbortAtPin();
                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = $"Yielding transaction lost key {probe.Key} to a foreground writer",
                    Refusal = CommitRefusalClass.Conflict
                };
                return false;
            }
        }

        return true;
    }
}
