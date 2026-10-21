using System.Collections.Concurrent;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The committed-head ledger, settled identities, and history windows of <see cref="PreparedIntentStore"/>.</summary>
internal sealed partial class PreparedIntentStore
{
    /// <summary>
    /// Reads the fence's committed-head memory for <paramref name="key"/>: the revision and state of the last
    /// durable-transaction commit whose settlement this node applied within retention. Lock-free (the map is
    /// concurrent and heads are monotonic per key), so read paths may probe it per operation. False means the
    /// memory holds nothing for the key — which can mean "no commit ever happened" or "the memory was pruned
    /// or never fed"; callers must not treat absence as proof of no history.
    /// </summary>
    internal bool TryGetCommittedHead(string key, out long revision, out KeyValueState state)
    {
        if (TryFindHead(key, out CommittedHead head))
        {
            revision = head.Revision;
            state = head.State;
            return true;
        }

        revision = -1;
        state = KeyValueState.Undefined;
        return false;
    }

    /// <summary>Number of keys currently held across every committed-head ledger slice. Observability only —
    /// logged in the leadership-change fingerprint so a promotion with an empty memory is visible in the node
    /// log, and exported as a gauge.</summary>
    internal int CommittedHeadCount
    {
        get
        {
            int count = 0;
            foreach (PartitionLedger ledger in ledgers.Values)
                count += ledger.Heads.Count;
            return count;
        }
    }

    /// <summary>Number of keys the committed-head ledger slice of <paramref name="partitionId"/> holds; 0 when the
    /// node holds no slice for it. Feeds the per-partition apply fingerprint, where two replicas at the same
    /// applied log id must report the same count.</summary>
    internal int CommittedHeadCountForPartition(int partitionId) =>
        ledgers.TryGetValue(partitionId, out PartitionLedger? ledger) ? ledger.Heads.Count : 0;

    /// <summary>Approximate bytes retained by every committed-head ledger slice (gauge).</summary>
    internal long CommittedHeadLedgerBytes
    {
        get
        {
            long bytes = 0;
            foreach (PartitionLedger ledger in ledgers.Values)
                bytes += Volatile.Read(ref ledger.Bytes);
            return bytes;
        }
    }

    /// <summary>Per-partition (entries, bytes) of the committed-head ledger, for the tagged gauges. The
    /// unattributed slice is reported under <see cref="UnattributedPartition"/>.</summary>
    internal IReadOnlyList<(int PartitionId, long Entries, long Bytes)> SnapshotLedgerSizes()
    {
        List<(int, long, long)> sizes = new(ledgers.Count);
        foreach (KeyValuePair<int, PartitionLedger> slice in ledgers)
            sizes.Add((slice.Key, slice.Value.Heads.Count, Volatile.Read(ref slice.Value.Bytes)));
        return sizes;
    }

    /// <summary>The committed head a partition's ledger slice holds for <paramref name="key"/>, or false when
    /// the slice holds none. Test/observability seam for the parity assertions: unlike
    /// <see cref="TryGetCommittedHead"/> it reads exactly one slice, the state the bundled commit gate judges by.</summary>
    internal bool TryGetLedgerHead(int partitionId, string key, out long revision, out KeyValueState state, out HLCTimestamp committedAt)
    {
        if (ledgers.TryGetValue(partitionId, out PartitionLedger? ledger) && ledger.Heads.TryGetValue(key, out CommittedHead head))
        {
            revision = head.Revision;
            state = head.State;
            committedAt = head.CommittedAt;
            return true;
        }

        revision = -1;
        state = KeyValueState.Undefined;
        committedAt = HLCTimestamp.Zero;
        return false;
    }

    /// <summary>A partition's ledger watermark (the highest commit timestamp its slice recorded), or Zero.</summary>
    internal HLCTimestamp GetLedgerWatermark(int partitionId)
    {
        lock (applyGate)
            return ledgers.TryGetValue(partitionId, out PartitionLedger? ledger) ? ledger.Watermark : HLCTimestamp.Zero;
    }

    /// <summary>Every (key, revision, state, committedAt) a partition's ledger slice currently retains, sorted by
    /// key. Test seam for cross-replica parity assertions.</summary>
    internal IReadOnlyList<(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)> SnapshotLedger(int partitionId)
    {
        List<(string, long, KeyValueState, HLCTimestamp)> entries = [];

        if (ledgers.TryGetValue(partitionId, out PartitionLedger? ledger))
        {
            foreach (KeyValuePair<string, CommittedHead> kv in ledger.Heads)
                entries.Add((kv.Key, kv.Value.Revision, kv.Value.State, kv.Value.CommittedAt));
        }

        entries.Sort(static (a, b) => string.CompareOrdinal(a.Item1, b.Item1));
        return entries;
    }

    /// <summary>Every settled identity a partition's ledger slice currently retains, sorted by transaction id,
    /// epoch, then key. Test seam for cross-replica parity assertions.</summary>
    internal IReadOnlyList<SettledIdentityEntry> SnapshotSettledIdentities(int partitionId)
    {
        List<SettledIdentityEntry> entries;
        lock (applyGate)
        {
            entries = ledgers.TryGetValue(partitionId, out PartitionLedger? ledger) ? CopySettledLocked(ledger) : [];
        }

        entries.Sort(static (a, b) =>
        {
            int byId = a.TransactionId.CompareTo(b.TransactionId);
            if (byId != 0)
                return byId;
            int byEpoch = a.Epoch.CompareTo(b.Epoch);
            return byEpoch != 0 ? byEpoch : string.CompareOrdinal(a.Key, b.Key);
        });
        return entries;
    }

    /// <summary>Number of live prepared intents currently held. Observability only — logged in the
    /// leadership-change fingerprint alongside <see cref="CommittedHeadCount"/>.</summary>
    internal int LiveIntentCount => intents.Count;

    /// <summary>
    /// Number of live prepared intents whose key routes to <paramref name="partitionId"/>. Feeds the per-partition
    /// apply fingerprint, where two replicas at the same applied log id must report the same count: prepares and
    /// settlements both apply from the log. Without a resolver every intent counts (a single-partition store).
    /// </summary>
    /// <remarks>
    /// A key the resolver cannot route (its key range is not in the map on this node yet) is left out rather
    /// than failing the count: the fingerprint is a diagnostic read, and an unroutable key belongs to no
    /// partition this node compares.
    /// </remarks>
    internal int LiveIntentCountForPartition(int partitionId)
    {
        Func<string, int>? resolver = resolvePartition;
        if (resolver is null)
            return intents.Count;

        int count = 0;

        foreach (KeyValuePair<string, PreparedIntent> kv in intents)
        {
            if (TryResolvePartition(resolver, kv.Key, out int owner) && owner == partitionId)
                count++;
        }

        return count;
    }

    /// <summary>Whether this node still holds an intent for exactly this transaction attempt on <paramref name="key"/>,
    /// pending or resolved-but-not-yet-removed. The presence answer of the record-less hold cross-check.</summary>
    internal bool HoldsIntent(HLCTimestamp transactionId, long epoch, string key) =>
        GetByIdentity(transactionId, epoch, key) is not null;

    /// <summary>
    /// Requests the convergence repair for a key whose locally visible durable row was observed strictly below
    /// the committed-head memory — the same wiring the fence-wedge watchdog drives (re-drive the parked head
    /// mutation, then reconcile from local revision history). A no-op when the memory does not actually exceed
    /// <paramref name="observedRevision"/> (a benign race with a concurrent settle) or when no hook is wired.
    /// Never blocks: the wired hook only schedules detached work. Returns whether a repair was requested.
    /// </summary>
    internal bool RequestConvergenceRepair(string key, long observedRevision)
    {
        if (!TryFindHead(key, out CommittedHead head) || head.Revision <= observedRevision)
            return false;

        onFenceWedgeRepair?.Invoke(key, observedRevision, head.Revision);
        return true;
    }

    /// <summary>
    /// Reads this node's staged-base fence verdict for each of one transaction's validated-base prepares —
    /// the replica half of the pre-decision fence confirmation. A key whose still-pending intent this store
    /// holds under the given identity is judged with the same fence the apply path runs; while that intent is
    /// live, the single-live-intent rule freezes the key's committed head, so the answer equals the verdict at
    /// the prepare's own apply position. A key without that intent answers
    /// <see cref="KeyValueStagedBaseVerdict.NotApplied"/> (this node cannot attest), and a resolved intent or
    /// a blind revision-free write answers <see cref="KeyValueStagedBaseVerdict.Clear"/> (nothing left for the
    /// fence to judge). Evaluated under the apply gate so the (intent, head, watermark) read is consistent.
    /// </summary>
    internal KeyValueStagedBaseVerdictEntry[] EvaluateReplicaFenceVerdicts(
        HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys)
    {
        KeyValueStagedBaseVerdictEntry[] verdicts = new KeyValueStagedBaseVerdictEntry[keys.Count];

        lock (applyGate)
        {
            for (int i = 0; i < keys.Count; i++)
            {
                if (!intents.TryGetValue(keys[i], out PreparedIntent? intent)
                    || intent.TransactionId != transactionId
                    || intent.Epoch != epoch)
                {
                    verdicts[i] = new(
                        KeyValueStagedBaseVerdict.NotApplied,
                        TryFindHead(keys[i], out CommittedHead head) ? head.Revision : -1);
                    continue;
                }

                if (!intent.IsPending || !IsFenceJudged(intent))
                {
                    verdicts[i] = new(KeyValueStagedBaseVerdict.Clear, -1);
                    continue;
                }

                string? conflict = JudgeStagedBase(intent, countAbsentHeadAdmission: false, out long headRevision, out _);
                verdicts[i] = new(
                    conflict is null ? KeyValueStagedBaseVerdict.Clear : KeyValueStagedBaseVerdict.StaleBase,
                    headRevision);
            }
        }

        return verdicts;
    }

    /// <summary>
    /// The waiting form of <see cref="EvaluateReplicaFenceVerdicts"/>: while any key answers
    /// <see cref="KeyValueStagedBaseVerdict.NotApplied"/>, parks on the intent-set pulse and re-evaluates
    /// when the set changes, until every key is attested or <paramref name="waitMs"/> runs out. Event-driven:
    /// a prepare that applies moments after the request wakes the waiter immediately, so a healthy in-flight
    /// apply costs the wake latency, never a poll tick. Returns the latest evaluation; cancellation and an
    /// exhausted budget return it as it stands (<c>NotApplied</c> is never an objection to the caller).
    /// </summary>
    internal async Task<KeyValueStagedBaseVerdictEntry[]> EvaluateReplicaFenceVerdictsAsync(
        HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys, int waitMs, CancellationToken cancellationToken)
    {
        long deadline = Environment.TickCount64 + waitMs;

        KeyValueStagedBaseVerdictEntry[] verdicts = EvaluateReplicaFenceVerdicts(transactionId, epoch, keys);
        if (!AnyNotApplied(verdicts))
            return verdicts;

        Interlocked.Increment(ref fenceWaiterCount);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Capture the pulse BEFORE evaluating: a mutation that applies after this evaluation then
                // completes the captured instance, so satisfaction can never slip past a parked waiter.
                Task changed = Volatile.Read(ref intentSetChangedPulse).Task;

                verdicts = EvaluateReplicaFenceVerdicts(transactionId, epoch, keys);
                if (!AnyNotApplied(verdicts))
                    return verdicts;

                long remainingMs = deadline - Environment.TickCount64;
                if (remainingMs <= 0)
                    return verdicts;

                try
                {
                    await changed.WaitAsync(TimeSpan.FromMilliseconds(remainingMs), cancellationToken).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    // Budget exhausted; the loop runs one final fresh evaluation and returns it.
                }
                catch (OperationCanceledException)
                {
                    return verdicts;
                }
            }

            return verdicts;
        }
        finally
        {
            Interlocked.Decrement(ref fenceWaiterCount);
        }
    }

    private static bool AnyNotApplied(KeyValueStagedBaseVerdictEntry[] verdicts)
    {
        foreach (KeyValueStagedBaseVerdictEntry verdict in verdicts)
        {
            if (verdict.Verdict == KeyValueStagedBaseVerdict.NotApplied)
                return true;
        }

        return false;
    }

    /// <summary>Empties every committed-head ledger slice, reproducing the memory a node holds after it lost
    /// its ledger (a snapshot written before the ledger existed, or an install from such a node): the fence
    /// then has no head to judge against. Test seam for driving that window deterministically; never called by
    /// production code.</summary>
    internal void ForgetCommittedHeadsForTesting()
    {
        lock (applyGate)
        {
            ledgers.Clear();
            fenceRefusalStreaks.Clear();
            ledgerWatermark = HLCTimestamp.Zero;
        }
    }

    /// <summary>
    /// Whether <paramref name="intent"/>'s leg (its transaction identity on its key) settled on
    /// <paramref name="partitionId"/>'s log within the settled-identity window — the verdict that rejects a
    /// re-driven prepare of a decided transaction. Judged only against the slice of the log the prepare arrives on: a node's other slices are
    /// node-local (a replica hosting a different partition set holds different ones), and this verdict changes
    /// the replicated transition, so it may depend on nothing but this partition's own applied prefix.
    /// Retention is logical, measured from the slice's watermark exactly as the committed heads' is, so the
    /// timing of the physical prune is unobservable. Caller holds <see cref="applyGate"/>.
    /// </summary>
    private bool IsSettledIdentity(int partitionId, PreparedIntent intent) =>
        ledgers.TryGetValue(partitionId, out PartitionLedger? ledger)
        && ledger.Settled.TryGetValue(new SettledIdentity(intent.TransactionId, intent.Epoch, intent.Key), out HLCTimestamp settledAt)
        && IsSettledWithinRetention(settledAt, ledger.Watermark);

    private static bool IsSettledWithinRetention(HLCTimestamp settledAt, HLCTimestamp watermark) =>
        settledAt.L >= watermark.L - SettledIdentityRetentionMs;

    /// <summary>Remembers a removed resolved intent's transaction identity in the slice of the partition whose
    /// log delivered the removal. The intent's commit timestamp is the entry's time base; a replayed removal
    /// records the same value. Stamps the partition dirty: the memory is persisted with the slice. Caller holds
    /// <see cref="applyGate"/>.</summary>
    private void RecordSettledIdentity(PreparedIntent intent, int partitionId)
    {
        PartitionLedger ledger = ledgers.GetOrAdd(partitionId, static _ => new PartitionLedger());

        SettledIdentity identity = new(intent.TransactionId, intent.Epoch, intent.Key);
        if (ledger.Settled.TryAdd(identity, intent.CommitTimestamp))
            ledger.Bytes += SettledIdentityBytes(intent.Key);
        else
            ledger.Settled[identity] = intent.CommitTimestamp;

        StampPartitionDirty(partitionId);

        // Physical prune once per bucket of watermark time. Unobservable to the verdict (logical retention).
        long bucket = ledger.Watermark.L / SettledIdentityPruneBucketMs;
        if (bucket != ledger.SettledPrunedBucket)
        {
            ledger.SettledPrunedBucket = bucket;
            PruneSettledIdentities(ledger);
        }
    }

    /// <summary>Physically drops settled identities that fell outside the window measured from the slice's
    /// watermark — only what the window already treats as absent. Caller holds <see cref="applyGate"/>.</summary>
    private void PruneSettledIdentities(PartitionLedger ledger)
    {
        List<SettledIdentity>? expired = null;
        foreach (KeyValuePair<SettledIdentity, HLCTimestamp> entry in ledger.Settled)
        {
            if (!IsSettledWithinRetention(entry.Value, ledger.Watermark))
                (expired ??= []).Add(entry.Key);
        }

        if (expired is null)
            return;

        foreach (SettledIdentity identity in expired)
        {
            if (ledger.Settled.Remove(identity))
                ledger.Bytes -= SettledIdentityBytes(identity.Key);
        }
    }

    /// <summary>Records a committed intent's mutation as its key's committed head in the slice of the
    /// partition whose log delivered the settlement, and advances that slice's watermark. Monotonic per key:
    /// an older revision never overwrites a newer one, so a replayed settlement is a no-op. Stamps the
    /// partition dirty for the checkpoint guard, because the slice is persisted with the partition's intent
    /// snapshot. Returns whether the head moved. Caller holds <see cref="applyGate"/>.</summary>
    private bool RecordCommittedHead(PreparedIntent intent, int partitionId)
    {
        PartitionLedger ledger = ledgers.GetOrAdd(partitionId, static _ => new PartitionLedger());

        bool existed = ledger.Heads.TryGetValue(intent.Key, out CommittedHead current);
        if (existed && current.Revision >= intent.Revision)
            return false;

        ledger.Heads[intent.Key] = new(intent.Revision, intent.State, intent.CommitTimestamp);
        if (!existed)
            ledger.Bytes += LedgerEntryBytes(intent.Key);

        // The head moved: whatever refusal streak the key held was measured against the old pair.
        fenceRefusalStreaks.TryRemove(intent.Key, out _);

        if (intent.CommitTimestamp > ledger.Watermark)
            ledger.Watermark = intent.CommitTimestamp;

        if (intent.CommitTimestamp > ledgerWatermark)
            ledgerWatermark = intent.CommitTimestamp;

        StampPartitionDirty(partitionId);

        // Physical prune once per retention bucket of commit time. Its timing is unobservable to any verdict
        // (the gate judges by logical retention; see PartitionLedger), so it need not — and does not — line
        // up with a log position.
        long bucket = ledger.Watermark.L / LedgerPruneBucketMs;
        if (bucket != ledger.PrunedBucket)
        {
            ledger.PrunedBucket = bucket;
            PruneLedger(ledger);
        }

        return true;
    }

    /// <summary>Hands a durable transaction's committed write to the watches on the partition whose log settled
    /// it. The intent carries the value: a by-reference materialization never clears it. No gate is held.</summary>
    private void PublishCommittedWrite(PreparedIntent intent, int partitionId, long logIndex)
    {
        Watch.KeyValueWatchBroker? broker = watchBroker;
        if (broker is null || !broker.HasSubscribers(partitionId))
            return;

        broker.Publish(
            partitionId,
            intent.State == KeyValueState.Deleted ? KeyValueWatchEventKind.Deleted : KeyValueWatchEventKind.Set,
            intent.Key,
            intent.Value,
            intent.Revision,
            intent.Expires,
            intent.CommitTimestamp,
            intent.TransactionId,
            logIndex,
            intent.Origin);
    }

    /// <summary>Marks one partition dirty for the checkpoint guard without consulting the routing resolver —
    /// the ledger slice belongs to the partition whose log fed it, whatever the key currently routes to. An
    /// unattributed slice (no log partition) dirties every partition; over-dirty is safe.</summary>
    private void StampPartitionDirty(int partitionId)
    {
        long tick = Interlocked.Increment(ref version);

        if (partitionId == UnattributedPartition)
        {
            StampMax(ref allPartitionsVersion, tick);
            return;
        }

        partitionVersion.AddOrUpdate(partitionId, static (_, t) => t, static (_, prev, t) => Math.Max(prev, t), tick);
    }

    /// <summary>Physically drops a slice's entries that fell outside its retention window. Removes only what
    /// the window already treats as absent, so a verdict never depends on whether this ran. Bumps the prune
    /// generation so a concurrent snapshot capture can detect the tear. Caller holds <see cref="applyGate"/>.</summary>
    private void PruneLedger(PartitionLedger ledger)
    {
        long cutoff = ledger.Watermark.L - stagedBaseFenceRetentionMs;

        List<string>? expired = null;
        foreach (KeyValuePair<string, CommittedHead> entry in ledger.Heads)
        {
            if (entry.Value.CommittedAt.L < cutoff)
                (expired ??= []).Add(entry.Key);
        }

        if (expired is null)
            return;

        foreach (string key in expired)
        {
            if (ledger.Heads.TryRemove(key, out _))
                ledger.Bytes -= LedgerEntryBytes(key);
        }

        ledger.PruneGeneration++;
    }

    /// <summary>The settled identity entries a ledger section or snapshot carries for one slice.</summary>
    internal readonly record struct SettledIdentityEntry(HLCTimestamp TransactionId, long Epoch, string Key, HLCTimestamp SettledAt);

    /// <summary>Replaces one partition's ledger slice with an installed one (whole-partition state transfer):
    /// the installed slice is the exporter's state at the snapshot boundary, which the entries that follow the
    /// boundary then advance exactly as they did on the exporter. Under the apply gate so an apply cannot
    /// interleave with the swap.</summary>
    private void InstallLedger(
        int partitionId, IReadOnlyList<(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)> heads,
        HLCTimestamp watermark, long reflectedThroughIndex, IReadOnlyList<SettledIdentityEntry>? settled, long fullyReflectedThroughIndex)
    {
        PartitionLedger ledger = BuildLedger(heads, watermark, reflectedThroughIndex, settled, fullyReflectedThroughIndex);

        lock (applyGate)
            InstallLedgerLocked(partitionId, ledger);
    }

    /// <summary>Materializes an installed slice outside the gate (the heads and the settled identities are
    /// copied in before the swap). The fully reflected position is clamped to the reflected one.</summary>
    private PartitionLedger BuildLedger(
        IReadOnlyList<(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)> heads,
        HLCTimestamp watermark, long reflectedThroughIndex, IReadOnlyList<SettledIdentityEntry>? settled, long fullyReflectedThroughIndex)
    {
        long reflected = Math.Max(0, reflectedThroughIndex);
        PartitionLedger ledger = new()
        {
            Watermark = watermark,
            PrunedBucket = watermark.L / LedgerPruneBucketMs,
            SettledPrunedBucket = watermark.L / SettledIdentityPruneBucketMs,
            ReflectedThroughIndex = reflected,
            FullyReflectedThroughIndex = Math.Clamp(fullyReflectedThroughIndex, 0, reflected)
        };

        foreach ((string key, long revision, KeyValueState state, HLCTimestamp committedAt) in heads)
        {
            if (ledger.Heads.TryAdd(key, new CommittedHead(revision, state, committedAt)))
                ledger.Bytes += LedgerEntryBytes(key);
        }

        if (settled is not null)
        {
            foreach (SettledIdentityEntry entry in settled)
            {
                if (ledger.Settled.TryAdd(new SettledIdentity(entry.TransactionId, entry.Epoch, entry.Key), entry.SettledAt))
                    ledger.Bytes += SettledIdentityBytes(entry.Key);
            }
        }

        return ledger;
    }

    /// <summary>Swaps a built slice in. The reflected position never regresses across the swap: a re-delivered
    /// or older section cannot re-open the history window a newer install closed. Caller holds <see cref="applyGate"/>.</summary>
    private void InstallLedgerLocked(int partitionId, PartitionLedger ledger)
    {
        if (ledgers.TryGetValue(partitionId, out PartitionLedger? previous))
        {
            if (previous.ReflectedThroughIndex > ledger.ReflectedThroughIndex)
                ledger.ReflectedThroughIndex = previous.ReflectedThroughIndex;
            if (previous.FullyReflectedThroughIndex > ledger.FullyReflectedThroughIndex)
                ledger.FullyReflectedThroughIndex = previous.FullyReflectedThroughIndex;
        }

        ledgers[partitionId] = ledger;

        if (ledger.Watermark > ledgerWatermark)
            ledgerWatermark = ledger.Watermark;

        StampPartitionDirty(partitionId);
    }

    /// <summary>
    /// Whether the entry at <paramref name="logIndex"/> on <paramref name="partitionId"/>'s log is history the
    /// partition's installed state already reflects: the slice was installed from a whole-partition snapshot
    /// whose exporter had applied through at least that index when it wrote the section.
    ///
    /// <para>A whole-partition snapshot is newer than the WAL boundary it is installed at — the exporter walks
    /// its state well after the boundary (the boundary is its compaction floor, minutes of traffic behind), and
    /// the receiver replays every retained entry above the boundary on top of the installed state. That replay
    /// runs the live apply path against a ledger that is already ahead of it. The replicated transitions are
    /// unaffected (an already-reflected prepare or settle folds idempotently), but every ADVISORY reading of the
    /// ledger at those positions is a false alarm: the staged-base fence "proves" a base moved that moved only
    /// after the prepare; the stale-base veto drives aborts for transactions long since committed, each counted
    /// as a late veto — the loss witness; and the below-head materialization witness fires for every replayed
    /// record. This predicate is what those readers consult to stay silent on history. Lock-free; a per-apply
    /// probe.</para>
    /// </summary>
    internal bool IsHistoricalApply(int partitionId, long logIndex) =>
        logIndex > 0
        && ledgers.TryGetValue(partitionId, out PartitionLedger? ledger)
        && logIndex <= Volatile.Read(ref ledger.ReflectedThroughIndex);

    /// <summary>The reflected position of a partition's installed slice (0 when none). Test seam.</summary>
    internal long GetLedgerReflectedThroughIndex(int partitionId) =>
        ledgers.TryGetValue(partitionId, out PartitionLedger? ledger) ? Volatile.Read(ref ledger.ReflectedThroughIndex) : 0;

    /// <summary>Reports where the entry at <paramref name="logIndex"/> on <paramref name="partitionId"/>'s log sits
    /// relative to the partition's installed snapshot. An unindexed log (the pure/in-memory test configuration)
    /// is never history.</summary>
    private HistoryWindow WindowOf(int partitionId, long logIndex)
    {
        if (logIndex <= 0 || !ledgers.TryGetValue(partitionId, out PartitionLedger? ledger))
            return HistoryWindow.Live;

        if (logIndex <= Volatile.Read(ref ledger.FullyReflectedThroughIndex))
            return HistoryWindow.FullyReflected;

        return logIndex <= Volatile.Read(ref ledger.ReflectedThroughIndex) ? HistoryWindow.Reflected : HistoryWindow.Live;
    }

    /// <summary>Records <paramref name="logIndex"/> as applied through this store on <paramref name="partitionId"/>'s
    /// log. Called AFTER the entry's commands are in the map, never before: the position is what an exporter or a
    /// checkpoint reads before walking the intents to certify "every intent prepared at or below this position is
    /// in the walk", and a position published while its own entry was still mid-apply would let the walk miss
    /// that entry's intent — which a replay at or below the fully reflected position then never re-installs.</summary>
    private void MarkApplied(int partitionId, long logIndex)
    {
        if (logIndex <= 0)
            return;

        appliedLogIndexByPartition.AddOrUpdate(
            partitionId, static (_, index) => index, static (_, current, index) => index > current ? index : current, logIndex);
    }

    /// <summary>
    /// Whether the entry at <paramref name="logIndex"/> on <paramref name="partitionId"/>'s log is at or below the
    /// installed slice's fully reflected position — the exporter's applied position when its intent walk began,
    /// so the installed intent set is exact for the entry (see <see cref="IsHistoricalApply"/> for the wider
    /// history window). A prepare replayed there that finds no intent installs nothing.
    /// </summary>
    internal bool IsFullyReflectedApply(int partitionId, long logIndex) =>
        logIndex > 0
        && ledgers.TryGetValue(partitionId, out PartitionLedger? ledger)
        && logIndex <= Volatile.Read(ref ledger.FullyReflectedThroughIndex);

    /// <summary>The fully reflected position of a partition's installed slice (0 when none). Test seam.</summary>
    internal long GetLedgerFullyReflectedThroughIndex(int partitionId) =>
        ledgers.TryGetValue(partitionId, out PartitionLedger? ledger) ? Volatile.Read(ref ledger.FullyReflectedThroughIndex) : 0;

    /// <summary>The highest log index applied through this store for the partition (0 when none).</summary>
    private long AppliedLogIndexOf(int partitionId) =>
        appliedLogIndexByPartition.TryGetValue(partitionId, out long index) ? index : 0;

    /// <summary>Drops one partition's ledger slice — the un-host purge: when this node stops replicating the
    /// partition its slice is dead retention (a re-gain installs the current one with the seeding snapshot).</summary>
    internal void PurgePartitionLedger(int partitionId)
    {
        lock (applyGate)
        {
            if (ledgers.TryRemove(partitionId, out _))
                StampPartitionDirty(partitionId);
        }
    }

    /// <summary>
    /// Captures one slice consistently for persistence: the watermark is read under the gate, the heads are
    /// scanned lock-free, and the scan is repeated if a physical prune ran in between — a prune under a newer
    /// watermark may drop a head the captured watermark still retains, and a snapshot that paired the two would
    /// judge differently on reload than a replica that never restarted. Heads recorded after the watermark read
    /// are harmless in the capture: the log entries that recorded them are replayed on top of it. Null when the
    /// partition has no slice.
    /// </summary>
    private LedgerCapture? CaptureLedger(int partitionId)
    {
        if (!ledgers.TryGetValue(partitionId, out PartitionLedger? ledger))
            return null;

        for (int attempt = 0; ; attempt++)
        {
            HLCTimestamp watermark;
            long generation;
            long reflectedThroughIndex;
            long fullyReflectedThroughIndex;
            List<SettledIdentityEntry> settled;
            lock (applyGate)
            {
                watermark = ledger.Watermark;
                generation = ledger.PruneGeneration;
                reflectedThroughIndex = ledger.ReflectedThroughIndex;
                fullyReflectedThroughIndex = ledger.FullyReflectedThroughIndex;
                // The settled memory is mutated and read under the gate only, so it is copied here: the copy
                // pairs with the watermark read in the same critical section and cannot tear.
                settled = CopySettledLocked(ledger);
            }

            List<KeyValuePair<string, CommittedHead>> heads = new(ledger.Heads.Count);
            foreach (KeyValuePair<string, CommittedHead> entry in ledger.Heads)
                heads.Add(entry);

            lock (applyGate)
            {
                if (generation == ledger.PruneGeneration)
                    return new LedgerCapture(watermark, reflectedThroughIndex, fullyReflectedThroughIndex, heads, settled);

                // A prune tore the capture. Prunes are rare (once per retention bucket), so a rescan almost
                // always succeeds; past a few attempts take the gate for the scan itself.
                if (attempt >= 3)
                {
                    heads.Clear();
                    foreach (KeyValuePair<string, CommittedHead> entry in ledger.Heads)
                        heads.Add(entry);
                    return new LedgerCapture(ledger.Watermark, ledger.ReflectedThroughIndex, ledger.FullyReflectedThroughIndex, heads, CopySettledLocked(ledger));
                }
            }
        }
    }

    /// <summary>Copies a slice's settled-identity memory. Caller holds <see cref="applyGate"/>.</summary>
    private static List<SettledIdentityEntry> CopySettledLocked(PartitionLedger ledger)
    {
        List<SettledIdentityEntry> settled = new(ledger.Settled.Count);
        foreach (KeyValuePair<SettledIdentity, HLCTimestamp> entry in ledger.Settled)
            settled.Add(new SettledIdentityEntry(entry.Key.TransactionId, entry.Key.Epoch, entry.Key.Key, entry.Value));
        return settled;
    }

    /// <summary>One slice as captured for persistence or transfer: its watermark, its reflected position, its
    /// heads (consistent with respect to physical pruning, see <see cref="CaptureLedger"/>) and its settled
    /// identities (copied under the gate with the watermark).</summary>
    private readonly record struct LedgerCapture(
        HLCTimestamp Watermark, long ReflectedThroughIndex, long FullyReflectedThroughIndex,
        List<KeyValuePair<string, CommittedHead>> Heads, List<SettledIdentityEntry> Settled);

}
