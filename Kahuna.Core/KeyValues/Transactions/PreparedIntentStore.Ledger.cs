using System.Collections.Concurrent;
using System.Diagnostics;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
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
            long count = 0;
            foreach (PartitionLedger ledger in ledgers.Values)
                count += Volatile.Read(ref ledger.HeadCount);
            return (int)Math.Min(count, int.MaxValue);
        }
    }

    /// <summary>Number of keys the committed-head ledger slice of <paramref name="partitionId"/> holds; 0 when the
    /// node holds no slice for it. Feeds the per-partition apply fingerprint, where two replicas at the same
    /// applied log id must report the same count.</summary>
    internal int CommittedHeadCountForPartition(int partitionId) =>
        ledgers.TryGetValue(partitionId, out PartitionLedger? ledger) ? (int)Math.Min(Volatile.Read(ref ledger.HeadCount), int.MaxValue) : 0;

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

    /// <summary>Per-partition sizes of the committed-head ledger, for the tagged gauges: the heads, the
    /// approximate bytes of the heads and the settled identities together, and the settled identities the slice
    /// holds. The unattributed slice is reported under <see cref="UnattributedPartition"/>.</summary>
    internal IReadOnlyList<(int PartitionId, long Entries, long Bytes, long SettledIdentities)> SnapshotLedgerSizes()
    {
        List<(int, long, long, long)> sizes = new(ledgers.Count);
        foreach (KeyValuePair<int, PartitionLedger> slice in ledgers)
            sizes.Add((slice.Key, Volatile.Read(ref slice.Value.HeadCount), Volatile.Read(ref slice.Value.Bytes), slice.Value.Settled.Count));
        return sizes;
    }

    /// <summary>Settled identities held across every ledger slice, swept or not. Observability only.</summary>
    internal long SettledIdentityCount
    {
        get
        {
            long count = 0;
            foreach (PartitionLedger ledger in ledgers.Values)
                count += ledger.Settled.Count;
            return count;
        }
    }

    /// <summary>The longest time the last ledger capture held the apply gate in one step. Test and diagnostic
    /// seam; the <c>kahuna.durable_tx.ledger_capture_gate_hold</c> histogram records the same value.</summary>
    internal TimeSpan LastLedgerCaptureLongestGateHold =>
        Stopwatch.GetElapsedTime(0, Volatile.Read(ref lastLedgerCaptureLongestGateHoldTicks));

    private long lastLedgerCaptureLongestGateHoldTicks;

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

    /// <summary>Every settled identity a partition's ledger slice retains inside its window, sorted by
    /// transaction id, epoch, then key. An identity outside the window is left out whether or not the sweep
    /// removed it, so the result is a function of the applied log prefix. Test seam for cross-replica parity
    /// assertions.</summary>
    internal IReadOnlyList<SettledIdentityEntry> SnapshotSettledIdentities(int partitionId)
    {
        List<SettledIdentityEntry> entries = [];

        if (ledgers.TryGetValue(partitionId, out PartitionLedger? ledger))
        {
            lock (applyGate)
            {
                List<SettledIdentityEntry> chunk = new(SettledCaptureChunkEntries);
                long retainedFromL = ledger.Watermark.L - SettledIdentityRetentionMs;
                long position = 0;
                bool more = true;

                while (more)
                {
                    more = ledger.Settled.CopyNext(ref position, chunk, retainedFromL);
                    entries.AddRange(chunk);
                    chunk.Clear();
                }
            }
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
    internal int LiveIntentCount => Count;

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
            return Count;

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
    ///
    /// <para>With <paramref name="probes"/> (one per key, the fields the fence judges — the ask of a current
    /// caller) a key whose prepare this node has not applied is judged from the committed-head memory alone
    /// instead of answering NotApplied. That verdict is sound: heads only advance and the key's head is frozen
    /// while the transaction's own intent is live, so the memory now is at or past the memory at the prepare's
    /// apply position, and a refusal here is one the apply-time fence gives too. The one head that is not a
    /// competitor's — the transaction's own settlement, already applied here — answers Clear through the
    /// settled-identity memory. So a replica that lags the prepare's apply attests anyway, and the commit no
    /// longer proceeds unattested for apply lag; only an unreachable replica stays silent.</para>
    /// </summary>
    internal KeyValueStagedBaseVerdictEntry[] EvaluateReplicaFenceVerdicts(
        HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys, IReadOnlyList<KeyValueStagedBaseProbe>? probes = null)
    {
        KeyValueStagedBaseVerdictEntry[] verdicts = new KeyValueStagedBaseVerdictEntry[keys.Count];
        bool probed = probes is not null && probes.Count == keys.Count;

        lock (applyGate)
        {
            for (int i = 0; i < keys.Count; i++)
            {
                if (!intents.TryGetValue(keys[i], out PreparedIntent? intent)
                    || intent.TransactionId != transactionId
                    || intent.Epoch != epoch)
                {
                    verdicts[i] = probed
                        ? JudgeFromMemory(transactionId, epoch, keys[i], probes![i])
                        : new(KeyValueStagedBaseVerdict.NotApplied, TryFindHead(keys[i], out CommittedHead head) ? head.Revision : -1);
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

    /// <summary>The memory-only verdict for a prepare this node has not applied (see
    /// <see cref="EvaluateReplicaFenceVerdicts"/>). Caller holds <see cref="applyGate"/>.</summary>
    private KeyValueStagedBaseVerdictEntry JudgeFromMemory(HLCTimestamp transactionId, long epoch, string key, in KeyValueStagedBaseProbe probe)
    {
        DurableTransactionMetrics.ReplicaFenceMemoryVerdicts.Add(1);

        // The transaction's own leg already settled through one of this node's logs: the head the memory would
        // judge against is (or follows) its own commit, and the record owns the outcome from there.
        if (IsSettledIdentityOnAnySlice(transactionId, epoch, key))
            return new(KeyValueStagedBaseVerdict.Clear, -1);

        bool hasValidatedBase = probe.BaseRevision != PreparedIntent.UnknownBaseRevision;

        if (!hasValidatedBase && probe.NoRevision)
            return new(KeyValueStagedBaseVerdict.Clear, -1);

        string? conflict = JudgeStagedBase(
            key, transactionId, hasValidatedBase, probe.BaseRevision, (KeyValueState)probe.BaseState, probe.NoRevision, probe.Revision,
            countAbsentHeadAdmission: false, out long headRevision, out _);

        return new(conflict is null ? KeyValueStagedBaseVerdict.Clear : KeyValueStagedBaseVerdict.StaleBase, headRevision);
    }

    /// <summary>Whether the leg (<paramref name="transactionId"/>, <paramref name="epoch"/>, <paramref name="key"/>)
    /// settled through any log this node applies, within that slice's settled-identity window. The replica verdict
    /// read consults it across slices because, unlike the apply-path rejection, it changes no replicated
    /// transition — it only keeps a node's own settlement from reading as a competitor's. Caller holds
    /// <see cref="applyGate"/>.</summary>
    private bool IsSettledIdentityOnAnySlice(HLCTimestamp transactionId, long epoch, string key)
    {
        SettledIdentity identity = new(transactionId, epoch, key);

        foreach (KeyValuePair<int, PartitionLedger> slice in ledgers)
        {
            if (slice.Value.Settled.TryGetValue(identity, out HLCTimestamp settledAt)
                && IsSettledWithinRetention(settledAt, slice.Value.Watermark))
                return true;
        }

        return false;
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
        HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys, int waitMs, CancellationToken cancellationToken,
        IReadOnlyList<KeyValueStagedBaseProbe>? probes = null)
    {
        long deadline = Environment.TickCount64 + waitMs;

        // With probes every key is attested at once (from the applied intent or from memory), so the wait below
        // is only ever entered for an ask without them.
        KeyValueStagedBaseVerdictEntry[] verdicts = EvaluateReplicaFenceVerdicts(transactionId, epoch, keys, probes);
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

                verdicts = EvaluateReplicaFenceVerdicts(transactionId, epoch, keys, probes);
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
            ledgerSizeFloor = 0;
        }
    }

    /// <summary>
    /// Whether <paramref name="intent"/>'s leg (its transaction identity on its key) settled on
    /// <paramref name="partitionId"/>'s log within the settled-identity window: the first half of the verdict
    /// that rejects a re-driven prepare of a decided transaction, the slice of the log the prepare arrives on.
    /// <see cref="IsSettledIdentityOnThisNode"/> adds the node's other slices for the leg a range move split
    /// across two logs. Retention is logical, measured from the slice's watermark exactly as the committed
    /// heads' is, so the timing of the physical prune is unobservable. Caller holds <see cref="applyGate"/>.
    /// </summary>
    private bool IsSettledIdentity(int partitionId, PreparedIntent intent) =>
        ledgers.TryGetValue(partitionId, out PartitionLedger? ledger)
        && ledger.Settled.TryGetValue(new SettledIdentity(intent.TransactionId, intent.Epoch, intent.Key), out HLCTimestamp settledAt)
        && IsSettledWithinRetention(settledAt, ledger.Watermark);

    /// <summary>
    /// Whether <paramref name="intent"/>'s leg settled through any log this node applies, within that slice's
    /// settled-identity window: the slice of <paramref name="partitionId"/>, the log that carries the prepare
    /// being judged, is consulted first, then every other slice. <paramref name="settledOnPartition"/> names the
    /// slice that answered. The other slices matter for a key that moved between its prepare and its settle: the
    /// prepare sits in the source partition's log and the settle in the destination's, and a node that applies
    /// the destination's stream ahead of the source's would otherwise re-install the settled leg as a phantom
    /// holder (see the settled-identity replay in <c>Apply</c>). Caller holds <see cref="applyGate"/>.
    /// </summary>
    private bool IsSettledIdentityOnThisNode(int partitionId, PreparedIntent intent, out int settledOnPartition)
    {
        settledOnPartition = partitionId;

        if (IsSettledIdentity(partitionId, intent))
            return true;

        SettledIdentity identity = new(intent.TransactionId, intent.Epoch, intent.Key);

        foreach (KeyValuePair<int, PartitionLedger> slice in ledgers)
        {
            if (slice.Key == partitionId)
                continue;

            if (slice.Value.Settled.TryGetValue(identity, out HLCTimestamp settledAt)
                && IsSettledWithinRetention(settledAt, slice.Value.Watermark))
            {
                settledOnPartition = slice.Key;
                return true;
            }
        }

        return false;
    }

    private static bool IsSettledWithinRetention(HLCTimestamp settledAt, HLCTimestamp watermark) =>
        settledAt.L >= watermark.L - SettledIdentityRetentionMs;

    /// <summary>Remembers a removed resolved intent's transaction identity in the slice of the partition whose
    /// log delivered the removal. The intent's commit timestamp is the entry's time base; a replayed removal
    /// records the same value. Stamps the partition dirty: the memory is persisted with the slice. Caller holds
    /// <see cref="applyGate"/>.</summary>
    private void RecordSettledIdentity(PreparedIntent intent, int partitionId) =>
        RecordSettledIdentity(intent.TransactionId, intent.Epoch, intent.Key, intent.CommitTimestamp, partitionId, overwrite: true);

    /// <summary>The field form of <see cref="RecordSettledIdentity(PreparedIntent, int)"/>, for a settle that
    /// found no intent to take the identity from. <paramref name="settledAt"/> is the time base of the
    /// identity's retention; with <paramref name="overwrite"/> false an identity remembered inside the window
    /// keeps the time base it has. An identity remembered outside the window is absent, and the record stores
    /// the new time base as it does for an identity the sweep already removed. Caller holds
    /// <see cref="applyGate"/>.</summary>
    private void RecordSettledIdentity(HLCTimestamp transactionId, long epoch, string key, HLCTimestamp settledAt, int partitionId, bool overwrite)
    {
        PartitionLedger ledger = ledgers.GetOrAdd(partitionId, static _ => new PartitionLedger());

        long retainedFromL = ledger.Watermark.L - SettledIdentityRetentionMs;

        if (ledger.Settled.Record(new SettledIdentity(transactionId, epoch, key), settledAt, overwrite, retainedFromL))
            ledger.Bytes += SettledIdentityBytes(key);

        StampPartitionDirty(partitionId);

        SweepSettledIdentities(ledger);
    }

    /// <summary>
    /// Runs one step of the physical sweep of a slice's settled memory. A sweep starts when the slice's
    /// watermark enters a new bucket of <see cref="SettledIdentityPruneBucketMs"/>, and each record that follows
    /// sweeps one shard until every shard was visited. One pass over a million identities is then spread over
    /// the next thousands of records, where one walk of the whole memory held the gate for all of it. The sweep
    /// removes only what the window already treats as absent, so its timing is unobservable to any verdict. It
    /// waits while a capture reads the memory. Caller holds <see cref="applyGate"/>.
    /// </summary>
    private static void SweepSettledIdentities(PartitionLedger ledger)
    {
        if (ledger.SettledCaptures > 0)
            return;

        long bucket = ledger.Watermark.L / SettledIdentityPruneBucketMs;
        if (bucket != ledger.SettledPrunedBucket)
        {
            ledger.SettledPrunedBucket = bucket;
            ledger.Settled.BeginSweep();
        }

        if (!ledger.Settled.SweepInProgress)
            return;

        ledger.Settled.SweepStep(ledger.Watermark.L - SettledIdentityRetentionMs, out int removed, out long removedKeyChars);

        if (removed > 0)
            ledger.Bytes -= SettledIdentityBytes(removedKeyChars, removed);
    }

    /// <summary>Records a committed intent's mutation as its key's committed head in the slice of the
    /// partition whose log delivered the settlement, and advances that slice's watermark. Monotonic per key:
    /// an older revision never overwrites a newer one, so a replayed settlement is a no-op. Stamps the
    /// partition dirty for the checkpoint guard, because the slice is persisted with the partition's intent
    /// snapshot. Returns whether the head moved. Caller holds <see cref="applyGate"/>.</summary>
    private bool RecordCommittedHead(PreparedIntent intent, int partitionId) =>
        RecordCommittedHead(intent.Key, intent.Revision, intent.State, intent.CommitTimestamp, partitionId);

    /// <summary>Applies a range move's seed of one key's committed head into the slice of the partition whose log
    /// carries it (see <see cref="SeedCommittedHeadCommand"/>). The same monotonic record a settlement makes, so
    /// a seed below a head the slice already holds — a re-driven hand-off, a replay over an installed slice, or a
    /// commit on the destination that settled before the seed replayed — changes nothing.</summary>
    private PreparedIntentApplyResult ApplySeededHead(SeedCommittedHeadCommand seed, int partitionId)
    {
        bool advanced;

        lock (applyGate)
            advanced = RecordCommittedHead(seed.Key, seed.Revision, seed.State, seed.CommittedAt, partitionId);

        return new PreparedIntentApplyResult(
            advanced ? TransactionApplyOutcome.Applied : TransactionApplyOutcome.IdempotentNoop, null, null);
    }

    /// <summary>
    /// The committed heads this node remembers for every key in <c>[startKey, endKey)</c>, across every ledger
    /// slice — for a key several slices remember (a range that moved before), the highest revision, exactly what
    /// the advisory fence judges by. The hand-off set a range move seeds into its destination slice. Lock-free,
    /// like the per-operation probes over the same maps; a head recorded during the walk is at worst missing from
    /// it, and the move's settle barrier and quiesce leave nothing to settle on the moving range while it runs.
    /// Unordered; the gather pages it by key.
    /// </summary>
    internal IReadOnlyList<CommittedHeadLedgerEntry> SnapshotCommittedHeadsRange(string? startKey, string? endKey)
    {
        Dictionary<string, CommittedHeadLedgerEntry> heads = [];

        foreach (KeyValuePair<int, PartitionLedger> slice in ledgers)
        {
            foreach (KeyValuePair<string, CommittedHead> entry in slice.Value.Heads)
            {
                if (startKey is not null && string.CompareOrdinal(entry.Key, startKey) < 0)
                    continue;

                if (endKey is not null && string.CompareOrdinal(entry.Key, endKey) >= 0)
                    continue;

                if (heads.TryGetValue(entry.Key, out CommittedHeadLedgerEntry known) && known.Revision >= entry.Value.Revision)
                    continue;

                heads[entry.Key] = new(entry.Key, entry.Value.Revision, entry.Value.State, entry.Value.CommittedAt);
            }
        }

        return [.. heads.Values];
    }

    /// <summary>Records one key's committed head in <paramref name="partitionId"/>'s slice: the settlement path
    /// and the range-move seed share it. Monotonic per key; see the intent overload for the contract. Caller
    /// holds <see cref="applyGate"/>.</summary>
    private bool RecordCommittedHead(string key, long revision, KeyValueState state, HLCTimestamp committedAt, int partitionId)
    {
        PartitionLedger ledger = ledgers.GetOrAdd(partitionId, static _ => new PartitionLedger());

        bool existed = ledger.Heads.TryGetValue(key, out CommittedHead current);
        if (existed && current.Revision >= revision)
            return false;

        // A new head below the slice's horizon is absent to every verdict, and the staleness gates refuse the
        // transactions it could matter to, so it is not stored. A restart replays log entries from below its
        // checkpoint; stored, the heads the prune had dropped would come back and count against the budget on
        // this replica alone. The answer is "moved": the slice held no head at or above this revision, and a
        // live commit that lands here must still reach its watches.
        if (!existed && committedAt.L < Math.Max(ledger.Watermark.L - stagedBaseFenceRetentionMs, ledger.SizeFloor))
            return true;

        ledger.Heads[key] = new(revision, state, committedAt);
        if (existed)
            RemoveFromHeadBucket(ledger, current.CommittedAt);
        else
        {
            ledger.Bytes += LedgerEntryBytes(key);
            ledger.HeadCount++;
        }

        AddToHeadBucket(ledger, committedAt);

        // The head moved: whatever refusal streak the key held was measured against the old pair.
        fenceRefusalStreaks.TryRemove(key, out _);

        if (committedAt > ledger.Watermark)
            ledger.Watermark = committedAt;

        if (committedAt > ledgerWatermark)
            ledgerWatermark = committedAt;

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

        if (ledgerMaxEntries > 0 && ledger.HeadCount > ledgerMaxEntries)
            EnforceLedgerBound(ledger, partitionId);

        return true;
    }

    private static void AddToHeadBucket(PartitionLedger ledger, HLCTimestamp committedAt)
    {
        long bucket = committedAt.L / LedgerSizeBucketMs;
        ledger.HeadsPerBucket[bucket] = ledger.HeadsPerBucket.TryGetValue(bucket, out int count) ? count + 1 : 1;
    }

    private static void RemoveFromHeadBucket(PartitionLedger ledger, HLCTimestamp committedAt)
    {
        long bucket = committedAt.L / LedgerSizeBucketMs;
        if (!ledger.HeadsPerBucket.TryGetValue(bucket, out int count))
            return;

        if (count <= 1)
            ledger.HeadsPerBucket.Remove(bucket);
        else
            ledger.HeadsPerBucket[bucket] = count - 1;
    }

    /// <summary>
    /// Brings a slice that passed its head budget back inside it: raises the slice's size floor to the lowest
    /// second of commit time that leaves at most three quarters of the budget above it, then drops the heads
    /// below the floor. The retention horizon alone bounds the slice by the write rate times the horizon, and a
    /// sustained load of new keys fills the heap long before the horizon releases its first head.
    ///
    /// <para>A raised floor is the retention horizon moved nearer: a head below it is absent, and the staleness
    /// gates refuse a transaction that began before it, so absence is never read as "no commit happened". The
    /// floor is computed from the slice's heads and watermark only, which are a function of the applied log
    /// prefix, so replicas with the same budget raise it at the same entry to the same value. The quarter of
    /// slack makes the scan of the heads run once per quarter budget of new heads, not once per record.</para>
    ///
    /// <para>The floor stays <see cref="LedgerSizeBoundMinRetentionMs"/> behind the watermark. A load that puts
    /// more than the budget inside that span keeps the slice over its budget, and the node warns.</para>
    /// Caller holds <see cref="applyGate"/>.
    /// </summary>
    private void EnforceLedgerBound(PartitionLedger ledger, int partitionId)
    {
        long floorBucket = ledger.SizeFloor / LedgerSizeBucketMs;
        long highestFloorBucket = (ledger.Watermark.L - LedgerSizeBoundMinRetentionMs) / LedgerSizeBucketMs;

        if (highestFloorBucket <= floorBucket)
        {
            WarnLedgerOverBudget(ledger, partitionId);
            return;
        }

        long target = ledgerMaxEntries - ledgerMaxEntries / 4;

        long[] buckets = new long[ledger.HeadsPerBucket.Count];
        ledger.HeadsPerBucket.Keys.CopyTo(buckets, 0);
        Array.Sort(buckets);

        // Newest second first: the floor is the second after the first one that does not fit.
        long newFloorBucket = floorBucket;
        long kept = 0;

        for (int i = buckets.Length - 1; i >= 0; i--)
        {
            kept += ledger.HeadsPerBucket[buckets[i]];
            if (kept > target)
            {
                newFloorBucket = buckets[i] + 1;
                break;
            }
        }

        if (newFloorBucket > highestFloorBucket)
            newFloorBucket = highestFloorBucket;

        // A floor at or below the oldest head drops nothing and only refuses transactions.
        if (newFloorBucket <= floorBucket || buckets[0] >= newFloorBucket)
        {
            WarnLedgerOverBudget(ledger, partitionId);
            return;
        }

        long before = ledger.HeadCount;

        ledger.SizeFloor = newFloorBucket * LedgerSizeBucketMs;
        if (ledger.SizeFloor > ledgerSizeFloor)
            ledgerSizeFloor = ledger.SizeFloor;

        PruneLedger(ledger);

        // A capture that read the old floor must not pair it with heads scanned after the raise.
        ledger.PruneGeneration++;

        DurableTransactionMetrics.CommittedHeadLedgerSizeFloorRaises.Add(1);

        long nowTicks = Environment.TickCount64;
        if (logger is not null && nowTicks - Volatile.Read(ref lastLedgerBoundLogTicks) >= LedgerBoundLogIntervalMs)
        {
            Volatile.Write(ref lastLedgerBoundLogTicks, nowTicks);

            logger.LogWarning(
                "Committed-head ledger of partition {PartitionId} passed its budget of {MaxEntries} heads ({Before} held): the horizon moved to {HorizonMs} ms behind the newest commit and {Dropped} heads left. A transaction that began before the new horizon is refused at its prepare or commit until the write rate falls",
                partitionId, ledgerMaxEntries, before, ledger.Watermark.L - ledger.SizeFloor, before - ledger.HeadCount);
        }

        if (ledger.HeadCount > ledgerMaxEntries)
            WarnLedgerOverBudget(ledger, partitionId);
        else
            ledger.SizeWarnedAt = 0;
    }

    /// <summary>Logs that a slice stays over its budget because the minimum horizon holds its floor back: once
    /// when it first happens and again each time the slice doubles. Caller holds <see cref="applyGate"/>.</summary>
    private void WarnLedgerOverBudget(PartitionLedger ledger, int partitionId)
    {
        if (ledger.SizeWarnedAt != 0 && ledger.HeadCount < 2 * ledger.SizeWarnedAt)
            return;

        ledger.SizeWarnedAt = ledger.HeadCount;
        DurableTransactionMetrics.CommittedHeadLedgerOverBudget.Add(1);

        logger?.LogWarning(
            "Committed-head ledger of partition {PartitionId} holds {Entries} heads (about {Bytes} bytes), over its budget of {MaxEntries}, and its horizon is already at the minimum of {MinRetentionMs} ms: the write rate of distinct keys is above what the budget holds, and the slice grows until the rate falls",
            partitionId, ledger.HeadCount, Volatile.Read(ref ledger.Bytes), ledgerMaxEntries, LedgerSizeBoundMinRetentionMs);
    }

    /// <summary>The size floor of a partition's ledger slice in commit-timestamp milliseconds (0 when the size
    /// bound never raised it, or the node holds no slice). Test and observability seam.</summary>
    internal long GetLedgerSizeFloor(int partitionId) =>
        ledgers.TryGetValue(partitionId, out PartitionLedger? ledger) ? Volatile.Read(ref ledger.SizeFloor) : 0;

    /// <summary>Recomputes the node-wide size floor after a slice left. Caller holds <see cref="applyGate"/>.</summary>
    private void RecomputeLedgerSizeFloorLocked()
    {
        long floor = 0;
        foreach (KeyValuePair<int, PartitionLedger> slice in ledgers)
        {
            if (slice.Value.SizeFloor > floor)
                floor = slice.Value.SizeFloor;
        }

        ledgerSizeFloor = floor;
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

    /// <summary>Physically drops a slice's entries that fell outside its retention window or below its size floor. Removes only what
    /// the window already treats as absent, so a verdict never depends on whether this ran. Bumps the prune
    /// generation so a concurrent snapshot capture can detect the tear. Caller holds <see cref="applyGate"/>.</summary>
    private void PruneLedger(PartitionLedger ledger)
    {
        long cutoff = Math.Max(ledger.Watermark.L - stagedBaseFenceRetentionMs, ledger.SizeFloor);

        // Each expired head is removed as the walk meets it: the map permits a removal during an enumeration.
        // A list of the expired heads was one array of megabytes when a prune dropped tens of thousands of
        // them, allocated here under the gate.
        bool expired = false;

        foreach (KeyValuePair<string, CommittedHead> entry in ledger.Heads)
        {
            if (entry.Value.CommittedAt.L >= cutoff)
                continue;

            expired = true;

            if (!ledger.Heads.TryRemove(entry.Key, out _))
                continue;

            ledger.Bytes -= LedgerEntryBytes(entry.Key);
            ledger.HeadCount--;
            RemoveFromHeadBucket(ledger, entry.Value.CommittedAt);
        }

        if (expired)
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
        HLCTimestamp watermark, long reflectedThroughIndex, IReadOnlyList<SettledIdentityEntry>? settled, long fullyReflectedThroughIndex,
        long sizeFloor)
    {
        PartitionLedger ledger = BuildLedger(heads, watermark, reflectedThroughIndex, settled, fullyReflectedThroughIndex, sizeFloor);

        lock (applyGate)
            InstallLedgerLocked(partitionId, ledger);
    }

    /// <summary>Materializes an installed slice outside the gate (the heads and the settled identities are
    /// copied in before the swap). The fully reflected position is clamped to the reflected one.</summary>
    private PartitionLedger BuildLedger(
        IReadOnlyList<(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)> heads,
        HLCTimestamp watermark, long reflectedThroughIndex, IReadOnlyList<SettledIdentityEntry>? settled, long fullyReflectedThroughIndex,
        long sizeFloor)
    {
        PartitionLedger ledger = NewInstalledLedger(watermark, reflectedThroughIndex, fullyReflectedThroughIndex, sizeFloor);

        foreach ((string key, long revision, KeyValueState state, HLCTimestamp committedAt) in heads)
            AddInstalledHead(ledger, key, new CommittedHead(revision, state, committedAt));

        if (settled is not null)
        {
            foreach (SettledIdentityEntry entry in settled)
                AddInstalledSettled(ledger, entry);
        }

        return ledger;
    }

    /// <summary>An empty slice with the scalar state of an installed one. A reader that decodes a snapshot one
    /// entry at a time fills it with <see cref="AddInstalledHead"/> and <see cref="AddInstalledSettled"/>
    /// outside the gate, then swaps it in. The fully reflected position is clamped to the reflected one.</summary>
    private PartitionLedger NewInstalledLedger(HLCTimestamp watermark, long reflectedThroughIndex, long fullyReflectedThroughIndex, long sizeFloor)
    {
        PartitionLedger ledger = new();
        SetInstalledState(ledger, watermark, reflectedThroughIndex, fullyReflectedThroughIndex, sizeFloor);
        return ledger;
    }

    /// <summary>Gives a slice that is not installed yet the scalar state of an installed one. The prune buckets
    /// follow from the watermark, so a loaded slice prunes on the schedule of a live one.</summary>
    private void SetInstalledState(PartitionLedger ledger, HLCTimestamp watermark, long reflectedThroughIndex, long fullyReflectedThroughIndex, long sizeFloor)
    {
        long reflected = Math.Max(0, reflectedThroughIndex);

        ledger.Watermark = watermark;
        ledger.PrunedBucket = watermark.L / LedgerPruneBucketMs;
        ledger.SettledPrunedBucket = watermark.L / SettledIdentityPruneBucketMs;
        ledger.ReflectedThroughIndex = reflected;
        ledger.FullyReflectedThroughIndex = Math.Clamp(fullyReflectedThroughIndex, 0, reflected);
        ledger.SizeFloor = Math.Max(0, sizeFloor);
    }

    /// <summary>Adds one head to a slice that is not installed yet. The first head of a key stays.</summary>
    private static void AddInstalledHead(PartitionLedger ledger, string key, CommittedHead head)
    {
        if (!ledger.Heads.TryAdd(key, head))
            return;

        ledger.Bytes += LedgerEntryBytes(key);
        ledger.HeadCount++;
        AddToHeadBucket(ledger, head.CommittedAt);
    }

    /// <summary>Adds one settled identity to a slice that is not installed yet. An identity outside the window of
    /// the slice's watermark is absent to every verdict, and the watermark only advances, so it is left out.</summary>
    private static void AddInstalledSettled(PartitionLedger ledger, in SettledIdentityEntry entry)
    {
        long retainedFromL = ledger.Watermark.L - SettledIdentityRetentionMs;

        if (entry.SettledAt.L < retainedFromL)
            return;

        if (ledger.Settled.Record(new SettledIdentity(entry.TransactionId, entry.Epoch, entry.Key), entry.SettledAt, replace: false, retainedFromL))
            ledger.Bytes += SettledIdentityBytes(entry.Key);
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

        // Recomputed, not raised: the slice this one replaces may be the one that held the node-wide floor.
        RecomputeLedgerSizeFloorLocked();

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
            {
                RecomputeLedgerSizeFloorLocked();
                StampPartitionDirty(partitionId);
            }
        }
    }

    /// <summary>
    /// Captures one slice consistently for persistence. Null when the partition has no slice.
    ///
    /// <para><b>Settled identities.</b> The watermark is read under the gate, and the settled memory is then
    /// read one chunk at a time, each chunk under the gate, and handed to <paramref name="settledSink"/> with the
    /// gate released. The capture holds no copy of the memory: a slice with a million identities costs one
    /// buffer of <see cref="SettledCaptureChunkEntries"/> entries, and an apply waits for one chunk. The sweep
    /// of the memory waits while the capture runs (<see cref="PartitionLedger.SettledCaptures"/>), so every
    /// identity the captured watermark retains reaches the sink. An identity recorded after the watermark read
    /// reaches it or not: the log entry that recorded it is replayed on top of the capture.</para>
    ///
    /// <para><b>Heads.</b> The heads are scanned lock-free after the settled memory, and the scan is repeated if
    /// a physical prune or a raise of the size floor ran in between: a prune under a newer watermark or floor may
    /// drop a head the captured watermark still retains, and a snapshot that paired the two would judge
    /// differently on reload than a replica that never restarted. Heads recorded after the watermark read are
    /// harmless in the capture: the log entries that recorded them are replayed on top of it. The scan comes
    /// after the settled memory so that the head of every captured identity is in it.</para>
    /// </summary>
    /// <param name="partitionId">The partition whose slice is captured.</param>
    /// <param name="settledSink">Receives each chunk of settled identities, outside the gate. The list is
    /// reused for the next chunk, so the sink must not keep it.</param>
    private LedgerCapture? CaptureLedger(int partitionId, Action<List<SettledIdentityEntry>> settledSink)
    {
        if (!ledgers.TryGetValue(partitionId, out PartitionLedger? ledger))
            return null;

        HLCTimestamp watermark;
        long generation;
        long reflectedThroughIndex;
        long fullyReflectedThroughIndex;
        long sizeFloor;

        lock (applyGate)
        {
            watermark = ledger.Watermark;
            generation = ledger.PruneGeneration;
            reflectedThroughIndex = ledger.ReflectedThroughIndex;
            fullyReflectedThroughIndex = ledger.FullyReflectedThroughIndex;
            sizeFloor = ledger.SizeFloor;
            ledger.SettledCaptures++;
        }

        long longestHoldTicks = 0;

        try
        {
            CopySettled(ledger, watermark, settledSink, ref longestHoldTicks);

            BetweenCaptureReadsForTesting?.Invoke();

            ChunkedList<KeyValuePair<string, CommittedHead>> heads = new();

            for (int attempt = 0; ; attempt++)
            {
                foreach (KeyValuePair<string, CommittedHead> entry in ledger.Heads)
                    heads.Add(entry);

                lock (applyGate)
                {
                    long entered = Stopwatch.GetTimestamp();

                    try
                    {
                        if (generation == ledger.PruneGeneration)
                            return new LedgerCapture(watermark, reflectedThroughIndex, fullyReflectedThroughIndex, heads, sizeFloor);

                        // A prune tore the capture. The values the next scan pairs with are read again here.
                        // The settled identities already written stay valid: they are the ones an older
                        // watermark retains, a superset of what the newer one does.
                        watermark = ledger.Watermark;
                        generation = ledger.PruneGeneration;
                        reflectedThroughIndex = ledger.ReflectedThroughIndex;
                        fullyReflectedThroughIndex = ledger.FullyReflectedThroughIndex;
                        sizeFloor = ledger.SizeFloor;
                        heads.Clear();

                        // Prunes are rare (once per retention bucket), so a rescan almost always succeeds; past
                        // a few attempts take the gate for the scan itself.
                        if (attempt >= 3)
                        {
                            foreach (KeyValuePair<string, CommittedHead> entry in ledger.Heads)
                                heads.Add(entry);

                            return new LedgerCapture(watermark, reflectedThroughIndex, fullyReflectedThroughIndex, heads, sizeFloor);
                        }
                    }
                    finally
                    {
                        longestHoldTicks = Math.Max(longestHoldTicks, Stopwatch.GetTimestamp() - entered);
                    }
                }
            }
        }
        finally
        {
            lock (applyGate)
                ledger.SettledCaptures--;

            Volatile.Write(ref lastLedgerCaptureLongestGateHoldTicks, longestHoldTicks);
            DurableTransactionMetrics.LedgerCaptureGateHold.Record(Stopwatch.GetElapsedTime(0, longestHoldTicks).TotalMilliseconds);
        }
    }

    /// <summary>Test seam, invoked after a capture read the settled identities and before it reads the heads.
    /// Never set in production.</summary>
    internal Action? BetweenCaptureReadsForTesting { get; set; }

    /// <summary>Hands every settled identity that <paramref name="watermark"/> retains to
    /// <paramref name="sink"/>, one chunk at a time. Takes the gate for each chunk and releases it for the
    /// sink. The caller holds the slice's <see cref="PartitionLedger.SettledCaptures"/> count above zero.</summary>
    private void CopySettled(PartitionLedger ledger, HLCTimestamp watermark, Action<List<SettledIdentityEntry>> sink, ref long longestHoldTicks)
    {
        List<SettledIdentityEntry> chunk = new(SettledCaptureChunkEntries);
        long retainedFromL = watermark.L - SettledIdentityRetentionMs;
        long position = 0;
        bool more = true;

        while (more)
        {
            lock (applyGate)
            {
                long entered = Stopwatch.GetTimestamp();
                more = ledger.Settled.CopyNext(ref position, chunk, retainedFromL);
                longestHoldTicks = Math.Max(longestHoldTicks, Stopwatch.GetTimestamp() - entered);
            }

            if (chunk.Count == 0)
                continue;

            sink(chunk);
            chunk.Clear();
        }
    }

    /// <summary>One slice as captured for persistence or transfer: its watermark, its reflected positions, its
    /// size floor and its heads (consistent with respect to physical pruning, see <see cref="CaptureLedger"/>).
    /// The settled identities are not held here: the capture hands them to its sink as it reads them.</summary>
    private sealed class LedgerCapture(
        HLCTimestamp watermark, long reflectedThroughIndex, long fullyReflectedThroughIndex,
        ChunkedList<KeyValuePair<string, CommittedHead>> heads, long sizeFloor)
    {
        public HLCTimestamp Watermark { get; } = watermark;

        public long ReflectedThroughIndex { get; } = reflectedThroughIndex;

        public long FullyReflectedThroughIndex { get; } = fullyReflectedThroughIndex;

        public long SizeFloor { get; } = sizeFloor;

        /// <summary>The heads the scan read. A key in <see cref="ReplacedHeads"/> is written from there.</summary>
        public ChunkedList<KeyValuePair<string, CommittedHead>> Heads { get; } = heads;

        /// <summary>Heads that replace the scanned head of their key: the keys whose transaction settled
        /// between the capture and the intent walk (see <c>AddHeadsSettledDuringCapture</c>). Null when none.</summary>
        public Dictionary<string, CommittedHead>? ReplacedHeads { get; set; }

        public bool IsReplaced(string key) => ReplacedHeads is not null && ReplacedHeads.ContainsKey(key);
    }

}
