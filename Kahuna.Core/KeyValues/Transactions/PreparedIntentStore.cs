
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

/// <summary>
/// The partition-scoped authority for durable prepared intents. Holds at most one live intent per logical key and
/// routes every prepare/resolve/remove transition through the deterministic
/// <see cref="PreparedIntentStateMachine"/>, so leader apply, follower apply, WAL replay, and state-transfer
/// replay converge. It is the source of truth for a key's pending mutation after its prepare commits — the
/// actor-local write intent is only a cache.
///
/// <para>Mutation happens on the single per-partition apply path (Raft apply / restore); reads (visibility
/// lookups and the recovery sweep) may run concurrently, which the concurrent map makes safe. This type owns no
/// replication or persistence; that plumbing serializes/dispatches these same transitions.</para>
/// </summary>
internal sealed partial class PreparedIntentStore
{
    /// <summary>The live intents by key. Written only through <see cref="InstallLive"/> and <see cref="RemoveLive"/>,
    /// which keep <see cref="orderedKeys"/> in step; point lookups read it lock-free.</summary>
    private readonly ConcurrentDictionary<string, PreparedIntent> intents = new();

    /// <summary>
    /// The live intents' keys in ordinal order, beside the map, so a scan window
    /// (<see cref="SnapshotScanWindow"/>, <see cref="SnapshotPrefix"/>) is a descent to the window's first key plus
    /// the keys inside it, instead of a walk of every live intent. The walk cost every scan page O(live intents)
    /// for a window that is almost always empty, and a concurrent map never shrinks its bucket table, so a store
    /// that once held a bulk load's intents walked all of its empty buckets per page long after they settled.
    /// Guarded by <see cref="orderedKeysGate"/>: a window read holds it for the descent and the window, a mutation
    /// for one tree update. Count is the live intent count without the map's per-bucket lock sweep.
    /// </summary>
    private readonly SortedSet<string> orderedKeys = new(StringComparer.Ordinal);

    private readonly object orderedKeysGate = new();

    /// <summary>Installs or replaces the live intent at <paramref name="key"/>: the map first, so a key the index
    /// names has its intent, then the index. Safe under concurrent writers (the apply gate serializes most, the
    /// snapshot import runs outside it); a reader between the two steps misses an intent whose apply has not
    /// completed, which is the state before the apply. <paramref name="partitionId"/> is the partition whose log
    /// (or snapshot) installed the intent. Returns true when the install resolved the last pending intent of its
    /// transaction on that partition (tracked only while the commit admission window is on).</summary>
    private bool InstallLive(string key, PreparedIntent intent, int partitionId)
    {
        intents[key] = intent;

        lock (orderedKeysGate)
            orderedKeys.Add(key);

        return trackPendingCommits && TrackPendingCommit(key, intent, partitionId);
    }

    /// <summary>Removes the live intent at <paramref name="key"/> from the map and the index; false when the map held
    /// none. A reader between the two steps finds the key in the index and no intent in the map, and skips it.</summary>
    private bool RemoveLive(string key, out PreparedIntent? removed)
    {
        bool found = intents.TryRemove(key, out removed);

        lock (orderedKeysGate)
            orderedKeys.Remove(key);

        if (trackPendingCommits)
            TrackPendingCommit(key, null, UnattributedPartition);

        return found;
    }

    // ── pending commit timestamps, per partition ────────────────────────────────────
    //
    // The commit admission watermark of a partition is capped by the lowest commit timestamp among the intents still
    // pending there: such an intent is already in the log, and its event comes when it settles, possibly long after
    // later entries. Kept only while the commit admission window is on (see ConfigureCommitAdmission), so the intent
    // apply path pays nothing for it otherwise. Each pending intent is counted under the partition whose log (or
    // snapshot) installed it, never under a routing guess.

    private sealed class PendingCommitOrder : IComparer<(HLCTimestamp Commit, string Key)>
    {
        public static readonly PendingCommitOrder Instance = new();

        public int Compare((HLCTimestamp Commit, string Key) x, (HLCTimestamp Commit, string Key) y)
        {
            int byCommit = x.Commit.CompareTo(y.Commit);
            return byCommit != 0 ? byCommit : string.CompareOrdinal(x.Key, y.Key);
        }
    }

    private volatile bool trackPendingCommits;

    private readonly object pendingCommitsGate = new();

    // Guarded by pendingCommitsGate: the partition and commit timestamp each pending key is counted under, and each
    // partition's pending commit timestamps in order.
    private readonly Dictionary<string, (int PartitionId, HLCTimestamp Commit)> pendingCommitByKey = new(StringComparer.Ordinal);

    private readonly Dictionary<int, SortedSet<(HLCTimestamp Commit, string Key)>> pendingCommitsByPartition = new();

    // Guarded by pendingCommitsGate: how many intents of each transaction attempt are still pending on each
    // partition. A commit that takes the count to zero is the transaction's last event on the partition.
    private readonly Dictionary<(HLCTimestamp TransactionId, long Epoch, int PartitionId), int> pendingCountByTransaction = new();

    // The pending intent each key is counted under, for the per-transaction count.
    private readonly Dictionary<string, (HLCTimestamp TransactionId, long Epoch)> pendingTransactionByKey = new(StringComparer.Ordinal);

    // The partition of each intent the on-disk snapshots loaded, kept from construction until the commit admission
    // window is configured, which runs after the load.
    private Dictionary<string, int>? loadedIntentPartitions;

    // Moves the key's entry in the index from its previous pending intent (if any) to the new one (if pending).
    // Returns true when that took the previous intent's transaction to zero pending intents on its partition.
    private bool TrackPendingCommit(string key, PreparedIntent? intent, int partitionId)
    {
        lock (pendingCommitsGate)
        {
            bool drained = false;

            if (pendingCommitByKey.Remove(key, out (int PartitionId, HLCTimestamp Commit) previous))
            {
                if (pendingCommitsByPartition.TryGetValue(previous.PartitionId, out SortedSet<(HLCTimestamp, string)>? previousSet))
                {
                    previousSet.Remove((previous.Commit, key));
                    if (previousSet.Count == 0)
                        pendingCommitsByPartition.Remove(previous.PartitionId);
                }

                if (pendingTransactionByKey.Remove(key, out (HLCTimestamp TransactionId, long Epoch) owner))
                {
                    (HLCTimestamp, long, int) counted = (owner.TransactionId, owner.Epoch, previous.PartitionId);
                    if (pendingCountByTransaction.TryGetValue(counted, out int count))
                    {
                        if (count <= 1)
                        {
                            pendingCountByTransaction.Remove(counted);
                            drained = true;
                        }
                        else
                            pendingCountByTransaction[counted] = count - 1;
                    }
                }
            }

            if (intent is not { IsPending: true })
                return drained;

            pendingCommitByKey[key] = (partitionId, intent.CommitTimestamp);
            pendingTransactionByKey[key] = (intent.TransactionId, intent.Epoch);

            (HLCTimestamp, long, int) transaction = (intent.TransactionId, intent.Epoch, partitionId);
            pendingCountByTransaction[transaction] = pendingCountByTransaction.GetValueOrDefault(transaction) + 1;

            if (!pendingCommitsByPartition.TryGetValue(partitionId, out SortedSet<(HLCTimestamp, string)>? set))
            {
                set = new SortedSet<(HLCTimestamp, string)>(PendingCommitOrder.Instance);
                pendingCommitsByPartition[partitionId] = set;
            }

            set.Add((intent.CommitTimestamp, key));
            return drained;
        }
    }

    /// <summary>
    /// Turns the per-partition index of pending commit timestamps on or off. On, it is rebuilt from the live
    /// intents: those the on-disk snapshots loaded are counted under the partition of the file they came from, any
    /// other under the partition the key routes to. Called once at manager construction, before any apply.
    /// </summary>
    public void ConfigureCommitAdmission(bool enabled)
    {
        lock (applyGate)
        {
            Dictionary<string, int>? loaded = loadedIntentPartitions;
            loadedIntentPartitions = null;

            lock (pendingCommitsGate)
            {
                pendingCommitByKey.Clear();
                pendingCommitsByPartition.Clear();
                pendingCountByTransaction.Clear();
                pendingTransactionByKey.Clear();
            }

            trackPendingCommits = enabled;
            if (!enabled)
                return;

            foreach (KeyValuePair<string, PreparedIntent> entry in intents)
            {
                if (!entry.Value.IsPending)
                    continue;

                int partitionId = loaded is not null && loaded.TryGetValue(entry.Key, out int filePartition)
                    ? filePartition
                    : resolvePartition is { } resolver && TryResolvePartition(resolver, entry.Key, out int routed) ? routed : UnattributedPartition;

                TrackPendingCommit(entry.Key, entry.Value, partitionId);
            }
        }
    }

    /// <summary>The lowest commit timestamp among the intents pending on <paramref name="partitionId"/>, or false
    /// when none is pending there or the index is off. Safe from any thread.</summary>
    public bool TryGetLowestPendingCommit(int partitionId, out HLCTimestamp lowest)
    {
        lock (pendingCommitsGate)
        {
            if (pendingCommitsByPartition.TryGetValue(partitionId, out SortedSet<(HLCTimestamp Commit, string Key)>? set) && set.Count > 0)
            {
                lowest = set.Min.Commit;
                return true;
            }
        }

        lowest = HLCTimestamp.Zero;
        return false;
    }

    /// <summary>The lowest commit timestamp among every pending intent on the node, for the oldest-pending-intent
    /// metric; false when none is pending or the index is off.</summary>
    public bool TryGetLowestPendingCommit(out HLCTimestamp lowest)
    {
        lowest = HLCTimestamp.Zero;
        bool found = false;

        lock (pendingCommitsGate)
        {
            foreach (SortedSet<(HLCTimestamp Commit, string Key)> set in pendingCommitsByPartition.Values)
            {
                if (set.Count == 0)
                    continue;

                HLCTimestamp candidate = set.Min.Commit;
                if (!found || candidate < lowest)
                {
                    lowest = candidate;
                    found = true;
                }
            }
        }

        return found;
    }

    /// <summary>One key's last committed durable-transaction write, as remembered by the committed-head ledger:
    /// the revision/state the committed head reached when that commit's settlement applied, and the commit
    /// timestamp the retention horizon is measured against.</summary>
    private readonly record struct CommittedHead(long Revision, KeyValueState State, HLCTimestamp CommittedAt);

    /// <summary>
    /// One data partition's slice of the committed-head ledger: for each key, the last durable-transaction
    /// commit whose settlement (or removal) applied THROUGH THIS PARTITION'S LOG, and the watermark — the
    /// highest commit timestamp among them — that bounds the slice's retention window. Fed only by replicated
    /// applies, on the same per-key ordered apply path as prepares, so at the moment a prepare or a bundled
    /// commit applies, any competitor commit of the same key that settled earlier in the log has already left
    /// its head here.
    ///
    /// <para>The slice is a pure function of the partition's applied log prefix: every replica of the
    /// partition that applied the same entries holds the same heads and the same watermark, whether it applied
    /// them live, replayed them from its WAL after a restart, or installed them from a partition snapshot.
    /// That determinism is what lets the one-phase bundled commit gate judge a stalled bundle's validated base
    /// and read-set against it at apply time and reach the same verdict everywhere. Keying by partition is
    /// what makes it hold: a node's set of hosted partitions is node-local, so one node-global map fed by every
    /// hosted partition would differ between replicas that host different partition sets.</para>
    ///
    /// <para>Retention is logical, not physical: an entry older than the retention horizon measured from the
    /// slice's own watermark is treated as absent by the gate whether or not it has been physically dropped.
    /// The watermark only advances, so an entry that fell outside the window never comes back into it, and the
    /// physical prune (at retention-bucket transitions of the watermark) is unobservable to any verdict. The
    /// staleness gate makes the window safe: a transaction that began before the horizon is refused rather
    /// than judged against a possibly-pruned head.</para>
    ///
    /// <para>Heads are mutated only under <see cref="applyGate"/>; the concurrent map lets the advisory reads
    /// (the two-phase fence's memory, the hydration fence, the convergence repair) probe it lock-free.</para>
    /// </summary>
    private sealed class PartitionLedger
    {
        public readonly ConcurrentDictionary<string, CommittedHead> Heads = new();

        // Settled-identity memory: every (TransactionId, Epoch, Key) whose resolved intent was REMOVED through
        // this partition's log, with the intent's commit timestamp as the time base of its retention. Fed and
        // read only under applyGate (a prepare apply consults it before the state machine runs), so a plain
        // map. See IsSettledIdentity for what it decides and why it must be part of the replicated slice.
        public readonly Dictionary<SettledIdentity, HLCTimestamp> Settled = new();

        // The settled-memory prune bucket (Watermark.L / SettledIdentityPruneBucketMs) the last prune ran in.
        public long SettledPrunedBucket;

        // Highest commit timestamp among the heads this slice ever recorded. Mutated under applyGate.
        public HLCTimestamp Watermark = HLCTimestamp.Zero;

        // The retention bucket (Watermark.L / bucket size) the last physical prune ran in; derived from the
        // watermark on load so a reloaded slice prunes on the same schedule as a live one.
        public long PrunedBucket;

        // Bumped by every physical prune, so a snapshot capture that raced a prune between its watermark read
        // and its head scan can detect the tear and rescan instead of persisting a slice that lacks heads the
        // captured watermark still retains.
        public long PruneGeneration;

        // Approximate retained bytes (keys plus entries), for the ledger gauge. Maintained under applyGate.
        public long Bytes;

        // The partition's highest applied log index this slice is known to reflect: set when the slice was
        // installed from a whole-partition snapshot (the exporter's applied position when it wrote the section)
        // and persisted with the slice, so it survives a restart mid catch-up. Entries at or below it are
        // history the installed state already accounts for — see IsHistoricalApply. Zero for a slice that was
        // never installed. Written under applyGate, read lock-free.
        public long ReflectedThroughIndex;

        // The partition's highest applied log index whose intents the installed set fully reflected: the
        // exporter's position when its intent walk began (see WritePartitionSection). A prepare replayed at
        // or below it that finds no intent installs nothing — the installed set is exact for such entries, up
        // to removals during the walk that replay on their own — so history can never re-create an intent the
        // exporter had rejected (a settled replay the exporter's memory refused but a memory captured later
        // no longer remembers). Never above ReflectedThroughIndex. Same lifecycle and access rules as it.
        public long FullyReflectedThroughIndex;
    }

    /// <summary>The slice key used when an apply cannot be attributed to a partition's log — the pure/in-memory
    /// configuration of the unit tests, where commands are applied directly rather than through
    /// <see cref="Replicate"/>/<see cref="Restore"/>. Production applies always carry their log's partition.</summary>
    internal const int UnattributedPartition = -1;

    /// <summary>Where one applied entry sits relative to the partition's installed snapshot (see
    /// <see cref="IsHistoricalApply"/> and <see cref="IsFullyReflectedApply"/>).</summary>
    private enum HistoryWindow
    {
        /// <summary>Above the installed position: live catch-up or steady state; every verdict is given.</summary>
        Live,

        /// <summary>At or below the installed position but above the fully reflected one — inside the
        /// exporter's walk: the transition applies (an intent the walk missed must install), no advisory
        /// verdict is given.</summary>
        Reflected,

        /// <summary>At or below the fully reflected position: the installed intent set is exact for the entry,
        /// so a prepare that finds no intent installs nothing, and no advisory verdict is given.</summary>
        FullyReflected
    }

    // The committed-head ledger, one slice per partition whose log this node applies. See PartitionLedger.
    private readonly ConcurrentDictionary<int, PartitionLedger> ledgers = new();

    // Highest commit timestamp across every slice — the ADVISORY fence's notion of "now" for its staleness
    // gate (the two-phase acknowledgement path judges by key across every slice, so it measures against the
    // node-wide watermark). Always an HLC from replicated commits, never a local clock. Guarded by applyGate.
    private HLCTimestamp ledgerWatermark = HLCTimestamp.Zero;

    // Partitions whose on-disk snapshot predates the ledger section. Their slices start empty on load, which
    // is exactly what an older build would hold, but a build that enables the apply-time bundled-commit gate
    // would then judge differently from replicas that persisted the ledger — so enabling the gate over such a
    // snapshot fails closed (see ConfigureOnePhaseApplyTimeValidation).
    private readonly List<int> ledgerMissingPartitions = [];

    // Highest log index applied through this store per partition (ApplyLog / ApplyDeltaAckPrepares). Read by
    // the whole-partition export after its intent walk: every entry at or below it was applied here before the
    // section was written, so a replica installing that section may treat the replay of those entries as
    // history rather than live catch-up (see IsHistoricalApply).
    private readonly ConcurrentDictionary<int, long> appliedLogIndexByPartition = new();

    /// <summary>Default staged-base fence retention (ms). Must comfortably exceed the longest possible
    /// transaction lifetime (begin → prepare), because the staleness gate refuses to acknowledge a
    /// validated-base prepare from a transaction older than this horizon — its base may predate every
    /// retained head. Ten minutes covers the decision-deadline ceiling and typical reaper horizons.</summary>
    internal const int DefaultStagedBaseFenceRetentionMs = 600_000;

    private int stagedBaseFenceRetentionMs = DefaultStagedBaseFenceRetentionMs;

    // Physical prunes run when the slice's watermark crosses into a new bucket of this many milliseconds:
    // one scan per quarter retention per partition, instead of one per apply.
    private int LedgerPruneBucketMs => Math.Max(1, stagedBaseFenceRetentionMs / 4);

    /// <summary>Approximate bytes one ledger entry retains: the key's characters plus the entry struct and
    /// the dictionary slot around it.</summary>
    private static long LedgerEntryBytes(string key) => 2L * key.Length + 64;

    /// <summary>One leg — a transaction attempt's identity on one key — as the settled-identity memory keys it.
    /// Per key, not per transaction: a decided multi-key transaction can have one leg settled here while a
    /// range move hands another of its legs, still pending, to this partition; that leg must install, and only
    /// a prepare of a key that itself settled here is a replay.</summary>
    private readonly record struct SettledIdentity(HLCTimestamp TransactionId, long Epoch, string Key);

    /// <summary>
    /// How long (in commit-timestamp time, measured from the slice's watermark) the settled-identity memory
    /// keeps rejecting a re-driven prepare of a settled transaction. The duplicate this guards against is a
    /// proposal the finalizer re-drove after a leader step-down or an unobserved apply while its first copy was
    /// already in the successor's log: the copies land seconds apart at most (the re-drive is immediate and
    /// the successor's queue is bounded by the finalize deadline), so a minute covers it with a wide margin,
    /// while keeping the memory at one small entry per settled transaction of the last minute rather than the
    /// staged-base fence's ten-minute horizon. A fixed constant, not a setting: the verdict is a replicated
    /// transition, so every replica must judge the same entry by the same window whatever its configuration.
    /// An identity older than the window is forgotten and its duplicate degrades to the pre-existing behaviour
    /// (a re-installed intent the recovery sweep settles again), never to anything worse.
    /// </summary>
    internal const int SettledIdentityRetentionMs = 60_000;

    // Physical prunes of the settled memory run when the slice's watermark crosses into a new bucket of this
    // many milliseconds — one scan per quarter window per partition.
    private const int SettledIdentityPruneBucketMs = SettledIdentityRetentionMs / 4;

    /// <summary>Approximate bytes one settled-identity entry retains: the key's characters (the string is the
    /// removed intent's, kept alive by the entry) plus the identity, the timestamp and the dictionary slot.</summary>
    private static long SettledIdentityBytes(string key) => 2L * key.Length + 80;

    // Monotonic tick source for the dirty stamps below: each mutation mints one tick, so stamps taken
    // from it order mutations against the pre-scan capture in <see cref="PersistSnapshot"/>.
    private long version;

    // Per-partition dirty stamp: the tick of the last mutation whose key routed to that partition. A
    // partition's checkpoint snapshot is skipped when neither its stamp, nor <see cref="allPartitionsVersion"/>,
    // nor the routing stamp moved since its last durable write — the file already holds exactly this content —
    // which turns the common quiet checkpoint from a full scan + serialize + rewrite into a counter comparison.
    // With the previous single global stamp, one intent change anywhere re-dirtied every partition, so a busy
    // checkpoint rescanned the whole set once per partition.
    private readonly ConcurrentDictionary<int, long> partitionVersion = new();

    // Tick of the last mutation that could not be attributed to one partition: no resolver attached yet
    // (load-time merges), or a bulk sweep that touched many keys. Dirties every partition; over-dirty is safe.
    private long allPartitionsVersion;

    // Stamps captured just before that partition's last successful snapshot write: the mutation tick it
    // covered and the routing stamp it routed with. Captured before the scan, so a mutation or a routing
    // change racing the scan leaves a stamp ahead and the next checkpoint rewrites the file. The routing
    // stamp is what keeps a skip from hiding an intent that silently moved partitions when the range map
    // changed: until every partition has re-persisted under the new routing, none of them compares equal.
    private readonly ConcurrentDictionary<int, PersistedStamp> persistedVersion = new();

    private readonly string? snapshotDirectory;

    private readonly string? snapshotPrefix;

    private readonly ILogger<IKahuna>? logger;

    private readonly object fileLock = new();

    // Serializes the read-decide-write of a single transition so the compare-and-set is atomic. The concurrent map
    // makes each dictionary operation safe, but not the read-then-write pair: Raft-ordered apply, follower apply,
    // the recovery sweep, and producer-side apply can all call Apply concurrently, and two interleaved transitions
    // on the same key would otherwise both observe the pre-state and clobber each other.
    private readonly object applyGate = new();

    // The open commit observations (see BeginCommitObservation). Copy-on-write under commitObservationGate, so the
    // install path reads it with no lock.
    private volatile PreparedIntentCommitObservation[] commitObservations = [];

    private readonly object commitObservationGate = new();

    // Resolves an intent's key to its current data partition, so a per-partition snapshot/transfer only covers
    // the intents this partition owns. Null in the pure/in-memory configuration used by unit tests.
    private Func<string, int>? resolvePartition;

    /// <summary>In-memory configuration (unit tests): no persistence.</summary>
    public PreparedIntentStore() { }

    /// <summary>Durable configuration: a per-partition on-disk snapshot under <paramref name="storagePath"/> is
    /// loaded on construction and rewritten by <see cref="PersistSnapshot"/> before the WAL checkpoint discards
    /// the log tail; a parse failure fails closed.</summary>
    public PreparedIntentStore(string? storagePath, string? storageRevision, ILogger<IKahuna>? logger)
    {
        this.logger = logger;

        if (!string.IsNullOrEmpty(storagePath))
        {
            snapshotDirectory = storagePath;
            snapshotPrefix = $"preparedintent_{storageRevision}";
            LoadFromDisk();
        }
    }

    /// <summary>Wires the key → data-partition resolver once the locator exists (manager construction).
    /// <paramref name="routingVersion"/> reports a stamp that changes whenever the resolver's routing may have
    /// changed (the range-map version); null means the routing is fixed for the store's lifetime.</summary>
    /// <param name="probe">The same routing as <paramref name="resolver"/>, answering an unroutable key with
    /// <see langword="false"/> instead of an exception. Null (unit tests) leaves the store catching the
    /// resolver's exception where it tolerates an unroutable key.</param>
    public void AttachPartitionResolver(Func<string, int> resolver, Func<long>? routingVersion = null, Ranges.PartitionProbe? probe = null)
    {
        resolvePartition = resolver;
        probePartition = probe;
        this.routingVersion = routingVersion;
    }

    // The non-throwing form of resolvePartition, for the paths that tolerate an unroutable key and visit many
    // keys per call. Null when only the throwing resolver was attached.
    private Ranges.PartitionProbe? probePartition;

    // Resolves the partition owning <paramref name="key"/> for a path that tolerates an unroutable key: false
    // when the key cannot be routed on this node now (a restart replays data-partition entries before the meta
    // partition has rebuilt the range map). These paths resolve every key the store holds on each call, and
    // one of them is read repeatedly at every leader change, so an unroutable key must cost a lookup, not an
    // exception: a store holding a few thousand such intents otherwise threw that many times per count.
    private bool TryResolvePartition(Func<string, int> resolver, string key, out int partitionId)
    {
        Ranges.PartitionProbe? probe = probePartition;
        if (probe is not null)
            return probe(key, out partitionId);

        try
        {
            partitionId = resolver(key);
            return true;
        }
        catch
        {
            partitionId = -1;
            return false;
        }
    }

    // Monotonic stamp of the routing the resolver reads (RangeMapStore.MapVersion), or null when routing is
    // fixed for the store's lifetime (tests, memory-only configuration). Pulled by the checkpoint guard.
    private Func<long>? routingVersion;

    // Marks the partition owning <paramref name="key"/> dirty for the checkpoint guard. Mutators call this
    // after the dictionary write, so a stamp equal to a pre-scan capture implies the scan saw the mutation.
    // Without a resolver the mutation cannot be attributed, so every partition is marked instead.
    //
    // This runs on the replicated apply/restore path, so it must never throw: routing can legitimately fail
    // there (a restart replays data-partition entries before the meta partition has rebuilt the range map,
    // and the resolver throws on an uncovered key-range key). An unattributable mutation falls back to the
    // all-partitions stamp — over-dirty is safe, a failed apply is not.
    private void StampDirty(string key)
    {
        long tick = Interlocked.Increment(ref version);

        Func<string, int>? resolver = resolvePartition;
        if (resolver is not null && TryResolvePartition(resolver, key, out int owner))
        {
            partitionVersion.AddOrUpdate(owner, static (_, t) => t, static (_, prev, t) => Math.Max(prev, t), tick);
            return;
        }

        StampMax(ref allPartitionsVersion, tick);
    }

    // Marks every partition dirty: for bulk sweeps whose per-key attribution would cost more than the
    // over-inclusive rewrite it avoids.
    private void StampAllDirty() => StampMax(ref allPartitionsVersion, Interlocked.Increment(ref version));

    // Monotonic max-write: a stamp must never move backward, or two racing mutators could leave the stamp
    // equal to a checkpoint's pre-scan capture while the later mutation was missed by its scan.
    private static void StampMax(ref long location, long tick)
    {
        long observed = Interlocked.Read(ref location);
        while (tick > observed)
        {
            long prior = Interlocked.CompareExchange(ref location, tick, observed);
            if (prior == observed)
                return;
            observed = prior;
        }
    }

    // The stamps a partition's snapshot file was written under: the mutation tick it covered, the routing stamp
    // it routed with, and the applied position it certified as fully reflected. A checkpoint skips the rewrite
    // only when all three still match — a file whose position fell behind the store's would replay the entries
    // in between through the live path on the next restart (see PersistSnapshot).
    private readonly record struct PersistedStamp(long Version, long RoutingVersion, long AppliedThroughIndex);

    // Invoked (outside the apply gate) with the committed intent whenever a commit settlement (resolve or
    // removal of a committed intent) applies on this node — the convergence hook: the same apply position
    // that advances the fence's committed head lets the wiring verify the mutation actually materialized
    // here and repair the visible entry when it did not. Must not re-enter this store.
    private Action<PreparedIntent>? onCommittedSettleApplied;

    /// <summary>Wires the commit-settlement convergence hook (manager construction). The callback runs on the
    /// apply path outside the gate, once per applied commit resolution/removal, and must not re-enter the
    /// store.</summary>
    public void AttachCommittedSettleObserver(Action<PreparedIntent> observer) => onCommittedSettleApplied = observer;

    // The node's open key-value watches. A durable transaction's write is a watch event at exactly one point: the
    // live apply that takes its intent from pending to committed. Null publishes nothing.
    private Watch.KeyValueWatchBroker? watchBroker;

    /// <summary>Wires the watch broker (manager construction). Publishing runs on the apply path outside the gate.</summary>
    public void AttachWatchBroker(Watch.KeyValueWatchBroker broker) => watchBroker = broker;

    // ── settled intents awaiting the flush of their materialized row ──────────────────────────────
    //
    // A by-reference materialization record carries no value: a replica applies it from the intent the record
    // names. The settle that follows removes that intent from the live set, and the next snapshot rewrite drops
    // it from disk — while the committed row the record materialized may still be queued for the background
    // flush. The durability floor certifies the prepare delta through the snapshot channel, collectively, so a
    // restart's replay can start above the prepare and at or below the record. Such a replay would then find the
    // mutation nowhere on this node: not in the live set, not in the snapshot, not in the backend. Once the intent
    // is settled, the flushed row is the prepare's only durable artifact — so a settled committed intent whose row
    // is not yet durable is retained here and persisted with the partition's snapshot until the flush that carries
    // its row is confirmed.
    //
    // Retention is decided at the settle apply from the unflushed overlay, the same witness the settle-time
    // convergence check uses: an overlay head at or above the intent's revision means the row is queued but not
    // durable. Release is driven by the overlay's confirmed flushes of the key, each releasing the retained intents
    // whose row is at or below the flushed head and no other (see ReleaseSettledIntentsAwaitingFlush for why the
    // whole key must never be released at once). Both the probe-and-retain and the release run under settledGate,
    // so a flush confirmed between the probe and the retention still finds the entry when its release runs.
    // Entries loaded from a snapshot are re-checked once the partition's restart replay completes: those whose
    // row the replay did not re-queue were durable before the crash, because the replay window started above
    // their record.
    //
    // Never part of the live set: invisible to Get/GetByIdentity, the recovery sweep, admission accounting and
    // every fence. The only readers are the restart restorer and the replayed materializing resolve, by exact
    // identity (the restorer also checks the revision), and only after the live set and the replay history.

    private readonly Dictionary<string, List<PreparedIntent>> settledAwaitingFlush = new(StringComparer.Ordinal);

    private readonly object settledGate = new();

    // Whether this node holds a queued-but-unflushed row for (key, revision) — an overlay head at or above the
    // revision. Null in the pure/in-memory configuration, which then retains nothing.
    private Func<string, long, bool>? rowUnflushed;

    /// <summary>Wires the unflushed-row probe (manager construction). Without it no settled intent is retained.</summary>
    public void AttachUnflushedRowProbe(Func<string, long, bool> probe) => rowUnflushed = probe;

    /// <summary>Retained settled intents across every key (observability / tests).</summary>
    internal int SettledIntentsAwaitingFlushCount
    {
        get
        {
            lock (settledGate)
            {
                int count = 0;
                foreach (List<PreparedIntent> retained in settledAwaitingFlush.Values)
                    count += retained.Count;
                return count;
            }
        }
    }

    // Caller holds applyGate: this is the removal of a committed intent. The probe and the insertion share
    // settledGate with the release so the two cannot interleave into a leaked entry.
    private void RetainSettledIntentIfRowUnflushed(PreparedIntent intent)
    {
        Func<string, long, bool>? probe = rowUnflushed;
        if (probe is null)
            return;

        lock (settledGate)
        {
            if (!probe(intent.Key, intent.Revision))
                return;

            AddSettledAwaitingFlushLocked(intent);
        }
    }

    // Caller holds settledGate. One entry per (transaction, epoch) identity per key; a re-retention of the same
    // identity replaces its entry.
    private void AddSettledAwaitingFlushLocked(PreparedIntent intent)
    {
        if (!settledAwaitingFlush.TryGetValue(intent.Key, out List<PreparedIntent>? retained))
        {
            settledAwaitingFlush[intent.Key] = [intent];
            return;
        }

        for (int i = 0; i < retained.Count; i++)
        {
            PreparedIntent existing = retained[i];
            if (existing.TransactionId == intent.TransactionId && existing.Epoch == intent.Epoch)
            {
                retained[i] = intent;
                return;
            }
        }

        retained.Add(intent);
    }

    /// <summary>The retained settled intent of the given transaction attempt at <paramref name="key"/>, if this
    /// node still awaits the flush of its materialized row. The restart restorer's fallback for a by-reference
    /// record whose intent is no longer live; the caller checks the revision.</summary>
    public bool TryGetSettledIntentAwaitingFlush(HLCTimestamp transactionId, long epoch, string key, out PreparedIntent? intent)
    {
        intent = null;

        lock (settledGate)
        {
            if (settledAwaitingFlush.Count == 0 || !settledAwaitingFlush.TryGetValue(key, out List<PreparedIntent>? retained))
                return false;

            foreach (PreparedIntent candidate in retained)
            {
                if (candidate.TransactionId == transactionId && candidate.Epoch == epoch)
                {
                    intent = candidate;
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>
    /// Drops the retained settled intents of <paramref name="key"/> whose materialized row is at or below the
    /// head a confirmed flush just made durable — (<paramref name="flushedRevision"/>,
    /// <paramref name="flushedLastModified"/>), ordered exactly as the overlay orders heads: the writer persists a
    /// key's revisions in queue order, so the confirmed head proves every row at or below it durable and nothing
    /// above it. Invoked by the overlay on the flush path.
    ///
    /// <para>Never the whole key. A restart replay re-queues a key's materializations one record at a time,
    /// resolving each from its own retained intent, and the writer flushes the first row before the replay
    /// reaches the second record: a release keyed by the key alone dropped every retained intent of the key on
    /// that first flush, and every later record of the key in the replay window found nothing (CamusDB fault
    /// soak sn2, 2026-09-30: 82,536 records over 2,000 keys on one restart).</para>
    /// </summary>
    public void ReleaseSettledIntentsAwaitingFlush(string key, long flushedRevision, HLCTimestamp flushedLastModified)
    {
        lock (settledGate)
        {
            if (settledAwaitingFlush.Count == 0 || !settledAwaitingFlush.TryGetValue(key, out List<PreparedIntent>? retained))
                return;

            int released = 0;
            for (int i = retained.Count - 1; i >= 0; i--)
            {
                if (IsRowAtOrBelowFlushedHead(retained[i], flushedRevision, flushedLastModified))
                {
                    retained.RemoveAt(i);
                    released++;
                }
            }

            if (released == 0)
                return;

            if (retained.Count == 0)
                settledAwaitingFlush.Remove(key);
        }

        // The partition's snapshot must drop the entry on its next rewrite.
        StampDirty(key);
    }

    // A retained intent's row is (Revision, CommitTimestamp) — the materialization stamps last-modified with the
    // commit timestamp — and it is durable once a confirmed flush's head is at or beyond it in the overlay's
    // newest-head order (revision first, commit HLC as the same-revision tiebreak: a delete or an extend reuses
    // a revision number).
    private static bool IsRowAtOrBelowFlushedHead(PreparedIntent retained, long flushedRevision, HLCTimestamp flushedLastModified) =>
        retained.Revision < flushedRevision
        || (retained.Revision == flushedRevision && retained.CommitTimestamp <= flushedLastModified);

    /// <summary>
    /// Drops the retained settled intents of <paramref name="partitionId"/> whose row this node holds no unflushed
    /// head for. Runs when the partition's restart replay completes: an entry loaded from the snapshot whose
    /// by-reference record the replay redelivered has its row queued again (the restorer records it in the overlay
    /// before anything else) and stays; one the replay did not reach was durable before the crash — the replay
    /// window started above its record — and would otherwise linger in memory and in every later snapshot. Keys
    /// the resolver cannot attribute (a range-map gap) are kept: over-retention is safe, release is not.
    /// </summary>
    public int ReleaseSettledIntentsWithDurableRows(int partitionId)
    {
        Func<string, long, bool>? probe = rowUnflushed;
        Func<string, int>? resolver = resolvePartition;
        if (probe is null || resolver is null)
            return 0;

        int released = 0;
        List<string>? emptied = null;

        lock (settledGate)
        {
            if (settledAwaitingFlush.Count == 0)
                return 0;

            foreach (KeyValuePair<string, List<PreparedIntent>> kv in settledAwaitingFlush)
            {
                if (!TryResolvePartition(resolver, kv.Key, out int owner) || owner != partitionId)
                    continue;

                List<PreparedIntent> retained = kv.Value;
                for (int i = retained.Count - 1; i >= 0; i--)
                {
                    if (probe(retained[i].Key, retained[i].Revision))
                        continue;

                    retained.RemoveAt(i);
                    released++;
                }

                if (retained.Count == 0)
                    (emptied ??= []).Add(kv.Key);
            }

            if (emptied is not null)
                foreach (string key in emptied)
                    settledAwaitingFlush.Remove(key);
        }

        if (released > 0)
            StampPartitionDirty(partitionId);

        return released;
    }

    // Drops the retained settled intents whose key matches — the un-host purge and the whole-partition install
    // both replace what this node holds for those keys. Caller holds applyGate (the gate order is applyGate,
    // then settledGate, as in the settle apply).
    private bool PurgeSettledAwaitingFlushLocked(Func<string, bool> shouldRemove)
    {
        lock (settledGate)
        {
            if (settledAwaitingFlush.Count == 0)
                return false;

            List<string>? toRemove = null;
            foreach (string key in settledAwaitingFlush.Keys)
            {
                if (shouldRemove(key))
                    (toRemove ??= []).Add(key);
            }

            if (toRemove is null)
                return false;

            foreach (string key in toRemove)
                settledAwaitingFlush.Remove(key);

            return true;
        }
    }

    // The partition's retained settled intents, copied out under settledGate so the snapshot's file write never
    // runs under it. Uses the same resolver as the live-intent scan, which the caller has checked is attached.
    private List<PreparedIntent>? CollectSettledAwaitingFlush(int partitionId, Func<string, int> resolver)
    {
        lock (settledGate)
        {
            if (settledAwaitingFlush.Count == 0)
                return null;

            List<PreparedIntent>? collected = null;
            foreach (KeyValuePair<string, List<PreparedIntent>> kv in settledAwaitingFlush)
            {
                if (resolver(kv.Key) != partitionId)
                    continue;

                (collected ??= []).AddRange(kv.Value);
            }

            return collected;
        }
    }

    // ── replay history: the fully reflected window's prepares, kept for the records that name them ─────────
    //
    // A restart replays the partition's log from its application-durability floor, and the entries between
    // that floor and the position the node's own checkpoint certified are history the reloaded intent set already
    // reflects: a replayed prepare there that finds no live intent installs nothing (see Apply), which is what
    // keeps a prepare the live apply refused from becoming a phantom holder. But the window's data side is NOT
    // reflected anywhere on this node — the floor sits below it precisely because those rows never reached the
    // backend — and a commit in the window is materialized by a by-reference record (or a materializing
    // resolve) that carries no value and names the intent the replay just declined to install. The value would
    // then exist nowhere here. So the folded prepare is kept in this side table, by identity, for exactly that
    // reader: present for a materialization to resolve from, never a holder — invisible to Get/GetByIdentity, the
    // recovery sweep, admission accounting, the settled-identity memory and every fence. A replayed removal drops
    // the entry (the record precedes the settle in the log, so it has resolved by then); the partition's
    // restore-finished hook clears whatever is left (a prepare the live apply refused, whose commit was rejected
    // and never produced a record). Fed and read on the partition's replay thread only; the concurrent map is
    // for the counter and the clear, which run elsewhere.
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<(HLCTimestamp TransactionId, long Epoch, string Key), PreparedIntent>> replayHistory = new();

    private void RememberReplayHistoryIntent(int partitionId, PreparedIntent intent)
    {
        ConcurrentDictionary<(HLCTimestamp, long, string), PreparedIntent> slice =
            replayHistory.GetOrAdd(partitionId, static _ => new ConcurrentDictionary<(HLCTimestamp, long, string), PreparedIntent>());

        slice[(intent.TransactionId, intent.Epoch, intent.Key)] = intent;
    }

    private void ForgetReplayHistoryIntent(int partitionId, HLCTimestamp transactionId, long epoch, string key)
    {
        if (replayHistory.TryGetValue(partitionId, out ConcurrentDictionary<(HLCTimestamp, long, string), PreparedIntent>? slice))
            slice.TryRemove((transactionId, epoch, key), out _);
    }

    /// <summary>The prepare of the given transaction attempt at <paramref name="key"/> that the restart replay of
    /// <paramref name="partitionId"/> folded as fully reflected history, if its settle has not replayed yet. The
    /// restorer's source for a by-reference materialization record (and the materializing resolve's, on replay)
    /// whose prepare lies inside the history window; the caller checks the revision.</summary>
    internal bool TryGetReplayHistoryIntent(int partitionId, HLCTimestamp transactionId, long epoch, string key, out PreparedIntent? intent)
    {
        intent = null;

        return replayHistory.TryGetValue(partitionId, out ConcurrentDictionary<(HLCTimestamp, long, string), PreparedIntent>? slice)
            && slice.TryGetValue((transactionId, epoch, key), out intent);
    }

    /// <summary>Drops every prepare the restart replay of <paramref name="partitionId"/> kept as history, once
    /// the replay is complete; returns how many were left. Nothing above the history window is ever kept here,
    /// so what is left is a prepare whose transaction never materialized on this partition's log.</summary>
    internal int ClearReplayHistory(int partitionId) =>
        replayHistory.TryRemove(partitionId, out ConcurrentDictionary<(HLCTimestamp, long, string), PreparedIntent>? slice) ? slice.Count : 0;

    /// <summary>Prepares currently kept as replay history across every partition (observability / tests).</summary>
    internal int ReplayHistoryIntentCount
    {
        get
        {
            int count = 0;
            foreach (ConcurrentDictionary<(HLCTimestamp, long, string), PreparedIntent> slice in replayHistory.Values)
                count += slice.Count;
            return count;
        }
    }

    // Invoked (outside the apply gate) with the key and the frozen (validated base, committed head) revision
    // pair when a fence-refusal streak indicates this node's visible entry stopped converging with its
    // committed head — the wiring re-drives convergence from the node's own durable state, and the head
    // revision lets it say plainly when even that state is missing the head commit. Must not block and must
    // not re-enter this store.
    private Action<string, long, long>? onFenceWedgeRepair;

    /// <summary>Wires the fence-wedge repair hook (manager construction): (key, validated base revision,
    /// committed head revision). The callback runs on the apply path outside the gate, rate-limited by the
    /// refusal streak, and must not block or re-enter the store.</summary>
    public void AttachFenceWedgeRepairer(Action<string, long, long> repairer) => onFenceWedgeRepair = repairer;

    // Invoked (outside the apply gate) with the refused prepare's intent and the remembered committed-head
    // revision whenever THIS node's fence flags a replicated validated-base prepare as stale. The verdict is
    // deterministically correct at the prepare's apply position (heads record only real commits, in log
    // order), even when the acknowledging leader's frozen memory admitted the prepare — so the wiring drives
    // a best-effort abort at the transaction's anchor. Must not block and must not re-enter the store.
    private Action<PreparedIntent, long>? onStaleBaseVeto;

    /// <summary>Wires the replica stale-base veto hook (manager construction): (refused intent, committed head
    /// revision). Runs on the apply path outside the gate, once per stale-flagged prepare apply on this node,
    /// and must not block or re-enter the store. See <see cref="ApplyDeltaAckPrepares"/> for the trigger.</summary>
    public void AttachStaleBaseVetoer(Action<PreparedIntent, long> vetoer) => onStaleBaseVeto = vetoer;

    /// <summary>Sets the staged-base fence retention horizon. The caller must pass a value comfortably above
    /// the longest possible transaction lifetime: the staleness gate refuses to acknowledge a validated-base
    /// prepare from a transaction older than this horizon, so a horizon below real transaction lifetimes turns
    /// long transactions into spurious aborts. Idempotent; safe to call on a shared store.</summary>
    public void ConfigureStagedBaseFence(int retentionMs) =>
        stagedBaseFenceRetentionMs = Math.Max(1, retentionMs);

    /// <summary>
    /// Declares whether this node produces and applies one-phase bundled commits that are validated against
    /// the committed-head ledger at apply time. The ledger itself is always maintained and persisted; what
    /// this guards is the load-time contract: a durable snapshot written by a build that predates the ledger
    /// section leaves the partition's slice empty, and a node that then applied the extended gate would admit
    /// bundles its peers refuse. Enabling the gate over such a snapshot therefore fails closed (throws) with
    /// the remedy — start once with the option off so the next checkpoint rewrites every partition's snapshot
    /// with its ledger, then enable it. Idempotent; safe to call on a shared store.
    /// </summary>
    public void ConfigureOnePhaseApplyTimeValidation(bool enabled)
    {
        if (!enabled)
            return;

        lock (applyGate)
        {
            if (ledgerMissingPartitions.Count == 0)
                return;

            throw new InvalidDataException(
                $"Prepared-intent snapshot(s) for partition(s) {string.Join(", ", ledgerMissingPartitions)} were written before the committed-head ledger existed; " +
                "refusing to enable OnePhaseApplyTimeValidation over them, because this node would judge bundled commits differently from its replicas. " +
                "Start once with OnePhaseApplyTimeValidation off so the next checkpoint rewrites every partition's snapshot with its ledger, then enable it.");
        }
    }

    // ── Fence-verdict wait ───────────────────────────────────────────────────────
    //
    // A pre-decision fence verdict request routinely arrives before the transaction's prepare has applied on
    // this node: the coordinator releases the verdict request and the apply broadcast from the same
    // post-durability callback, so the request wins that race deterministically on the synchronous-fsync
    // commit path. Waiters therefore park on this pulse and are woken by the mutation paths the moment the
    // intent set changes, instead of sleeping on a poll timer whose full tick every read-modify-write commit
    // would pay as a latency floor.

    // Completed and replaced whenever the intent set changes while a waiter is parked. Waiters capture the
    // current instance BEFORE evaluating; a mutation that lands after their evaluation completes the instance
    // they hold (every replaced instance is completed first), so a wakeup can never be lost. Created with
    // RunContinuationsAsynchronously so the mutation paths never run waiter continuations inline.
    private TaskCompletionSource intentSetChangedPulse = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Number of parked fence waiters. While zero — the common case on every apply that no verdict request is
    // waiting on — the mutation paths skip the pulse swap and its allocation entirely.
    private int fenceWaiterCount;

    /// <summary>Wakes parked fence-verdict waiters after a mutation of the intent set. Called outside
    /// <see cref="applyGate"/>; a single volatile read when no waiter is parked.</summary>
    private void SignalFenceWaiters()
    {
        if (Volatile.Read(ref fenceWaiterCount) == 0)
            return;

        TaskCompletionSource pulse = Interlocked.Exchange(
            ref intentSetChangedPulse, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        pulse.TrySetResult();
    }

    /// <summary>Applies one transition to the intent at the command's key and reflects the result in the map:
    /// install/update on <see cref="TransactionApplyOutcome.Applied"/> with a record, delete when the applied
    /// record is null (removal), and leave the map unchanged on a no-op or rejection. This overload cannot
    /// attribute the apply to a partition's log, so any committed head it records lands in the
    /// <see cref="UnattributedPartition"/> ledger slice; production applies go through
    /// <see cref="Replicate"/>/<see cref="Restore"/>, which carry the log's partition.</summary>
    public PreparedIntentApplyResult Apply(PreparedIntentCommand command) => Apply(command, UnattributedPartition);

    /// <summary>Applies one transition that arrived through <paramref name="partitionId"/>'s log. The partition
    /// selects the committed-head ledger slice a commit settlement feeds — the slice the one-phase bundled
    /// commit gate later judges bundles of that partition against — so it must be the log's partition, never a
    /// routing guess: routing changes with the range map while the log's partition is fixed.</summary>
    public PreparedIntentApplyResult Apply(PreparedIntentCommand command, int partitionId) =>
        Apply(command, partitionId, HistoryWindow.Live, replay: false, HLCTimestamp.Zero, logIndex: 0);

    /// <param name="window">Where the entry sits relative to the partition's installed snapshot. The advisory
    /// staged-base fence judges a validated-base prepare only for a <see cref="HistoryWindow.Live"/> entry; for
    /// history the partition's installed ledger already reflects (<see cref="IsHistoricalApply"/>) the transition
    /// still applies, but no refusal, veto, wedge streak or metric is produced from a ledger that is ahead of
    /// the entry by construction — and at or below the fully reflected position a prepare that finds no intent
    /// installs nothing at all.</param>
    /// <param name="replay">True on a restart replay of the retained log. The transitions apply as live ones do,
    /// but nothing is reported to watches: the replayed entries were reported, if at all, before the restart.</param>
    /// <param name="entryTime">The time Kommander stamped on the log entry that carries the command, for the commit
    /// admission window; <see cref="HLCTimestamp.Zero"/> for an apply with no log entry behind it, which the window
    /// cannot judge and admits.</param>
    /// <param name="logIndex">The log entry that carries the command; zero for an apply with no entry behind it.</param>
    private PreparedIntentApplyResult Apply(PreparedIntentCommand command, int partitionId, HistoryWindow window, bool replay, HLCTimestamp entryTime, long logIndex)
    {
        string key = KeyOf(command);
        bool judgeFence = window == HistoryWindow.Live;

        PreparedIntent? settledCommit = null;
        PreparedIntent? watchCommit = null;
        bool transactionDrained = false;
        bool watchTransactionEnd = false;
        bool wedgeRepairDue = false;
        long wedgeBaseRevision = 0;
        long wedgeHeadRevision = 0;
        PreparedIntentApplyResult result;

        lock (applyGate)
        {
            intents.TryGetValue(key, out PreparedIntent? existing);

            // ── Fully reflected history: nothing to install ──
            // The installed intent set is exact for entries at or below the fully reflected position (the
            // exporter's position when its walk began): every intent prepared there and still live at the
            // export is installed, and one removed during the walk is removed again by its own replayed
            // removal. A prepare that finds no intent here is therefore either a transaction the installed
            // state already settled, or one the exporter rejected — installing it would create, out of history,
            // a phantom holder no replica of the partition ever had. Folded as a no-op; the entry's later
            // transitions fold the same way. On a restart replay the caller keeps the folded prepare as replay
            // history (see replayHistory): the window's materializations name it and have no other source.
            if (window == HistoryWindow.FullyReflected && existing is null && command is PrepareIntentCommand)
                return new PreparedIntentApplyResult(TransactionApplyOutcome.IdempotentNoop, null, null);

            // ── Settled-identity replay, judged before the state machine ──
            // A prepare of a leg that already settled on this partition's log is a re-driven duplicate: the
            // finalizer re-proposed the delta (a proposal released as not-leader at a step-down, or an apply this
            // node never observed) after its first copy had landed, committed and settled on the successor. The
            // state machine alone would re-install it — the settlement removed the intent, so the key looks free
            // — as a pending intent for a decided transaction: a phantom holder the recovery sweep has to settle
            // again, and one the staged-base fence then judges against a head that already contains the
            // transaction's own commit, refusing it as stale and driving a veto that finds the commit and counts
            // it as an acknowledged stale-base commit. Rejecting it here installs nothing, judges nothing, and
            // names the reason so the producer reads the decision from the record. The memory is a pure
            // function of the partition's log (fed by the settlement's remove, persisted and installed with the
            // slice), so every replica rejects the same entry — this is a replicated transition, not an
            // advisory verdict, and it therefore applies on every path including history replay.
            if (command is PrepareIntentCommand replayCandidate && IsSettledIdentity(partitionId, replayCandidate.Intent))
            {
                result = new PreparedIntentApplyResult(
                    TransactionApplyOutcome.Rejected, existing,
                    "prepare of a transaction whose settlement already applied on this partition (re-driven duplicate)",
                    SettledReplay: true);

                if (judgeFence)
                {
                    DurableTransactionMetrics.SettledPrepareReplays.Add(1);

                    if (logger is not null && logger.IsEnabled(LogLevel.Information))
                        logger.LogInformation(
                            "Prepare of transaction {TransactionId} (epoch {Epoch}) on key {Key} arrived after its settlement applied on partition {PartitionId}; rejected as a re-driven duplicate instead of re-installing a settled intent",
                            replayCandidate.Intent.TransactionId, replayCandidate.Intent.Epoch, key, partitionId);
                }

                return result;
            }

            // ── Commit admission window, judged before the state machine ──
            // A prepare that would install a new pending intent creates a new commit, so its commit timestamp must
            // fall inside the window around its entry's time (see CommitAdmission); one outside it installs nothing
            // on any replica. A same-identity re-prepare of an intent already here creates no new commit (the intent
            // is already pending or already decided), so it keeps the state machine's answer; the settled-identity
            // replay above keeps its own. The verdict reads only the entry, so it is a replicated transition and
            // applies on every path, history replay included.
            if (command is PrepareIntentCommand { AdmissionLagMs: > 0 } admissionCandidate
                && !(existing is not null
                     && existing.TransactionId == admissionCandidate.Intent.TransactionId
                     && existing.Epoch == admissionCandidate.Intent.Epoch))
            {
                CommitAdmissionVerdict verdict = CommitAdmission.Judge(entryTime, admissionCandidate.Intent.CommitTimestamp, admissionCandidate.AdmissionLagMs);
                if (verdict != CommitAdmissionVerdict.Admitted)
                {
                    if (judgeFence && !replay)
                        CommitAdmission.CountRefusal(verdict, "prepare");

                    return new PreparedIntentApplyResult(
                        TransactionApplyOutcome.Rejected, existing,
                        verdict == CommitAdmissionVerdict.BelowFloor
                            ? "commit timestamp at or below the commit admission floor of its log entry"
                            : "commit timestamp too far ahead of the time of its log entry",
                        AdmissionRefused: true);
                }
            }

            result = PreparedIntentStateMachine.Apply(existing, command);

            if (result.Outcome == TransactionApplyOutcome.Applied)
            {
                if (result.Intent is null)
                {
                    RemoveLive(key, out _);
                    if (existing is not null)
                        Interlocked.Add(ref totalBytes, -IntentBytes(existing));
                }
                else
                {
                    transactionDrained = InstallLive(key, result.Intent, partitionId);
                    ObserveInstalled(result.Intent);
                    Interlocked.Add(ref totalBytes, IntentBytes(result.Intent) - (existing is null ? 0 : IntentBytes(existing)));
                }

                StampDirty(key);
            }

            // A commit resolution (or the removal of a committed intent — the import/replay orderings where the
            // resolve applied elsewhere) makes the intent's mutation the key's committed head; remember it for
            // the staged-base fence. Idempotent replays record the same values. A resolve over an absent intent
            // (already settled and garbage-collected, decision replaying) has no mutation left to record.
            if (existing is not null)
            {
                bool committedNow = command is ResolveIntentCommand { Commit: true }
                    && result.Outcome is TransactionApplyOutcome.Applied or TransactionApplyOutcome.IdempotentNoop;
                bool removedCommitted = command is RemoveIntentCommand
                    && existing.Resolution == PreparedIntentResolution.Committed;

                if (committedNow || removedCommitted)
                {
                    bool headAdvanced = RecordCommittedHead(existing, partitionId);
                    settledCommit = existing;

                    // The watch event of a durable write: the one transition from pending to committed, applied
                    // live. Not the idempotent re-resolve of an intent already committed, not the removal that
                    // follows (both also reach this branch), not a restart replay, and not history an installed
                    // snapshot already reflects. A re-driven prepare that arrives after the settled-identity window
                    // installs a pending intent again for a transaction that already settled here; its second
                    // settlement finds the ledger head already at its revision, so it is not reported twice.
                    if (committedNow
                        && headAdvanced
                        && result.Outcome == TransactionApplyOutcome.Applied
                        && existing.IsPending
                        && !replay
                        && window == HistoryWindow.Live)
                        watchCommit = existing;

                    // The last pending intent of the transaction on this partition committed live: the consumer now
                    // holds every event of the transaction on the partition. Independent of the repeat guard above,
                    // which can suppress this key's event while an earlier key's event was delivered.
                    if (committedNow
                        && transactionDrained
                        && result.Outcome == TransactionApplyOutcome.Applied
                        && existing.IsPending
                        && !replay
                        && window == HistoryWindow.Live)
                        watchTransactionEnd = true;
                }

                // The removal of a resolved intent — commit or abort — is the settlement that opens the hole a
                // re-driven prepare of this leg would fall into; remember the identity and key so that prepare is
                // rejected instead.
                if (command is RemoveIntentCommand && result.Outcome == TransactionApplyOutcome.Applied && existing.IsResolved)
                    RecordSettledIdentity(existing, partitionId);

                // The removal takes the intent out of the live set and, at the next rewrite, out of the snapshot;
                // if its materialized row is still only queued here, keep the intent reachable for a restart replay
                // until the row is durable.
                if (removedCommitted)
                    RetainSettledIntentIfRowUnflushed(existing);
            }

            // ── Staged-base fence, evaluated at the prepare's own apply position ──
            // A freshly installed prepare is checked against the committed-head memory. The pre-propose
            // validation cannot see a competitor that commits the same base between its probe and this prepare
            // landing (the competitor's intent settled and was removed, so single-live-intent sees nothing) —
            // the interleaving that silently discards the competitor's write (a lost update). Here that
            // competitor is always visible: either its intent is still live (the state machine already rejected
            // this prepare), or its settlement applied earlier on this key and left its head above.
            //
            // Two rules judge it. A validated base (read-then-write) must not have been moved past. And every
            // revision-carrying prepare — blind writes included — must stage a revision above the head: a
            // revision identifies one mutation, and a head already at it means another transaction committed
            // that revision. The producer of that shape is a writer whose in-memory exclusion was lost at a
            // leader change: it staged base+1 on the deposed leader, the new leader granted the key again, and
            // the second writer staged and committed the same base+1 first. Committing the first writer then
            // puts two records at one revision in the log, and the later one silently replaces the earlier,
            // acknowledged one.
            //
            // The verdict does NOT change the replicated transition — the intent installs identically on every
            // node, so replicas and replay never diverge. It only flags the result; the local producer's
            // acknowledgement path (<see cref="ApplyDeltaAckPrepares"/>) folds the flag into a refused prepare,
            // which drives the coordinator's standard truthful conflict abort, and the abort's resolution rolls
            // the installed intent back everywhere.
            //
            // Judged on a fresh install AND on the idempotent same-identity re-prepare of a still-pending
            // intent: the finalizer's prepare-retry loop re-proposes the same delta and reads the no-op as "already
            // prepared", so a verdict given only on the first apply would be washed away by the first retry.
            // Heads only move forward, so once a base is stale every re-ask answers stale, and the retry budget
            // exhausts into the truthful abort. A RESOLVED existing intent is left alone — that is a decision
            // replay, owned by the record, with no acknowledgement left to protect. A foreign holder is the state
            // machine's rejection to report.
            bool freshInstall = existing is null && result.Outcome == TransactionApplyOutcome.Applied;
            bool pendingSameIdentityReprepare = existing is { IsPending: true } && result.Outcome == TransactionApplyOutcome.IdempotentNoop;

            if ((freshInstall || pendingSameIdentityReprepare)
                && command is PrepareIntentCommand fencedPrepare
                && IsFenceJudged(fencedPrepare.Intent))
            {
                if (!judgeFence)
                {
                    // History below an installed snapshot's reflected position: the ledger already holds this
                    // transaction's own settlement (or a later one), so any verdict here would be a false
                    // refusal. Counted, never judged.
                    DurableTransactionMetrics.StagedBaseFenceHistoryReplays.Add(1);
                }
                else
                {
                    string? fenceConflict = EvaluateStagedBaseFence(fencedPrepare.Intent, out bool revisionCollision);
                    if (fenceConflict is not null)
                    {
                        DurableTransactionMetrics.StagedBasePrepareRejections.Add(1);
                        if (revisionCollision)
                            DurableTransactionMetrics.StagedRevisionCollisions.Add(1);

                        // The revision the refused write was built on: its validated base, or — for a revision
                        // collision — the entry it staged over, one below its staged revision.
                        wedgeBaseRevision = revisionCollision ? fencedPrepare.Intent.Revision - 1 : fencedPrepare.Intent.BaseRevision;
                        wedgeRepairDue = TrackFenceRefusal(fencedPrepare.Intent.Key, wedgeBaseRevision, out wedgeHeadRevision);

                        logger?.LogWarning(
                            "Committed head for {Key} moved past the prepare of transaction {TransactionId} before it applied; refusing the prepare acknowledgement to prevent a lost update: {Reason}",
                            fencedPrepare.Intent.Key, fencedPrepare.Intent.TransactionId, fenceConflict);

                        result = result with { StaleBase = true };
                    }
                    else
                        fenceRefusalStreaks.TryRemove(key, out _);
                }
            }
        }

        // Outside the gate: the hooks may rent messages, schedule tasks, and enqueue actor work, none of which
        // may run under the apply lock. The hooks must not re-enter this store.
        if (result.Outcome == TransactionApplyOutcome.Applied)
            SignalFenceWaiters();

        if (settledCommit is not null)
            onCommittedSettleApplied?.Invoke(settledCommit);

        if (watchCommit is not null)
            PublishCommittedWrite(watchCommit, partitionId, logIndex);

        if (watchTransactionEnd && settledCommit is not null)
            watchBroker?.PublishTransactionEnd(partitionId, settledCommit.TransactionId, logIndex);

        if (wedgeRepairDue)
            onFenceWedgeRepair?.Invoke(key, wedgeBaseRevision, wedgeHeadRevision);

        return result;
    }

    /// <summary>The current intent at <paramref name="key"/>, or null. This runs on every point read/write
    /// in the actor hot path, so it is a single lock-free lookup. It must not be guarded by an emptiness
    /// pre-check: <see cref="ConcurrentDictionary{TKey,TValue}.IsEmpty"/> answers "empty" only after taking
    /// every bucket lock of the map, so on a workload with no durable transactions — the one case the check
    /// was meant to shortcut — every actor thread serialized on those locks for every read.</summary>
    public PreparedIntent? Get(string key) =>
        intents.TryGetValue(key, out PreparedIntent? intent) ? intent : null;

    /// <summary>The intent at <paramref name="key"/> only when it belongs to the given transaction attempt.
    /// Lock-free for the same reason as <see cref="Get"/>.</summary>
    public PreparedIntent? GetByIdentity(HLCTimestamp transactionId, long epoch, string key) =>
        intents.TryGetValue(key, out PreparedIntent? intent)
        && intent.TransactionId == transactionId
        && intent.Epoch == epoch
            ? intent
            : null;

    /// <summary>Every intent currently held on this partition — the input to the recovery sweep.</summary>
    public IReadOnlyCollection<PreparedIntent> Snapshot() => intents.Values.ToArray();

    /// <summary>
    /// Every intent this node holds, together with each of <paramref name="partitionIds"/>'s applied log position
    /// read BEFORE the walk: every intent an entry at or below that position prepared, and that no later entry
    /// removed, is in the walk (a position is published only after its entry's commands are in the map). The map
    /// copy is atomic across keys.
    /// </summary>
    internal LiveIntentWalk WalkLiveIntents(IReadOnlyList<int> partitionIds)
    {
        Dictionary<int, long> appliedThrough = new(partitionIds.Count);
        foreach (int partitionId in partitionIds)
            appliedThrough[partitionId] = AppliedLogIndexOf(partitionId);

        return new LiveIntentWalk(intents.Values.ToArray(), appliedThrough);
    }

    /// <summary>Pending (undecided) intents whose recovery deadline is at or before <paramref name="now"/> —
    /// candidates for a recovery decision lookup. Deadline comparison is by HLC, never a local clock.</summary>
    public IReadOnlyList<PreparedIntent> DueForRecovery(HLCTimestamp now)
    {
        List<PreparedIntent> due = [];
        foreach (KeyValuePair<string, PreparedIntent> kv in intents)
        {
            PreparedIntent intent = kv.Value;
            if (intent.IsPending && intent.RecoveryDeadline != HLCTimestamp.Zero && intent.RecoveryDeadline <= now)
                due.Add(intent);
        }

        return due;
    }

    /// <summary>Due pending intents whose key routes to <paramref name="partitionId"/>. When no partition resolver
    /// is attached (the in-memory/test configuration), every due intent is returned.</summary>
    public IReadOnlyList<PreparedIntent> DueForRecovery(HLCTimestamp now, int partitionId)
    {
        Func<string, int>? resolver = resolvePartition;

        List<PreparedIntent> due = [];

        foreach (KeyValuePair<string, PreparedIntent> kv in intents)
        {
            PreparedIntent intent = kv.Value;
            if (!intent.IsPending || intent.RecoveryDeadline == HLCTimestamp.Zero || intent.RecoveryDeadline > now)
                continue;

            if (resolver is not null && resolver(intent.Key) != partitionId)
                continue;

            due.Add(intent);
        }

        return due;
    }

    /// <summary>
    /// The lowest commit timestamp among the live intents that can still commit (pending, or resolved to commit
    /// and not yet removed), or <see cref="HLCTimestamp.Zero"/> when there is none. A point-in-time read: an intent
    /// whose prepare applies after the scan is not seen — <see cref="BeginCommitObservation"/> covers that.
    /// </summary>
    public HLCTimestamp MinUnsettledCommitTimestamp()
    {
        HLCTimestamp min = HLCTimestamp.Zero;

        foreach (KeyValuePair<string, PreparedIntent> kv in intents)
        {
            PreparedIntent intent = kv.Value;
            if (intent.Resolution == PreparedIntentResolution.Aborted || intent.CommitTimestamp == HLCTimestamp.Zero)
                continue;

            if (min == HLCTimestamp.Zero || intent.CommitTimestamp.CompareTo(min) < 0)
                min = intent.CommitTimestamp;
        }

        return min;
    }

    /// <summary>
    /// Opens an observation of the lowest commit timestamp among the intents live now and every intent installed
    /// on this node until the observation is disposed (see <see cref="PreparedIntentCommitObservation"/>).
    ///
    /// <para>The observation is registered before the live set is scanned. An install writes the map before it
    /// reads the registry, so an install the registry read misses happened before the registration, and the scan
    /// that follows the registration sees it.</para>
    /// </summary>
    public PreparedIntentCommitObservation BeginCommitObservation()
    {
        PreparedIntentCommitObservation observation = new(EndCommitObservation);

        lock (commitObservationGate)
            commitObservations = [.. commitObservations, observation];

        foreach (KeyValuePair<string, PreparedIntent> kv in intents)
            observation.Observe(kv.Value);

        return observation;
    }

    private void EndCommitObservation(PreparedIntentCommitObservation observation)
    {
        lock (commitObservationGate)
        {
            PreparedIntentCommitObservation[] current = commitObservations;
            int index = Array.IndexOf(current, observation);
            if (index < 0)
                return;

            PreparedIntentCommitObservation[] next = new PreparedIntentCommitObservation[current.Length - 1];
            Array.Copy(current, 0, next, 0, index);
            Array.Copy(current, index + 1, next, index, current.Length - index - 1);
            commitObservations = next;
        }
    }

    // Called after every write of an intent into the map. Costs one volatile read when no observation is open.
    private void ObserveInstalled(PreparedIntent intent)
    {
        PreparedIntentCommitObservation[] open = commitObservations;
        for (int i = 0; i < open.Length; i++)
            open[i].Observe(intent);
    }

    public int Count => intents.Count;

    // Running sum of resident intent value bytes, maintained on every install/update/remove across Apply and the
    // load/import merge, so durable admission can bound resident prepared-intent memory without an O(n) scan.
    private long totalBytes;

    /// <summary>Resident prepared-intent value bytes across this node (durable admission bound / observability).</summary>
    public long TotalBytes => Interlocked.Read(ref totalBytes);

    private static long IntentBytes(PreparedIntent intent) => intent.Value?.Length ?? 0;

    private static string KeyOf(PreparedIntentCommand command) => command switch
    {
        PrepareIntentCommand prepare => prepare.Intent.Key,
        ResolveIntentCommand resolve => resolve.Key,
        RemoveIntentCommand remove => remove.Key,
        _ => throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown prepared-intent command")
    };

}
