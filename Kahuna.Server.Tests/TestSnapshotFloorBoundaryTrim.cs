
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for the snapshot-floor boundary-pinning logic inside
/// <see cref="BaseHandler.RemoveExpiredRevisions"/>.
///
/// <para>These tests exercise the in-memory trim site directly: a <see cref="SnapshotFloorStore"/>
/// with an injected hold at timestamp T is wired into a <see cref="KeyValueContext"/>; a crafted
/// <see cref="KeyValueEntry"/> with an explicit <see cref="KeyValueEntry.Revisions"/> dictionary is
/// trimmed; the post-trim set is asserted.</para>
///
/// <para>Verified invariants:
/// <list type="bullet">
///   <item>With a live hold at T: the highest revision whose <see cref="KeyValueRevisionEntry.LastModified"/>
///   is at-or-before T is pinned (floor-boundary), even if it falls below the normal retention cutoff.</item>
///   <item>Revisions older than the floor-boundary are still removed.</item>
///   <item>Revisions within the normal retention window are always kept.</item>
///   <item>Without a hold (floor = Zero): exactly <see cref="KahunaConfiguration.RevisionRetention"/>
///   newest revisions are retained — no change from today's behaviour.</item>
///   <item>When the floor-boundary revision is already inside the normal retention window, no extra
///   revision is pinned (bounded: at most RetentionCount + 1 in memory).</item>
/// </list></para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestSnapshotFloorBoundaryTrim
{
    // ── thin subclass to expose RemoveExpiredRevisions for unit testing ──────────────────
    private sealed class TestableTrimHandler(KeyValueContext ctx) : BaseHandler(ctx)
    {
        public void TrimRevisions(KeyValueEntry entry, long refRevision) =>
            RemoveExpiredRevisions(entry, refRevision);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static RaftManager BuildRaft()
    {
        return new RaftManager(
            new RaftConfiguration
            {
                NodeName  = "floor-trim-test",
                NodeId    = 1,
                Host      = "localhost",
                Port      = 0,
                InitialPartitions = 1,
                EnableQuiescence  = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    private static KahunaConfiguration BuildConfig(int retentionCount = 3)
    {
        return ConfigurationValidator.Validate(new()
        {
            LocksWorkers              = 1,
            KeyValueWorkers           = 1,
            BackgroundWriterWorkers   = 1,
            Storage                   = "memory",
            RevisionRetention         = retentionCount,
            MaxEntriesPerActor        = 50_000,
            MaxBytesPerActor          = 256L * 1024 * 1024,
            CacheEntriesToRemove      = 1000,
            CollectBatchMax           = 1000,
            CacheEntryTtl             = TimeSpan.FromMinutes(5)
        });
    }

    private static bool InjectHold(SnapshotFloorStore store, SnapshotHold hold)
    {
        SnapshotFloorDeltaMessage delta = new();
        delta.Entries.Add(new SnapshotFloorDeltaEntry
        {
            Remove = false,
            Hold = new SnapshotHoldMessage
            {
                HoldId            = hold.HoldId,
                HolderId          = hold.HolderId,
                TimestampNode     = hold.Timestamp.N,
                TimestampPhysical = hold.Timestamp.L,
                TimestampCounter  = hold.Timestamp.C,
                LeaseExpiryNode     = hold.LeaseExpiry.N,
                LeaseExpiryPhysical = hold.LeaseExpiry.L,
                LeaseExpiryCounter  = hold.LeaseExpiry.C,
            },
        });
        byte[]  data = ReplicationSerializer.Serialize(delta);
        RaftLog log  = new() { LogType = ReplicationTypes.SnapshotFloor, LogData = data };
        return store.Restore(RangeMapStore.MetaPartitionId, log);
    }

    /// <summary>
    /// Builds a <see cref="KeyValueContext"/> suitable for exercising
    /// <see cref="BaseHandler.RemoveExpiredRevisions"/>. Only the fields accessed by that method
    /// are set; everything else is left null.
    /// </summary>
    private static KeyValueContext BuildContext(
        RaftManager raft,
        KahunaConfiguration config,
        SnapshotFloorStore? floorStore = null)
    {
        return new KeyValueContext(
            actorContext:        null!,
            store:               null!,
            locksByPrefix:       null!,
            locksByRange:        null!,
            proposals:           null!,
            backgroundWriter:    null!,
            proposalRouter:      null!,
            persistenceBackend:  null!,
            raft:                raft,
            keySpaceRegistry:    null!,
            rangeMapStore:       null!,
            configuration:       config,
            logger:              NullLogger<IKahuna>.Instance,
            snapshotFloorStore:  floorStore);
    }

    /// <summary>
    /// Builds a <see cref="KeyValueEntry"/> with <paramref name="count"/> archived revisions.
    /// Revision numbers run from 1 to <paramref name="count"/>; the LastModified of revision i is
    /// <c>new HLCTimestamp(1, (ulong)(i * 1000), 0)</c> so they are strictly ordered and easy
    /// to reason about.
    /// </summary>
    private static KeyValueEntry BuildEntry(int count)
    {
        Dictionary<long, KeyValueRevisionEntry> revisions = new(count);
        for (int i = 1; i <= count; i++)
        {
            HLCTimestamp ts = new(1, i * 1000L, 0);
            revisions[i] = new KeyValueRevisionEntry(
                new byte[] { (byte)i }, // value
                ts,                     // LastModified
                HLCTimestamp.Zero,      // Expires (no TTL)
                KeyValueState.Set);
        }
        return new KeyValueEntry
        {
            Revisions   = revisions,
            CachedBytes = 100_000   // large enough that accounting deltas stay non-negative
        };
    }

    // ── test cases ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// With a live hold at T (matching revision 2's LastModified) and RevisionRetention = 3,
    /// trimming entry with 6 revisions (cutoff = 6-3+1 = 4) should:
    /// <list type="bullet">
    ///   <item>Remove revision 1 (below cutoff, older than boundary)</item>
    ///   <item>Pin revision 2 (floor-boundary: highest revision ≤ T below cutoff)</item>
    ///   <item>Remove revision 3 (below cutoff, newer than boundary → left to disk)</item>
    ///   <item>Keep revisions 4-6 (within normal retention window)</item>
    /// </list>
    /// </summary>
    [Fact]
    public void WithHold_BoundaryRevision_PinnedBelowCutoff()
    {
        const int count     = 6;
        const int retention = 3;
        const long refRevision = count;
        const long boundaryRevision = 2L;

        RaftManager         raft   = BuildRaft();
        KahunaConfiguration config = BuildConfig(retention);
        SnapshotFloorStore  store  = new(raft, null, null, NullLogger<IKahuna>.Instance);

        // Hold timestamp = LastModified of revision 2
        HLCTimestamp holdTs     = new(1, boundaryRevision * 1000L, 0);
        HLCTimestamp farFuture  = new(1, long.MaxValue / 2, 0);

        bool ok = InjectHold(store, new SnapshotHold("h1", "client", holdTs, farFuture));
        Assert.True(ok);

        KeyValueContext ctx     = BuildContext(raft, config, store);
        TestableTrimHandler h  = new(ctx);
        KeyValueEntry entry    = BuildEntry(count);

        h.TrimRevisions(entry, refRevision);

        Assert.NotNull(entry.Revisions);
        Assert.False(entry.Revisions!.ContainsKey(1),  "revision 1 must be trimmed (below boundary)");
        Assert.True(entry.Revisions.ContainsKey(2),    "revision 2 must be pinned (floor-boundary)");
        Assert.False(entry.Revisions.ContainsKey(3),   "revision 3 must be trimmed (below cutoff, newer than boundary)");
        Assert.True(entry.Revisions.ContainsKey(4),    "revision 4 must be kept (retention window)");
        Assert.True(entry.Revisions.ContainsKey(5),    "revision 5 must be kept (retention window)");
        Assert.True(entry.Revisions.ContainsKey(6),    "revision 6 must be kept (current)");
        Assert.Equal(4, entry.Revisions.Count); // boundary (2) + retention window (4,5,6)
    }

    /// <summary>
    /// Without any hold the floor is Zero: trimming must keep exactly RevisionRetention
    /// entries and nothing more.
    /// </summary>
    [Fact]
    public void WithoutHold_TrimIsUnchanged()
    {
        const int count     = 6;
        const int retention = 3;
        const long refRevision = count;

        RaftManager         raft   = BuildRaft();
        KahunaConfiguration config = BuildConfig(retention);
        SnapshotFloorStore  store  = new(raft, null, null, NullLogger<IKahuna>.Instance);
        // No holds injected → floor = Zero

        KeyValueContext ctx     = BuildContext(raft, config, store);
        TestableTrimHandler h  = new(ctx);
        KeyValueEntry entry    = BuildEntry(count);

        h.TrimRevisions(entry, refRevision);

        Assert.NotNull(entry.Revisions);
        Assert.False(entry.Revisions!.ContainsKey(1), "revision 1 must be trimmed (beyond retention)");
        Assert.False(entry.Revisions.ContainsKey(2),  "revision 2 must be trimmed (beyond retention)");
        Assert.False(entry.Revisions.ContainsKey(3),  "revision 3 must be trimmed (beyond retention)");
        Assert.True(entry.Revisions.ContainsKey(4),   "revision 4 must be kept");
        Assert.True(entry.Revisions.ContainsKey(5),   "revision 5 must be kept");
        Assert.True(entry.Revisions.ContainsKey(6),   "revision 6 must be kept");
        Assert.Equal(retention, entry.Revisions.Count); // exactly RevisionRetention newest revisions, nothing more
    }

    /// <summary>
    /// Without a floor store at all (null SnapshotFloorStore) the trim falls back to the
    /// plain retention policy — RevisionRetention newest revisions.
    /// </summary>
    [Fact]
    public void WithNoFloorStore_TrimIsUnchanged()
    {
        const int count     = 6;
        const int retention = 3;
        const long refRevision = count;

        RaftManager         raft   = BuildRaft();
        KahunaConfiguration config = BuildConfig(retention);

        // No SnapshotFloorStore at all
        KeyValueContext ctx     = BuildContext(raft, config, floorStore: null);
        TestableTrimHandler h  = new(ctx);
        KeyValueEntry entry    = BuildEntry(count);

        h.TrimRevisions(entry, refRevision);

        Assert.NotNull(entry.Revisions);
        Assert.False(entry.Revisions!.ContainsKey(1), "revision 1 must be trimmed");
        Assert.False(entry.Revisions.ContainsKey(2),  "revision 2 must be trimmed");
        Assert.False(entry.Revisions.ContainsKey(3),  "revision 3 must be trimmed");
        // Revisions 4-6 kept: cutoff = 6-3+1 = 4 → keys 4,5,6 (exactly RevisionRetention newest)
        Assert.True(entry.Revisions.ContainsKey(4), "revision 4 must be kept");
        Assert.True(entry.Revisions.ContainsKey(5), "revision 5 must be kept");
        Assert.True(entry.Revisions.ContainsKey(6), "revision 6 must be kept");
    }

    /// <summary>
    /// When no revision below the cutoff has a LastModified ≤ floor (all trimmed candidates are
    /// newer than the floor), no boundary pin is applied — identical to the no-hold case.
    /// </summary>
    [Fact]
    public void WithHold_NoRevisionAtOrBeforeFloor_NoPinApplied()
    {
        const int count     = 6;
        const int retention = 3;
        const long refRevision = count;

        RaftManager         raft   = BuildRaft();
        KahunaConfiguration config = BuildConfig(retention);
        SnapshotFloorStore  store  = new(raft, null, null, NullLogger<IKahuna>.Instance);

        // Hold at a timestamp BEFORE any revision's LastModified (revision 1 has ts 1000)
        HLCTimestamp holdTs    = new(1, 500, 0);  // older than every revision
        HLCTimestamp farFuture = new(1, long.MaxValue / 2, 0);

        bool ok = InjectHold(store, new SnapshotHold("h1", "client", holdTs, farFuture));
        Assert.True(ok);

        KeyValueContext ctx    = BuildContext(raft, config, store);
        TestableTrimHandler h = new(ctx);
        KeyValueEntry entry   = BuildEntry(count);

        h.TrimRevisions(entry, refRevision);

        Assert.NotNull(entry.Revisions);
        Assert.False(entry.Revisions!.ContainsKey(1), "revision 1 must be trimmed (no boundary)");
        Assert.False(entry.Revisions.ContainsKey(2),  "revision 2 must be trimmed (no boundary)");
        Assert.False(entry.Revisions.ContainsKey(3), "revision 3 must be trimmed (no boundary)");
        Assert.True(entry.Revisions.ContainsKey(4), "revision 4 kept");
        Assert.True(entry.Revisions.ContainsKey(5), "revision 5 kept");
        Assert.True(entry.Revisions.ContainsKey(6), "revision 6 kept");
    }

    /// <summary>
    /// When multiple revisions fall below the normal retention cutoff, the floor-boundary
    /// pin selects the highest revision whose LastModified ≤ effectiveFloor. Revisions below
    /// the boundary (including those below cutoff with LastModified > floor) are still removed.
    ///
    /// <para>Setup: RetentionCount = 3, 8 revisions, refRevision = 8 → cutoff = 6.
    /// Revisions 1-5 are candidates. Hold at T = revision 3's timestamp (3000).</para>
    /// <list type="bullet">
    ///   <item>Revision 1 (ts=1000 ≤ T): candidate for boundary</item>
    ///   <item>Revision 2 (ts=2000 ≤ T): candidate for boundary</item>
    ///   <item>Revision 3 (ts=3000 ≤ T): candidate → selected as floor-boundary (highest)</item>
    ///   <item>Revision 4 (ts=4000 > T): NOT a boundary candidate → removed</item>
    ///   <item>Revision 5 (ts=5000 > T): NOT a boundary candidate → removed</item>
    ///   <item>Revisions 6-8: ≥ cutoff → kept by normal retention</item>
    /// </list>
    /// Final set: {3, 6, 7, 8}.
    /// </summary>
    [Fact]
    public void WithHold_HighestQualifyingBelowCutoff_SelectedAsBoundary()
    {
        const int count     = 8;
        const int retention = 3;
        const long refRevision = count;
        // cutoff = 8 - 3 + 1 = 6; candidates below cutoff: 1, 2, 3, 4, 5

        RaftManager         raft   = BuildRaft();
        KahunaConfiguration config = BuildConfig(retention);
        SnapshotFloorStore  store  = new(raft, null, null, NullLogger<IKahuna>.Instance);

        // Hold at revision 3's timestamp → boundary = 3 (highest revision below cutoff with ts ≤ 3000)
        HLCTimestamp holdTs    = new(1, 3000L, 0);
        HLCTimestamp farFuture = new(1, long.MaxValue / 2, 0);

        bool ok = InjectHold(store, new SnapshotHold("h1", "client", holdTs, farFuture));
        Assert.True(ok);

        KeyValueContext ctx    = BuildContext(raft, config, store);
        TestableTrimHandler h = new(ctx);
        KeyValueEntry entry   = BuildEntry(count);

        h.TrimRevisions(entry, refRevision);

        Assert.NotNull(entry.Revisions);
        Assert.False(entry.Revisions!.ContainsKey(1), "revision 1 trimmed (older than boundary)");
        Assert.False(entry.Revisions.ContainsKey(2),  "revision 2 trimmed (older than boundary)");
        Assert.True(entry.Revisions.ContainsKey(3),   "revision 3 pinned (floor-boundary)");
        Assert.False(entry.Revisions.ContainsKey(4),  "revision 4 trimmed (ts=4000 > floor=3000)");
        Assert.False(entry.Revisions.ContainsKey(5),  "revision 5 trimmed (below cutoff, ts=5000 > floor=3000)");
        Assert.True(entry.Revisions.ContainsKey(6),   "revision 6 kept (≥ cutoff)");
        Assert.True(entry.Revisions.ContainsKey(7),   "revision 7 kept");
        Assert.True(entry.Revisions.ContainsKey(8),   "revision 8 kept");
        Assert.Equal(4, entry.Revisions.Count); // boundary (3) + normal (6,7,8)
    }

    /// <summary>
    /// With an expired hold the floor is Zero (no live holds): trimming must behave exactly
    /// as if no floor were set — only RevisionRetention newest revisions kept.
    /// </summary>
    [Fact]
    public void WithExpiredHold_TrimIsUnchanged()
    {
        const int count     = 6;
        const int retention = 3;
        const long refRevision = count;

        RaftManager         raft   = BuildRaft();
        KahunaConfiguration config = BuildConfig(retention);
        SnapshotFloorStore  store  = new(raft, null, null, NullLogger<IKahuna>.Instance);

        // Hold at revision 2's timestamp but with an expiry in the past
        HLCTimestamp holdTs  = new(1, 2000, 0);
        HLCTimestamp expired = new(1, 1, 0);  // epoch — definitely expired

        bool ok = InjectHold(store, new SnapshotHold("h1", "client", holdTs, expired));
        Assert.True(ok);

        KeyValueContext ctx    = BuildContext(raft, config, store);
        TestableTrimHandler h = new(ctx);
        KeyValueEntry entry   = BuildEntry(count);

        h.TrimRevisions(entry, refRevision);

        Assert.NotNull(entry.Revisions);
        // Expired hold → floor = Zero → normal trim
        Assert.False(entry.Revisions!.ContainsKey(1), "revision 1 must be trimmed (hold expired)");
        Assert.False(entry.Revisions.ContainsKey(2),  "revision 2 must be trimmed (hold expired)");
        Assert.False(entry.Revisions.ContainsKey(3), "revision 3 must be trimmed (hold expired)");
        Assert.True(entry.Revisions.ContainsKey(4), "revision 4 kept");
        Assert.True(entry.Revisions.ContainsKey(5), "revision 5 kept");
        Assert.True(entry.Revisions.ContainsKey(6), "revision 6 kept");
    }

    // ── fail-loud floor guard (MissingProtectedVersion counter) ──────────────────────────

    /// <summary>
    /// Direct test of the detection seam behind the MissingProtectedVersion counter. The counter
    /// fires only when the floor-boundary revision appears in the trim removal set — a state correct
    /// trim logic never produces, so it can only be exercised in isolation. Proves the counter *would*
    /// fire on a regression (true positive) and stays quiet otherwise (true negative).
    /// </summary>
    [Fact]
    public void RemovalSetDropsFloorBoundary_DetectsProtectedRevisionOnlyWhenPresent()
    {
        // Boundary (2) present in the removal set → violation.
        Assert.True(BaseHandler.RemovalSetDropsFloorBoundary(new HashSet<long> { 1, 2, 3 }, 2));

        // Boundary (2) exempted from the removal set → no violation (the normal case).
        Assert.False(BaseHandler.RemovalSetDropsFloorBoundary(new HashSet<long> { 1, 3 }, 2));

        // No active floor boundary (-1) → never a violation regardless of the removal set.
        Assert.False(BaseHandler.RemovalSetDropsFloorBoundary(new HashSet<long> { 1, 2, 3 }, -1));

        // Empty removal set → nothing dropped.
        Assert.False(BaseHandler.RemovalSetDropsFloorBoundary(new HashSet<long>(), 2));
    }

    /// <summary>
    /// A normal trim under a live hold must never fire the MissingProtectedVersion counter: the
    /// boundary is exempted, so no floor-protected revision is dropped. This guards the
    /// "counter stays 0 in normal operation" invariant at the reclamation site.
    /// </summary>
    [Fact]
    public void WithHold_NormalTrim_DoesNotFireMissingProtectedCounter()
    {
        long counter = 0;
        using System.Diagnostics.Metrics.MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "kahuna.snapshot_floor.missing_protected_version_total")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            Interlocked.Add(ref counter, measurement));
        listener.Start();

        const int count = 6;
        const int retention = 3;
        const long refRevision = count;

        RaftManager         raft   = BuildRaft();
        KahunaConfiguration config = BuildConfig(retention);
        SnapshotFloorStore  store  = new(raft, null, null, NullLogger<IKahuna>.Instance);

        HLCTimestamp holdTs    = new(1, 2000L, 0);
        HLCTimestamp farFuture = new(1, long.MaxValue / 2, 0);
        Assert.True(InjectHold(store, new SnapshotHold("h1", "client", holdTs, farFuture)));

        KeyValueContext ctx    = BuildContext(raft, config, store);
        TestableTrimHandler h = new(ctx);
        KeyValueEntry entry   = BuildEntry(count);

        h.TrimRevisions(entry, refRevision);

        // Boundary (rev 2) retained, counter untouched.
        Assert.True(entry.Revisions!.ContainsKey(2));
        Assert.Equal(0, Interlocked.Read(ref counter));
    }
}
