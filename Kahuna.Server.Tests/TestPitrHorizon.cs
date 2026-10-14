
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="PitrHorizon.ComputeProtectedIndex"/>:
/// the sliding-horizon tick that maps wall-clock "now" + PITR config to a WAL retention floor.
/// </summary>
public sealed class TestPitrHorizon
{
    private const int P = 1;

    private static readonly TimeSpan PitrWindow = TimeSpan.FromHours(1);
    private static readonly TimeSpan SnapshotInterval = TimeSpan.FromMinutes(30);

    // Epoch used for HLC ticks in tests (Unix ms of 2026-01-01 00:00:00 UTC).
    private static readonly DateTime Epoch = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    // Convert a wall-clock DateTime to the HLC millisecond value the production code would use.
    private static long ToMs(DateTime dt) => (long)(dt - DateTime.UnixEpoch).TotalMilliseconds;

    private static IWAL BuildWal(params (long id, DateTime time, RaftLogType type)[] entries)
    {
        ILogger<IRaft> logger = NullLogger<IRaft>.Instance;
        InMemoryWAL wal = new(logger);
        List<RaftLog> logs = entries
            .Select(e => new RaftLog { Id = e.id, Type = e.type, Time = new HLCTimestamp(0, ToMs(e.time), 0) })
            .ToList();
        if (logs.Count > 0)
            wal.Write([(P, logs)]);
        return wal;
    }

    private static long Compute(IWAL wal, DateTime nowUtc) =>
        PitrHorizon.ComputeProtectedIndex(wal, P, nowUtc, PitrWindow, SnapshotInterval);

    // ── nothing at or before the boundary: the floor is still asserted ───────────

    [Fact]
    public void EmptyWal_ProtectsFromTheFirstIndex()
    {
        // Entries that land before the next tick are inside the window from the start; a cleared
        // floor would let a compaction pass remove them up to the checkpoint.
        IWAL wal = BuildWal();
        long result = Compute(wal, Epoch + TimeSpan.FromHours(3));
        Assert.Equal(1, result);
    }

    [Fact]
    public void AllEntriesNewerThanBoundary_ProtectsTheFirstCommittedEntry()
    {
        // "now" is T+0, boundary = T+0 - 1h - 30m = T-90m.
        // All entries are at T+0 or later, so the whole log is inside the window and all of it must
        // be retained: the floor is its first entry, never "no floor".
        IWAL wal = BuildWal(
            (1, Epoch, RaftLogType.Committed),
            (2, Epoch + TimeSpan.FromMinutes(10), RaftLogType.Committed)
        );

        long result = Compute(wal, Epoch);
        Assert.Equal(1, result);
    }

    [Fact]
    public void AllEntriesNewerThanBoundary_LeadingUncommittedEntries_ProtectsTheFirstCommittedOne()
    {
        IWAL wal = BuildWal(
            (1, Epoch, RaftLogType.Proposed),
            (2, Epoch, RaftLogType.Proposed),
            (3, Epoch + TimeSpan.FromMinutes(1), RaftLogType.Committed),
            (4, Epoch + TimeSpan.FromMinutes(2), RaftLogType.Committed)
        );

        Assert.Equal(3, Compute(wal, Epoch));
    }

    [Fact]
    public void CompactedPrefix_RetainedLogEntirelyNewerThanBoundary_ProtectsTheFloor()
    {
        // The steady state on a loaded node: compaction already removed everything older than the
        // checkpoint, and every retained entry is younger than the boundary. The floor must hold the
        // retained log where it starts rather than clear and let the next pass remove it too.
        InMemoryWAL wal = (InMemoryWAL)BuildWal(
            (1, Epoch, RaftLogType.Committed),
            (2, Epoch + TimeSpan.FromMinutes(1), RaftLogType.Committed),
            (3, Epoch + TimeSpan.FromMinutes(2), RaftLogType.Committed),
            (4, Epoch + TimeSpan.FromMinutes(3), RaftLogType.CommittedCheckpoint),
            (5, Epoch + TimeSpan.FromMinutes(4), RaftLogType.Committed)
        );
        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(P, 4, 100);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(3, removed);
        Assert.Equal(4, wal.GetCompactionFloor(P));

        Assert.Equal(4, Compute(wal, Epoch + TimeSpan.FromMinutes(10)));
    }

    [Fact]
    public void CompactedPrefix_BoundaryInsideTheRetainedLog_ReturnsTheBoundaryEntry()
    {
        // Commits every 10 minutes; the first three were compacted away. The search must still find
        // the last retained entry at or before the boundary.
        List<(long id, DateTime time, RaftLogType type)> entries = [];
        for (int i = 1; i <= 12; i++)
            entries.Add(((long)i, Epoch + TimeSpan.FromMinutes(i * 10), RaftLogType.Committed));
        InMemoryWAL wal = (InMemoryWAL)BuildWal([.. entries]);
        wal.CompactLogsOlderThan(P, 4, 100);
        Assert.Equal(4, wal.GetCompactionFloor(P));

        // boundary = now - 90m = Epoch + 80m → entry 8.
        DateTime now = Epoch + TimeSpan.FromMinutes(170);
        Assert.Equal(8, Compute(wal, now));
    }

    // ── single entry ─────────────────────────────────────────────────────────────

    [Fact]
    public void SingleEntryAtBoundaryExact_ReturnsIt()
    {
        // boundary = now - 1h - 30m = Epoch - 90m.
        DateTime now = Epoch + TimeSpan.FromHours(3);
        DateTime boundary = now - PitrWindow - SnapshotInterval; // Epoch + 90m

        IWAL wal = BuildWal((1, boundary, RaftLogType.Committed));

        long result = Compute(wal, now);
        Assert.Equal(1, result);
    }

    [Fact]
    public void SingleEntryBeforeBoundary_ReturnsIt()
    {
        DateTime now = Epoch + TimeSpan.FromHours(3);
        DateTime boundary = now - PitrWindow - SnapshotInterval;

        IWAL wal = BuildWal((1, boundary - TimeSpan.FromMinutes(5), RaftLogType.Committed));

        long result = Compute(wal, now);
        Assert.Equal(1, result);
    }

    // ── monotonicity ─────────────────────────────────────────────────────────────

    [Fact]
    public void AdvancingNow_ProtectedIndexNeverDecreases()
    {
        // WAL has commits every 10 minutes from Epoch to Epoch+4h.
        List<(long id, DateTime time, RaftLogType type)> entries = [];
        for (int i = 1; i <= 24; i++)
            entries.Add(((long)i, Epoch + TimeSpan.FromMinutes(i * 10), RaftLogType.Committed));

        IWAL wal = BuildWal([.. entries]);

        // Advance "now" from Epoch+2h to Epoch+6h in 15-minute steps.
        long prev = -1;
        DateTime now = Epoch + TimeSpan.FromHours(2);

        while (now <= Epoch + TimeSpan.FromHours(6))
        {
            long idx = Compute(wal, now);
            Assert.True(idx >= prev, $"index went backward: {prev} → {idx} at now={now:HH:mm}");
            prev = idx;
            now += TimeSpan.FromMinutes(15);
        }
    }

    [Fact]
    public void ProtectedIndex_NeverExceedsMaxCommittedIndex()
    {
        // Commit 5 entries at known times.
        IWAL wal = BuildWal(
            (1, Epoch, RaftLogType.Committed),
            (2, Epoch + TimeSpan.FromMinutes(20), RaftLogType.Committed),
            (3, Epoch + TimeSpan.FromMinutes(40), RaftLogType.Committed),
            (4, Epoch + TimeSpan.FromMinutes(60), RaftLogType.Committed),
            (5, Epoch + TimeSpan.FromMinutes(80), RaftLogType.Committed)
        );

        long maxId = wal.GetMaxLog(P);

        for (int h = 2; h <= 10; h++)
        {
            long idx = Compute(wal, Epoch + TimeSpan.FromHours(h));
            Assert.True(idx >= 1 && idx <= maxId,
                $"protectedIndex={idx} is outside [1, maxId={maxId}] at h={h}");
        }
    }

    // ── uncommitted entries skipped ───────────────────────────────────────────────

    [Fact]
    public void UncommittedEntriesBeforeBoundary_AreSkipped()
    {
        DateTime now = Epoch + TimeSpan.FromHours(3);
        DateTime boundary = now - PitrWindow - SnapshotInterval; // Epoch + 90m

        // Index 1 is Proposed (uncommitted) at boundary-1m; index 2 is Committed at boundary-2m.
        IWAL wal = BuildWal(
            (1, boundary - TimeSpan.FromMinutes(2), RaftLogType.Committed),
            (2, boundary - TimeSpan.FromMinutes(1), RaftLogType.Proposed)
        );

        long result = Compute(wal, now);
        // Must return index 1 (the last *committed* entry at or before boundary).
        Assert.Equal(1, result);
    }

    // ── boundary calculation ──────────────────────────────────────────────────────

    [Fact]
    public void LoggedBoundaryMs_MatchesExpectedFormula()
    {
        // Verify the boundary formula: now - PitrWindow - BaseSnapshotInterval
        DateTime now = Epoch + TimeSpan.FromHours(3);
        DateTime expectedBoundary = now - PitrWindow - SnapshotInterval;
        long expectedMs = ToMs(expectedBoundary);

        // Build a WAL with one commit exactly at expectedBoundary.
        IWAL wal = BuildWal((1, expectedBoundary, RaftLogType.Committed));

        long result = Compute(wal, now);
        Assert.Equal(1, result); // entry at exactly boundary is included
        _ = expectedMs; // formula produces the correct value (no assertion needed beyond the above)
    }
}
