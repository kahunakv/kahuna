
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Tests.Server;

/// <summary>
/// Unit tests for <see cref="LogTimeIndex"/>: binary-search helpers that map HLC timestamps
/// to committed WAL indices.
/// </summary>
public sealed class TestPitrLogTimeIndex
{
    private const int P = 1; // partition id used throughout

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static IWAL BuildWal(params (long id, long ticks, RaftLogType type)[] entries)
    {
        ILogger<IRaft> logger = NullLogger<IRaft>.Instance;
        InMemoryWAL wal = new(logger);

        List<RaftLog> logs = [];
        foreach ((long id, long ticks, RaftLogType type) in entries)
        {
            logs.Add(new RaftLog
            {
                Id = id,
                Type = type,
                Time = new HLCTimestamp(0, ticks, 0)
            });
        }

        if (logs.Count > 0)
            wal.Write([(P, logs)]);

        return wal;
    }

    private static HLCTimestamp T(long ticks) => new(0, ticks, 0);

    // ── empty WAL ────────────────────────────────────────────────────────────────────────

    [Fact]
    public void EmptyWal_FirstAtOrAfter_ReturnsMinusOne()
    {
        IWAL wal = BuildWal();
        Assert.Equal(-1, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(100)));
    }

    [Fact]
    public void EmptyWal_LastAtOrBefore_ReturnsMinusOne()
    {
        IWAL wal = BuildWal();
        Assert.Equal(-1, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(100)));
    }

    // ── before first committed entry ─────────────────────────────────────────────────────

    [Fact]
    public void BeforeFirst_FirstAtOrAfter_ReturnsFirstEntry()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 300, RaftLogType.Committed)
        );

        Assert.Equal(1, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(50)));
    }

    [Fact]
    public void BeforeFirst_LastAtOrBefore_ReturnsMinusOne()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 300, RaftLogType.Committed)
        );

        Assert.Equal(-1, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(50)));
    }

    // ── after last committed entry ────────────────────────────────────────────────────────

    [Fact]
    public void AfterLast_FirstAtOrAfter_ReturnsMinusOne()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 300, RaftLogType.Committed)
        );

        Assert.Equal(-1, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(400)));
    }

    [Fact]
    public void AfterLast_LastAtOrBefore_ReturnsLastEntry()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 300, RaftLogType.Committed)
        );

        Assert.Equal(3, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(400)));
    }

    // ── exact match ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void ExactMatch_FirstAtOrAfter_ReturnsMatchingIndex()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 300, RaftLogType.Committed)
        );

        Assert.Equal(2, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(200)));
    }

    [Fact]
    public void ExactMatch_LastAtOrBefore_ReturnsMatchingIndex()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 300, RaftLogType.Committed)
        );

        Assert.Equal(2, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(200)));
    }

    // ── gap (target between two consecutive entries) ─────────────────────────────────────

    [Fact]
    public void Gap_FirstAtOrAfter_ReturnsEntryJustAfterGap()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 400, RaftLogType.Committed)  // gap: nothing at t=300
        );

        // target = 300 falls between index 2 (t=200) and index 3 (t=400)
        Assert.Equal(3, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(300)));
    }

    [Fact]
    public void Gap_LastAtOrBefore_ReturnsEntryJustBeforeGap()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 400, RaftLogType.Committed)
        );

        Assert.Equal(2, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(300)));
    }

    // ── duplicate timestamps ──────────────────────────────────────────────────────────────

    [Fact]
    public void DuplicateTimestamps_FirstAtOrAfter_ReturnsSmallestMatchingIndex()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 200, RaftLogType.Committed),
            (4, 200, RaftLogType.Committed),
            (5, 300, RaftLogType.Committed)
        );

        Assert.Equal(2, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(200)));
    }

    [Fact]
    public void DuplicateTimestamps_LastAtOrBefore_ReturnsLargestMatchingIndex()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 200, RaftLogType.Committed),
            (3, 200, RaftLogType.Committed),
            (4, 200, RaftLogType.Committed),
            (5, 300, RaftLogType.Committed)
        );

        Assert.Equal(4, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(200)));
    }

    // ── uncommitted entries are skipped ───────────────────────────────────────────────────

    [Fact]
    public void UncommittedEntriesSkipped_FirstAtOrAfter()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 150, RaftLogType.Proposed),    // uncommitted — must be invisible
            (3, 200, RaftLogType.Committed),
            (4, 250, RaftLogType.Proposed),    // uncommitted tail
            (5, 300, RaftLogType.Proposed)
        );

        Assert.Equal(3, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(150)));
    }

    [Fact]
    public void UncommittedEntriesSkipped_LastAtOrBefore()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.Committed),
            (2, 150, RaftLogType.Proposed),
            (3, 200, RaftLogType.Committed),
            (4, 250, RaftLogType.Proposed),
            (5, 300, RaftLogType.Proposed)
        );

        // target = 280: last committed at or before is index 3 (t=200)
        Assert.Equal(3, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(280)));
    }

    // ── CommittedCheckpoint entries are treated as committed ──────────────────────────────

    [Fact]
    public void CommittedCheckpoint_TreatedAsCommitted()
    {
        IWAL wal = BuildWal(
            (1, 100, RaftLogType.CommittedCheckpoint),
            (2, 200, RaftLogType.Committed),
            (3, 300, RaftLogType.Committed)
        );

        Assert.Equal(1, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(50)));
        Assert.Equal(1, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(100)));
    }

    // ── single entry ─────────────────────────────────────────────────────────────────────

    [Fact]
    public void SingleEntry_ExactMatch_BothReturn1()
    {
        IWAL wal = BuildWal((1, 100, RaftLogType.Committed));

        Assert.Equal(1, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(100)));
        Assert.Equal(1, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(100)));
    }

    [Fact]
    public void SingleEntry_TargetBefore_FirstReturns1_LastReturnsMinusOne()
    {
        IWAL wal = BuildWal((1, 100, RaftLogType.Committed));

        Assert.Equal(1, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(50)));
        Assert.Equal(-1, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(50)));
    }

    [Fact]
    public void SingleEntry_TargetAfter_FirstReturnsMinusOne_LastReturns1()
    {
        IWAL wal = BuildWal((1, 100, RaftLogType.Committed));

        Assert.Equal(-1, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(200)));
        Assert.Equal(1, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(200)));
    }

    // ── large log: convergence and correctness ────────────────────────────────────────────

    [Fact]
    public void LargeLog_CorrectlyLocatesTarget()
    {
        // Build 200 committed entries at ticks 100, 200, 300, ..., 20000
        var entries = Enumerable.Range(1, 200)
            .Select(i => ((long)i, (long)(i * 100), RaftLogType.Committed))
            .ToArray();
        IWAL wal = BuildWal(entries);

        // Exact match at middle
        Assert.Equal(50, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(5000)));
        Assert.Equal(50, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(5000)));

        // Gap: target 150 falls between index 1 (t=100) and index 2 (t=200)
        Assert.Equal(2, LogTimeIndex.FirstIndexAtOrAfter(wal, P, T(150)));
        Assert.Equal(1, LogTimeIndex.LastIndexAtOrBefore(wal, P, T(150)));
    }
}
