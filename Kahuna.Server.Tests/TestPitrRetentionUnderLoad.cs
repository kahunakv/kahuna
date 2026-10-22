
using System.Text;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The PITR retention floor on a real node under sustained writes with compaction running. Compaction
/// trails the checkpoint by seconds while the protected boundary trails "now" by the window, so right
/// after a pass every retained entry is younger than the boundary. The horizon must then hold the log
/// where it starts rather than clear the floor and let the next pass remove the window too; otherwise
/// no WAL is ever retained for point-in-time recovery on a loaded node, whatever the window says.
/// </summary>
public sealed class TestPitrRetentionUnderLoad
{
    private static readonly TimeSpan PitrWindow = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan BaseSnapshotInterval = TimeSpan.FromSeconds(1);

    private readonly ILoggerFactory loggerFactory;

    public TestPitrRetentionUnderLoad(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task ContinuousWritesWithCompaction_RetainTheWholeWindow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string backupDir = Path.Combine(Path.GetTempPath(), "kahuna-pitr-retention-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(backupDir);

        await using EmbeddedKahunaNode node = new(new()
        {
            InitialPartitions = 1,
            Storage = "memory",
            WalStorage = "memory",
            DirtyObjectsWriterDelay = 100,
            CheckpointInterval = TimeSpan.FromMilliseconds(250),
            CompactEveryOperations = 20,
            PitrWindow = PitrWindow,
            BaseSnapshotInterval = BaseSnapshotInterval,
            // The retention floor is asserted only when backups are configured.
            BackupDir = backupDir
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("pitr/k0", ct);

        IWAL wal = node.Raft.WalAdapter;
        byte[] value = Encoding.UTF8.GetBytes("v");

        // Write for well over window + interval so the boundary moves deep into the written log and
        // several compaction passes and horizon ticks have run.
        long start = Environment.TickCount64;
        int writes = 0;
        while (Environment.TickCount64 - start < 6_000)
        {
            (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"pitr/k{writes++ % 50}", value, null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Set, setType);
        }

        int partitionId = node.Raft.GetPartitionMap()
            .Select(p => p.PartitionId)
            .OrderByDescending(wal.GetMaxLog)
            .First();

        // Compaction must actually have run, or the assertion below proves nothing.
        long floor = wal.GetCompactionFloor(partitionId);
        Assert.True(floor > 1, $"partition {partitionId}: no compaction pass removed anything in 6 s of writes (floor {floor}, max {wal.GetMaxLog(partitionId)})");

        // Every committed entry younger than the boundary is still in the log: the first retained
        // committed entry is at or before the boundary. "now" is read after the floor so a tick that
        // advanced the floor in between can only make the check stricter for the floor, never looser.
        long boundaryMs = (long)(DateTime.UtcNow - PitrWindow - BaseSnapshotInterval - DateTime.UnixEpoch).TotalMilliseconds;
        long firstCommitted = LogTimeIndex.FirstCommittedIndex(wal, partitionId);
        Assert.True(firstCommitted >= floor, $"first committed {firstCommitted} below floor {floor}");

        RaftLog first = Assert.Single(wal.ReadLogsRange(partitionId, firstCommitted, 1));
        Assert.True(first.Time.L <= boundaryMs,
            $"partition {partitionId}: the log starts at index {first.Id} ({first.Time.L} ms), younger than the boundary " +
            $"({boundaryMs} ms): compaction removed entries inside the PITR window (floor {floor}, max {wal.GetMaxLog(partitionId)}, {writes} writes)");
    }
}
