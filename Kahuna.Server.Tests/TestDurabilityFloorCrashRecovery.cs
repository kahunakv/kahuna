using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end reproduction of the fencing-token regression seen under Jepsen partition+kill, on a
/// single persistent node: committed lock/key-value state whose background flush had not run is
/// stranded below a WAL checkpoint, the process dies, and without the application-durability floor
/// restart replay would begin above the checkpoint — silently losing the committed entries and
/// letting a fresh acquisition hand out a lower fencing token than one already granted.
///
/// The checkpoint is forced directly through <c>IRaft.ReplicateCheckpoint</c>, bypassing the
/// background writer's leader-side flush gate — the same durable state a <b>follower</b> is left
/// with when the leader's checkpoint replicates over its unflushed applies (the Jepsen scenario).
/// The flush timer is stretched so no flush runs between the acquisitions and the kill, and
/// disposing the node discards the queued flushes exactly like a process kill would.
///
/// With the durability floor wired (KahunaDurabilityProvider → Kommander), the persisted floor
/// stays below the unflushed entries, restart replay redelivers them, and post-restart operations
/// observe the newest committed state: fencing tokens stay monotonic and reads return the last
/// committed value.
/// </summary>
public sealed class TestDurabilityFloorCrashRecovery
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurabilityFloorCrashRecovery(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        // A stable, non-empty revision so the reconstructed node reopens the SAME database and WAL
        // files (an empty revision defaults to a fresh GUID per construction).
        StorageRevision = "durability-floor",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "durability-floor-wal",
        WalSyncWrites = true,
        // No periodic flush during the test window: every flush is explicit (FlushAsync), so the
        // state left behind at dispose is exactly "committed in the WAL, absent from the backend".
        DirtyObjectsWriterDelay = 600_000
    };

    [Fact]
    public async Task FencingToken_StaysMonotonic_AfterCheckpointThenKillBeforeFlush()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-floor-store-");
        string walPath = CreateTempDir("kahuna-floor-wal-");

        const string resource = "floor/lock1";
        long lastTokenBeforeKill;

        try
        {
            // ── Phase 1: raise the fencing token, flush part of the history, commit more
            // acquisitions above the flush, checkpoint above them, and kill. ──
            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync(resource, ct);

                (LockResponseType r0, long t0) = await node.Kahuna.LocateAndTryLock(resource, "owner-0"u8.ToArray(), 60_000, LockDurability.Persistent, ct);
                Assert.Equal(LockResponseType.Locked, r0);
                Assert.Equal(LockResponseType.Unlocked, await node.Kahuna.LocateAndTryUnlock(resource, "owner-0"u8.ToArray(), LockDurability.Persistent, ct));

                (LockResponseType r1, long t1) = await node.Kahuna.LocateAndTryLock(resource, "owner-1"u8.ToArray(), 60_000, LockDurability.Persistent, ct);
                Assert.Equal(LockResponseType.Locked, r1);
                Assert.True(t1 > t0);
                Assert.Equal(LockResponseType.Unlocked, await node.Kahuna.LocateAndTryUnlock(resource, "owner-1"u8.ToArray(), LockDurability.Persistent, ct));

                // Flush an UNLOCKED state at token t1: a node serving from this stale backend image
                // would happily grant the next acquisition token t1+1 — the Jepsen regression.
                await node.FlushAsync();

                // Two more committed acquisitions ABOVE the flushed state — these live only in the
                // WAL and the in-memory overlay from here on. The final holder's lease is short so
                // phase 2 can obtain a fresh grant and check its token.
                (LockResponseType r2, long t2) = await node.Kahuna.LocateAndTryLock(resource, "owner-2"u8.ToArray(), 60_000, LockDurability.Persistent, ct);
                Assert.Equal(LockResponseType.Locked, r2);
                Assert.Equal(LockResponseType.Unlocked, await node.Kahuna.LocateAndTryUnlock(resource, "owner-2"u8.ToArray(), LockDurability.Persistent, ct));
                (LockResponseType r3, long t3) = await node.Kahuna.LocateAndTryLock(resource, "owner-3"u8.ToArray(), 10_000, LockDurability.Persistent, ct);
                Assert.Equal(LockResponseType.Locked, r3);
                Assert.True(t3 > t2 && t2 > t1);

                lastTokenBeforeKill = t3;

                // Force a durable checkpoint on every partition, above the unflushed lock entries —
                // bypassing the flush gate, exactly the state a follower is left in when the
                // leader's checkpoint replicates over its lagging background flush.
                for (int partitionId = 0; partitionId <= node.Raft.Configuration.InitialPartitions; partitionId++)
                    await node.Raft.ReplicateCheckpoint(partitionId, ct);

                // Dispose without flushing: the queued lock writes above t1 are discarded — a kill.
            }

            // ── Phase 2: restart on the same durable state. Replay must widen below the checkpoint
            // (durability floor) and redeliver the lost acquisitions; the next token must be higher
            // than every token already handed out. ──
            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync(resource, ct);

                // owner-3's replayed 10s lease may still be live right after restart (Busy is the
                // correct answer then); once it lapses the fresh grant MUST carry a token above
                // every one already handed out. A node serving from the stale flushed image would
                // instead grant immediately with token t1+1 — the regression this test pins down.
                LockResponseType r4 = LockResponseType.Busy;
                long t4 = -1;
                long deadline = Environment.TickCount64 + 30_000;

                while (Environment.TickCount64 < deadline)
                {
                    (r4, t4) = await node.Kahuna.LocateAndTryLock(resource, "owner-4"u8.ToArray(), 60_000, LockDurability.Persistent, ct);
                    if (r4 == LockResponseType.Locked)
                        break;
                    Assert.Equal(LockResponseType.Busy, r4);
                    await Task.Delay(250, ct);
                }

                Assert.Equal(LockResponseType.Locked, r4);
                Assert.True(t4 > lastTokenBeforeKill,
                    $"fencing token regressed: granted {t4} after {lastTokenBeforeKill} was already handed out");
            }
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    [Fact]
    public async Task CommittedKeyValue_SurvivesCheckpointThenKillBeforeFlush()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-floor-kv-store-");
        string walPath = CreateTempDir("kahuna-floor-kv-wal-");

        const string key = "floor/kv1";

        try
        {
            // ── Phase 1: write v1 and flush it, write v2 committed-but-unflushed, checkpoint, kill. ──
            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync(key, ct);

                (KeyValueResponseType s1, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v1"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Set, s1);

                await node.FlushAsync();

                (KeyValueResponseType s2, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v2"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Set, s2);

                for (int partitionId = 0; partitionId <= node.Raft.Configuration.InitialPartitions; partitionId++)
                    await node.Raft.ReplicateCheckpoint(partitionId, ct);
            }

            // ── Phase 2: restart; the acknowledged v2 must be readable, not the stale flushed v1. ──
            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync(key, ct);

                (KeyValueResponseType g, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

                Assert.Equal(KeyValueResponseType.Get, g);
                Assert.Equal("v2"u8.ToArray(), entry!.Value);
            }
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }
}
