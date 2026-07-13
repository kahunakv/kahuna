using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Validates the log-semantics prerequisite for anchoring durable transaction records on their data
/// partition: on a single partition an independently committed mutation must apply <b>without</b>
/// forcing an earlier, still-uncommitted prepared mutation to apply, and the committed mutation must be
/// durable across a cold restart while the pending prepare — which lives only in the leader's actor
/// memory — is not.
///
/// <para>Kahuna prepare uses uncommitted Raft proposal tickets (<c>ReplicateLogs(autoCommit: false)</c>);
/// commit/rollback settle a single ticket by its id. Because commit is per-proposal rather than a
/// monotonic log-index prefix, committing a later mutation does not drag an earlier pending ticket into
/// the state machine. A durable transaction record therefore can share a partition's Raft/WAL stream
/// with unrelated in-flight prepares: later record deltas do not disturb them, and an uncommitted
/// prepare does not surface as applied state.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestMixedManualAutomaticLog
{
    private readonly ILoggerFactory loggerFactory;

    public TestMixedManualAutomaticLog(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaOptions InMemoryOptions() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    };

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath) => new()
    {
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "mixed-log",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "mixed-log-wal",
        InitialPartitions = 1,
        DirtyObjectsWriterDelay = 60000
    };

    // Stages a persistent transactional write and proposes it (a manual, uncommitted proposal ticket).
    // The value is NOT visible to a latest read until the ticket is committed.
    private static async Task<HLCTimestamp> PreparePending(
        EmbeddedKahunaNode node, HLCTimestamp txId, HLCTimestamp commitId, string key, byte[] value)
    {
        (KeyValueResponseType setType, _, _) = await node.Kahuna.TrySetKeyValue(
            txId, key, value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent);
        Assert.Equal(KeyValueResponseType.Set, setType);

        (KeyValueResponseType prepType, HLCTimestamp ticket, _, _) = await node.Kahuna.TryPrepareMutations(
            txId, commitId, key, KeyValueDurability.Persistent, recordAnchorKey: key);
        Assert.Equal(KeyValueResponseType.Prepared, prepType);
        return ticket;
    }

    private static async Task<KeyValueResponseType> Commit(
        EmbeddedKahunaNode node, HLCTimestamp txId, string key, HLCTimestamp ticket)
    {
        (KeyValueResponseType type, _) = await node.Kahuna.TryCommitMutations(
            txId, key, ticket, KeyValueDurability.Persistent);
        return type;
    }

    private static async Task<KeyValueResponseType> Rollback(
        EmbeddedKahunaNode node, HLCTimestamp txId, string key, HLCTimestamp ticket)
    {
        (KeyValueResponseType type, _) = await node.Kahuna.TryRollbackMutations(
            txId, key, ticket, KeyValueDurability.Persistent);
        return type;
    }

    // Latest committed read (no transaction, no snapshot) through the locator, which routes to the
    // partition leader and waits for it — the external read path.
    private static async Task<(KeyValueResponseType, string?)> Read(EmbeddedKahunaNode node, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        return (type, entry?.Value is null ? null : Encoding.UTF8.GetString(entry.Value));
    }

    // ── The gate: a committed mutation does not force-apply an earlier pending prepare ─────────────

    /// <summary>
    /// Prepare A (left uncommitted) proposes a manual ticket at some log index; committing an
    /// independent mutation B at a higher index applies B alone. A stays invisible until it is
    /// committed on its own ticket, and that commit is idempotent.
    /// </summary>
    [Fact]
    public async Task CommittedMutation_DoesNotForceApply_AnEarlierPendingPreparedMutation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(), loggerFactory);
        await node.StartAsync(ct);

        var clock = node.Raft.HybridLogicalClock;
        int nodeId = node.Raft.GetLocalNodeId();

        const string keyA = "mixedlog/a";
        const string keyB = "mixedlog/b";
        byte[] valueA = "value-a"u8.ToArray();
        byte[] valueB = "value-b"u8.ToArray();

        // A: prepared, left uncommitted (earlier log index).
        HLCTimestamp txA = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp commitA = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp ticketA = await PreparePending(node, txA, commitA, keyA, valueA);

        // The pending prepare is not visible to a latest read.
        (KeyValueResponseType beforeA, _) = await Read(node, keyA, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, beforeA);

        // B: prepared and committed (higher log index than A's still-pending ticket).
        HLCTimestamp txB = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp commitB = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp ticketB = await PreparePending(node, txB, commitB, keyB, valueB);
        Assert.Equal(KeyValueResponseType.Committed, await Commit(node, txB, keyB, ticketB));

        // B applied.
        (KeyValueResponseType readB, string? valB) = await Read(node, keyB, ct);
        Assert.Equal(KeyValueResponseType.Get, readB);
        Assert.Equal("value-b", valB);

        // The gate: committing B did NOT drag the earlier, lower-indexed pending A into the state
        // machine. A is still invisible.
        (KeyValueResponseType afterB, _) = await Read(node, keyA, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, afterB);

        // A applies only on its own commit.
        Assert.Equal(KeyValueResponseType.Committed, await Commit(node, txA, keyA, ticketA));
        (KeyValueResponseType readA, string? valA) = await Read(node, keyA, ct);
        Assert.Equal(KeyValueResponseType.Get, readA);
        Assert.Equal("value-a", valA);

        // Committing the same ticket again is an idempotent no-op — exactly once.
        Assert.Equal(KeyValueResponseType.Committed, await Commit(node, txA, keyA, ticketA));
        (KeyValueResponseType readAgain, string? valAgain) = await Read(node, keyA, ct);
        Assert.Equal(KeyValueResponseType.Get, readAgain);
        Assert.Equal("value-a", valAgain);
    }

    // ── Multiple pending tickets resolve independently, each exactly once ──────────────────────────

    /// <summary>
    /// Two prepared-but-uncommitted mutations coexist with an independently committed one; each pending
    /// ticket is then settled on its own — one committed, one rolled back — and the settled outcome
    /// wins over a later opposite request.
    /// </summary>
    [Fact]
    public async Task MultiplePendingPreparedMutations_ResolveIndependently_ExactlyOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(), loggerFactory);
        await node.StartAsync(ct);

        var clock = node.Raft.HybridLogicalClock;
        int nodeId = node.Raft.GetLocalNodeId();

        const string keyOne = "mixedlog/one";
        const string keyTwo = "mixedlog/two";
        const string keyMid = "mixedlog/mid";

        // Two pending prepares.
        HLCTimestamp txOne = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp commitOne = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp ticketOne = await PreparePending(node, txOne, commitOne, keyOne, "one"u8.ToArray());

        HLCTimestamp txTwo = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp commitTwo = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp ticketTwo = await PreparePending(node, txTwo, commitTwo, keyTwo, "two"u8.ToArray());

        // An independent mutation commits in between.
        HLCTimestamp txMid = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp commitMid = clock.TrySendOrLocalEvent(nodeId);
        HLCTimestamp ticketMid = await PreparePending(node, txMid, commitMid, keyMid, "mid"u8.ToArray());
        Assert.Equal(KeyValueResponseType.Committed, await Commit(node, txMid, keyMid, ticketMid));

        Assert.Equal(KeyValueResponseType.Get, (await Read(node, keyMid, ct)).Item1);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(node, keyOne, ct)).Item1);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(node, keyTwo, ct)).Item1);

        // Settle ticket one with commit (idempotent).
        Assert.Equal(KeyValueResponseType.Committed, await Commit(node, txOne, keyOne, ticketOne));
        Assert.Equal(KeyValueResponseType.Committed, await Commit(node, txOne, keyOne, ticketOne));
        Assert.Equal("one", (await Read(node, keyOne, ct)).Item2);

        // Settle ticket two with rollback — it never becomes visible.
        Assert.Equal(KeyValueResponseType.RolledBack, await Rollback(node, txTwo, keyTwo, ticketTwo));
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(node, keyTwo, ct)).Item1);

        // A commit after the rollback cannot resurrect it: the settled outcome wins, and the key stays
        // invisible.
        KeyValueResponseType commitAfterRollback = await Commit(node, txTwo, keyTwo, ticketTwo);
        Assert.NotEqual(KeyValueResponseType.Committed, commitAfterRollback);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(node, keyTwo, ct)).Item1);
    }

    // ── Committed mutation is durable across cold restart; pending prepare is memory-only ──────────

    /// <summary>
    /// With a persistent WAL, a committed mutation replays on cold restart while a prepared-but-
    /// uncommitted mutation — whose write intent and MVCC entry live only in the leader's actor memory —
    /// does not surface as applied state and cannot be committed on the fresh node (its prepare is gone).
    /// This is the deliberate participant-prepare boundary: a lost prepare is a retry, never a silent
    /// commit.
    /// </summary>
    [Fact]
    public async Task CommittedMutation_SurvivesColdRestart_WhilePendingPrepareIsMemoryOnly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-mixedlog-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-mixedlog-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            const string keyA = "mixedlog/restart-a";
            const string keyB = "mixedlog/restart-b";

            HLCTimestamp txA;
            HLCTimestamp ticketA;

            await using (EmbeddedKahunaNode first = new(PersistentOptions(storagePath, walPath), loggerFactory))
            {
                await first.StartAsync(ct);

                var clock = first.Raft.HybridLogicalClock;
                int nodeId = first.Raft.GetLocalNodeId();

                // A: prepared, left uncommitted.
                txA = clock.TrySendOrLocalEvent(nodeId);
                HLCTimestamp commitA = clock.TrySendOrLocalEvent(nodeId);
                ticketA = await PreparePending(first, txA, commitA, keyA, "value-a"u8.ToArray());

                // B: committed.
                HLCTimestamp txB = clock.TrySendOrLocalEvent(nodeId);
                HLCTimestamp commitB = clock.TrySendOrLocalEvent(nodeId);
                HLCTimestamp ticketB = await PreparePending(first, txB, commitB, keyB, "value-b"u8.ToArray());
                Assert.Equal(KeyValueResponseType.Committed, await Commit(first, txB, keyB, ticketB));

                Assert.Equal("value-b", (await Read(first, keyB, ct)).Item2);
                Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(first, keyA, ct)).Item1);

                // Checkpoint committed state to the backend. B is a committed mutation and is persisted;
                // A is an uncommitted prepare (a write intent, not committed MVCC) and is not.
                await first.FlushAsync();
            }

            // Cold restart over the same durable WAL + storage.
            await using EmbeddedKahunaNode second = new(PersistentOptions(storagePath, walPath), loggerFactory);
            await second.StartAsync(ct);

            // B survived cold replay.
            long deadline = Environment.TickCount64 + 10_000;
            (KeyValueResponseType typeB, string? valB) = await Read(second, keyB, ct);
            while (typeB != KeyValueResponseType.Get && Environment.TickCount64 < deadline)
            {
                await Task.Delay(50, ct);
                (typeB, valB) = await Read(second, keyB, ct);
            }
            Assert.Equal(KeyValueResponseType.Get, typeB);
            Assert.Equal("value-b", valB);

            // The pending prepare A was memory-only: its uncommitted entry did not apply, so A is still
            // invisible after restart.
            Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(second, keyA, ct)).Item1);

            // A's prepare state is gone on the fresh node — a re-commit of the stale ticket cannot commit
            // it (there is no write intent to commit); it is a retry, not a silent commit.
            KeyValueResponseType recommitA = await Commit(second, txA, keyA, ticketA);
            Assert.NotEqual(KeyValueResponseType.Committed, recommitA);
            Assert.Equal(KeyValueResponseType.DoesNotExist, (await Read(second, keyA, ct)).Item1);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }
}
