
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestScriptAsOfReads : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestScriptAsOfReads(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string GetRandomKey() => "k" + Guid.NewGuid().ToString("N")[..9];

    /// <summary>
    /// GET key AS OF T1 returns the value written at T1; GET key returns the latest.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_GetAsOf_ServesHistoricalRevision(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            // Write v1.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v1"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Capture T1 from v1's LastModified.
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entryV1) =
                await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entryV1);
            long snapshotMs = entryV1.LastModified.L;

            // Write v2 — will have LastModified > snapshotMs.
            (setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v2"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Script: GET key AS OF T1 → v1.
            string script = $"""
                LET a = GET "{key}" AS OF {snapshotMs}
                RETURN a
                """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("v1", Encoding.UTF8.GetString(resp.Value ?? []));

            // Script: GET key (no snapshot) → v2.
            script = $"""
                LET b = GET "{key}"
                RETURN b
                """;
            resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("v2", Encoding.UTF8.GetString(resp.Value ?? []));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// GET key AS OF T where T is before the key's first write returns not-found.
    /// Uses Task.Delay to guarantee wall-clock (and HLC L) advances past T before writing.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_GetAsOf_BeforeFirstWrite_NotFound(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            // Capture a timestamp BEFORE writing the key.
            long earlyMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await Task.Delay(10, TestContext.Current.CancellationToken);

            // Write after the captured time.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("value"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Bare GET AS OF (no LET/RETURN): context.Result is set directly to DoesNotExist.
            string script = $"""GET "{key}" AS OF {earlyMs}""";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// BEGIN (snapshot = T) reads two keys both as of T even though one was overwritten after T.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_TransactionSnapshot_AllReadsConsistent(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key1 = GetRandomKey();
            string key2 = GetRandomKey();

            // Write initial values for both keys.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key1, Encoding.UTF8.GetBytes("k1v1"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            (setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key2, Encoding.UTF8.GetBytes("k2v1"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Capture T from key2's LastModified (written last, so >= key1's).
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry2) =
                await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, key2, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry2);
            long snapshotMs = entry2.LastModified.L;

            // Overwrite key1 after T.
            (setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key1, Encoding.UTF8.GetBytes("k1v2"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // A snapshot transaction at T must see k1v1 (not k1v2) for key1.
            // Use COMMIT (not RETURN) so context.Action flips to Commit — BEGIN requires explicit COMMIT.
            string script = $"""
                BEGIN (snapshot = {snapshotMs})
                  GET "{key1}"
                  COMMIT
                END
                """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("k1v1", Encoding.UTF8.GetString(resp.Value ?? []));

            // A snapshot transaction at T must see k2v1 for key2.
            script = $"""
                BEGIN (snapshot = {snapshotMs})
                  GET "{key2}"
                  COMMIT
                END
                """;
            resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("k2v1", Encoding.UTF8.GetString(resp.Value ?? []));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// EXISTS key AS OF T reflects existence at T, not now.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_ExistsAsOf_ReflectsSnapshot(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            // Capture T before the key exists.
            long earlyMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await Task.Delay(10, TestContext.Current.CancellationToken);

            // Write the key.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("x"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Bare EXISTS AS OF (no LET/RETURN) → DoesNotExist.
            string script = $"""EXISTS "{key}" AS OF {earlyMs}""";
            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);

            // Bare EXISTS now → Exists.
            script = $"EXISTS \"{key}\"";
            resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Exists, resp.Type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// GET BY BUCKET prefix AS OF T returns only the members present at T.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_GetByBucketAsOf_ReturnsSnapshotMembers(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix = "bucket/" + GetRandomKey();
            string key1 = prefix + "/a";
            string key2 = prefix + "/b";
            string key3 = prefix + "/c";

            // Write key1 and key2.
            foreach (string k in new[] { key1, key2 })
            {
                (KeyValueResponseType st, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, k, Encoding.UTF8.GetBytes("v"), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, st);
            }

            // Capture T from key2's LastModified.
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry2) =
                await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, key2, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry2);
            long snapshotMs = entry2.LastModified.L;

            // Write key3 after T — delay ensures a different HLC millisecond so it falls outside snapshot.
            await Task.Delay(10, TestContext.Current.CancellationToken);
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key3, Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // GET BY BUCKET at T → should return key1 and key2 only (key3 not yet present).
            // Use a bare statement so the result carries (key, value) pairs with Key populated.
            string script = $"""GET BY BUCKET "{prefix}" AS OF {snapshotMs}""";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.NotNull(resp.Values);
            Assert.Equal(2, resp.Values.Count);
            Assert.All(resp.Values, item => Assert.StartsWith(prefix, item.Key));
            Assert.DoesNotContain(resp.Values, item => item.Key == key3);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A per-statement AS OF T2 inside a snapshot=T1 transaction reads at T2.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_PerStatementAsOf_OverridesTransactionSnapshot(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            // Write v1.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v1"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entryV1) =
                await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entryV1);
            long t1Ms = entryV1.LastModified.L;

            // Write v2 (T2 > T1).
            (setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v2"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            (getType, ReadOnlyKeyValueEntry? entryV2) =
                await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entryV2);
            long t2Ms = entryV2.LastModified.L;

            // Write v3 (after t2Ms).
            (setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v3"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Inside a snapshot=T1 transaction, an explicit AS OF T2 should return v2 (not v1).
            // COMMIT is required — BEGIN requires explicit COMMIT for context.Action to flip.
            string script = $"""
                BEGIN (snapshot = {t1Ms})
                  GET "{key}" AS OF {t2Ms}
                  COMMIT
                END
                """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("v2", Encoding.UTF8.GetString(resp.Value ?? []));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A SET under an active snapshot throws and does not mutate state.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_SnapshotTransaction_RejectsWrites(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            // Establish T.
            long snapshotMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Attempt SET inside snapshot — must fail / abort.
            string script = $"""
                BEGIN (snapshot = {snapshotMs})
                  SET "{key}" "should-not-appear"
                END
                """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);

            // Key must not exist.
            (KeyValueResponseType getType, _) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, getType);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// AS OF 0 is rejected with an error.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_AsOfZero_Rejected(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            string script = $"""
                LET x = GET "{key}" AS OF 0
                RETURN x
                """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// SCAN BY PREFIX prefix AS OF T returns only the members present at T.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_ScanByPrefixAsOf_ReturnsSnapshotMembers(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix = "scan/" + GetRandomKey();
            string key1 = prefix + "/a";
            string key2 = prefix + "/b";
            string key3 = prefix + "/c";

            // Write key1 and key2.
            foreach (string k in new[] { key1, key2 })
            {
                (KeyValueResponseType st, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, k, Encoding.UTF8.GetBytes("v"), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, st);
            }

            // Capture T from key2's LastModified.
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry2) =
                await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, key2, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry2);
            long snapshotMs = entry2.LastModified.L;

            // Write key3 after T — delay ensures a later HLC millisecond so it falls outside snapshot.
            await Task.Delay(10, TestContext.Current.CancellationToken);
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key3, Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // SCAN BY PREFIX at T → key1 and key2 only (key3 not yet present).
            string script = $"""SCAN BY PREFIX "{prefix}" AS OF {snapshotMs}""";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.NotNull(resp.Values);
            Assert.Equal(2, resp.Values.Count);
            Assert.All(resp.Values, item => Assert.StartsWith(prefix, item.Key));
            Assert.DoesNotContain(resp.Values, item => item.Key == key3);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// AT (revision) and AS OF (snapshot) on the same statement is a parse error.
    /// The grammar has no production combining the two selectors, so the script fails to parse.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Script_AtAndAsOf_OnSameStatement_ParseError(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _k2, IKahuna _k3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            string script = $"""
                LET x = GET "{key}" AT 1 AS OF 123
                RETURN x
                """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A script GET AS OF T blocks transparently on a foreign prepared write whose pending
    /// commit ts is ≤ T (safe-time), then returns the committed value once a concurrent commit
    /// resolves the intent — the wait is invisible at the script surface.
    ///
    /// Mirrors the storage-level PreparedWrite_BelowSnapshot scenario, driven through a script.
    /// </summary>
    [Fact]
    public async Task Script_SnapshotRead_WaitsForPendingCommitAtOrBeforeT()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key = "sasof:" + Guid.NewGuid().ToString("N")[..8];
            byte[] valA = "before"u8.ToArray();
            byte[] valB = "after"u8.ToArray();

            // Commit valA to establish a base committed revision.
            (KeyValueResponseType setA, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setA);

            // Open a 2PC transaction and stage valB.
            (KeyValueResponseType startType, HLCTimestamp txId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType setB, _, _) = await kahuna2.LocateAndTrySetKeyValue(
                txId, key, valB, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setB);

            // Prepare: parks the write intent with CommitTimestamp = mvccEntry.LastModified.
            HLCTimestamp commitId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
            (KeyValueResponseType prep, HLCTimestamp ticket, _, _) = await kahuna3.LocateAndTryPrepareMutations(
                txId, commitId, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Prepared, prep);

            // Advance wall clock past CommitTimestamp, then capture T > CommitTimestamp.
            await Task.Delay(10, ct);
            HLCTimestamp T = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            // Commit the parked intent after 50 ms — inside the read's back-off window.
            Task commitTask = Task.Run(async () =>
            {
                await Task.Delay(50, ct);
                await kahuna1.LocateAndTryCommitMutations(txId, key, ticket, KeyValueDurability.Persistent, ct);
            }, ct);

            // Script GET AS OF T: the intent is live with CommitTimestamp ≤ T, so the read waits
            // transparently and returns valB once the concurrent commit lands.
            string script = $"""
                LET a = GET "{key}" AS OF {T.L}
                RETURN a
                """;
            KeyValueTransactionResult resp = await kahuna2.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, null);

            await commitTask;

            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("after", Encoding.UTF8.GetString(resp.Value ?? []));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
