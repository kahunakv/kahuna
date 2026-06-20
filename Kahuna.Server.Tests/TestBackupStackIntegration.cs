
using System.Text;
using Kahuna.Client;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Integration tests for the P1.11 backup/restore stack end-to-end:
/// KahunaManager → BackupService → BackupDriver / RestoreEngine.
/// Uses EmbeddedKahunaNode so BackupDir is wired through the full
/// IKahuna interface (KahunaManager.IsBackupConfigured, TakeFullBackupAsync,
/// RestoreToAsync, etc.).
/// </summary>
public sealed class TestBackupStackIntegration : IDisposable
{
    private readonly ILoggerFactory loggerFactory;
    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_svc_int_" + Guid.NewGuid().ToString("N"));

    public TestBackupStackIntegration(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
        Directory.CreateDirectory(tempRoot);
    }

    public void Dispose()
    {
        try { Directory.Delete(tempRoot, recursive: true); } catch { /* ignore */ }
    }

    private string BakDir(string tag) => Path.Combine(tempRoot, "bak_" + tag);

    // ── C1: IsBackupConfigured ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task IsBackupConfigured_False_WhenNoDirSet()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(node.Kahuna.IsBackupConfigured);
    }

    [Fact]
    public async Task IsBackupConfigured_True_WhenBackupDirSet()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = BakDir("cfg_true")
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(node.Kahuna.IsBackupConfigured);
    }

    // ── C2: TakeFullBackupAsync flushes dirty data, returns Full DTO ────────────────────────

    [Fact]
    public async Task TakeFullBackupAsync_AfterWritingData_ReturnsDtoTypeFullAndCheckpointExists()
    {
        string bakDir = BakDir("full_e2e");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        // Write a key so there is something to flush.
        (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            Kommander.Time.HLCTimestamp.Zero, "c2/key",
            Encoding.UTF8.GetBytes("value"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, setType);

        KahunaBackupInfo dto = await node.Kahuna.TakeFullBackupAsync(TestContext.Current.CancellationToken);

        Assert.Equal("Full", dto.Type);
        Assert.NotEqual(Guid.Empty, dto.BackupId);
        Assert.True(Directory.Exists(
            Path.Combine(bakDir, dto.BackupId.ToString("N"), "checkpoint")));
    }

    // ── C3: flush hook is actually invoked through the KahunaManager path ─────────────────
    // (P1.11a's acceptance criterion: "flush hook actually invoked")

    [Fact]
    public async Task TakeFullBackupAsync_FlushHookActuallyInvoked_DataAppearsInCheckpoint()
    {
        string bakDir = BakDir("flush_hook");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        // Write a key via IKahuna (goes through actor → persistence pipeline).
        (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            Kommander.Time.HLCTimestamp.Zero, "flush_probe",
            Encoding.UTF8.GetBytes("probe_val"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, setType);

        KahunaBackupInfo dto = await node.Kahuna.TakeFullBackupAsync(TestContext.Current.CancellationToken);

        // The flush hook (KahunaManager.FlushPersistenceAsync) must drain the actor
        // pipeline before CreateCheckpoint. If it ran, the key is in the checkpoint.
        string checkpointDir = Path.Combine(bakDir, dto.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointDir);
        Assert.NotNull(restored.GetKeyValue("flush_probe"));
    }

    // ── C4: incremental backup after full has parent ID ──────────────────────────────────────

    [Fact]
    public async Task TakeIncrementalBackupAsync_AfterFull_HasCorrectParentId()
    {
        string bakDir = BakDir("inc");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaBackupInfo full = await node.Kahuna.TakeFullBackupAsync(TestContext.Current.CancellationToken);

        // Write a key so the incremental has something to cover.
        await node.Kahuna.LocateAndTrySetKeyValue(
            Kommander.Time.HLCTimestamp.Zero, "inc/key",
            Encoding.UTF8.GetBytes("v2"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        KahunaBackupInfo inc = await node.Kahuna.TakeIncrementalBackupAsync(full.BackupId, TestContext.Current.CancellationToken);

        Assert.Equal("Incremental", inc.Type);
        Assert.Equal(full.BackupId, inc.ParentBackupId);
    }

    // ── C5: list + chain ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ListBackupsAsync_ReturnsAllTakenBackups()
    {
        string bakDir = BakDir("list");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaBackupInfo full = await node.Kahuna.TakeFullBackupAsync(TestContext.Current.CancellationToken);
        KahunaBackupInfo inc = await node.Kahuna.TakeIncrementalBackupAsync(full.BackupId, TestContext.Current.CancellationToken);

        IReadOnlyList<KahunaBackupInfo> all = await node.Kahuna.ListBackupsAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, all.Count);
        Assert.Contains(all, b => b.BackupId == full.BackupId);
        Assert.Contains(all, b => b.BackupId == inc.BackupId);
    }

    [Fact]
    public async Task GetBackupChainAsync_ChainOrderFullFirst()
    {
        string bakDir = BakDir("chain");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaBackupInfo full = await node.Kahuna.TakeFullBackupAsync(TestContext.Current.CancellationToken);
        KahunaBackupInfo inc = await node.Kahuna.TakeIncrementalBackupAsync(full.BackupId, TestContext.Current.CancellationToken);

        IReadOnlyList<KahunaBackupInfo> chain = await node.Kahuna.GetBackupChainAsync(inc.BackupId, TestContext.Current.CancellationToken);
        Assert.Equal(2, chain.Count);
        Assert.Equal("Full", chain[0].Type);
        Assert.Equal("Incremental", chain[1].Type);
    }

    // ── C6: offline restore ───────────────────────────────────────────────────────────────────
    // This is the P1.11f accept criterion: "restore to a T into a target dir against a running test server"

    [Fact]
    public async Task RestoreToAsync_AfterWritingData_RestoredDirContainsKey()
    {
        string bakDir = BakDir("restore_e2e");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string key = "restore/probe";
        (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            Kommander.Time.HLCTimestamp.Zero, key,
            Encoding.UTF8.GetBytes("restored_val"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, setType);

        KahunaBackupInfo full = await node.Kahuna.TakeFullBackupAsync(TestContext.Current.CancellationToken);

        string targetDir = Path.Combine(tempRoot, "restored_e2e");
        KahunaRestoreResponse result = await node.Kahuna.RestoreToAsync(
            full.BackupId, targetDir, targetTimeMs: 0,
            TestContext.Current.CancellationToken);

        Assert.Equal(targetDir, result.TargetDir);
        Assert.Single(result.Chain);
        Assert.True(Directory.Exists(targetDir));

        // The restored directory must contain the written key.
        MemoryPersistenceBackend check = MemoryPersistenceBackend.OpenCheckpoint(targetDir);
        Assert.NotNull(check.GetKeyValue(key));
    }

    // ── C6b: regression — Zero targetTimeMs must not drop incrementals ───────────────────────
    // Before the fix, targetTime = Zero was passed verbatim to RestoreEngine whose stop-predicate
    // (entry.Time > Zero) broke immediately, silently discarding all incremental WAL entries.

    [Fact]
    public async Task RestoreToAsync_ZeroTarget_AppliesIncrementals()
    {
        string bakDir = BakDir("zero_target");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string key1 = "zero/pre";
        const string key2 = "zero/post";

        // Write key1 before the full backup → appears in checkpoint.
        await node.Kahuna.LocateAndTrySetKeyValue(
            Kommander.Time.HLCTimestamp.Zero, key1,
            Encoding.UTF8.GetBytes("before"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        KahunaBackupInfo full = await node.Kahuna.TakeFullBackupAsync(TestContext.Current.CancellationToken);

        // Write key2 AFTER the full backup → only in incremental WAL.
        await node.Kahuna.LocateAndTrySetKeyValue(
            Kommander.Time.HLCTimestamp.Zero, key2,
            Encoding.UTF8.GetBytes("after"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        KahunaBackupInfo inc = await node.Kahuna.TakeIncrementalBackupAsync(full.BackupId, TestContext.Current.CancellationToken);

        string targetDir = Path.Combine(tempRoot, "zero_target_out");

        // targetTimeMs = 0 → must restore to chain tip, not drop the incremental.
        KahunaRestoreResponse result = await node.Kahuna.RestoreToAsync(
            inc.BackupId, targetDir, targetTimeMs: 0,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Chain.Count);

        MemoryPersistenceBackend check = MemoryPersistenceBackend.OpenCheckpoint(targetDir);

        // key1 always present (from checkpoint); key2 only present if incrementals were applied.
        Assert.NotNull(check.GetKeyValue(key1));
        Assert.NotNull(check.GetKeyValue(key2));
    }

    // ── C7: not configured → throws ────────────────────────────────────────────────────────

    [Fact]
    public async Task TakeFullBackupAsync_WhenNotConfigured_ThrowsInvalidOperation()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
            // BackupDir NOT set
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => node.Kahuna.TakeFullBackupAsync(TestContext.Current.CancellationToken));
    }

    // ── C8: client-level backup via IKahuna via InProcessCommunication ─────────────────────

    [Fact]
    public async Task KahunaClient_TakeFullBackup_ReturnsDtoTypeFullViaInProcess()
    {
        string bakDir = BakDir("client");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        InProcessKahunaCommunication comm = new(node.Kahuna);
        KahunaClient client = new("http://localhost", communication: comm);

        KahunaBackupInfo dto = await client.TakeFullBackupAsync(TestContext.Current.CancellationToken);
        Assert.Equal("Full", dto.Type);
    }

    [Fact]
    public async Task KahunaClient_ListBackups_RoundTripsViaInProcess()
    {
        string bakDir = BakDir("client_list");
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            BackupDir = bakDir
        }, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        InProcessKahunaCommunication comm = new(node.Kahuna);
        KahunaClient client = new("http://localhost", communication: comm);

        await client.TakeFullBackupAsync(TestContext.Current.CancellationToken);
        List<KahunaBackupInfo> all = await client.ListBackupsAsync(TestContext.Current.CancellationToken);
        Assert.Single(all);
        Assert.Equal("Full", all[0].Type);
    }
}
