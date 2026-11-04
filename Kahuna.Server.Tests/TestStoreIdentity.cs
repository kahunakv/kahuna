using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The store identity check that runs at node construction, and the durability-floor answer an empty
/// store gives. A RocksDB store is stamped with the node's Raft endpoint the first time the node opens it;
/// a store carrying another node's stamp is refused, a store written before the stamp existed is stamped
/// and used, and a store found empty while the Raft log holds history is reported as replaced. For such a
/// store the durability provider answers 0 for every data partition — the contract that lets Kommander
/// refuse to replay a compacted suffix onto it — while the system partition keeps "no opinion".
/// </summary>
public sealed class TestStoreIdentity
{
    private const string Revision = "v1";

    private static readonly RaftPartitionRange[] Partitions =
        [new RaftPartitionRange { PartitionId = 1, State = RaftPartitionState.Active }];

    [Fact]
    public void FreshStore_IsStamped_AndReopensUnderTheSameNode()
    {
        string dir = CreateTempDir("kahuna-identity-fresh-");
        try
        {
            TestBackupService.StubRaft raft = new(new InMemoryWAL(NullLogger<IRaft>.Instance), Partitions);

            using (RocksDbPersistenceBackend backend = new(dir, Revision))
            {
                Assert.True(backend.SupportsStoreIdentity);
                Assert.Null(backend.GetStoreIdentity());
                Assert.False(backend.HoldsDurableState());

                Assert.Null(StoreIdentityGuard.Verify(backend, raft, dir, NullLogger.Instance));

                PersistentStoreIdentity? identity = backend.GetStoreIdentity();
                Assert.NotNull(identity);
                Assert.Equal(raft.GetLocalEndpoint(), identity.NodeEndpoint);
            }

            using (RocksDbPersistenceBackend reopened = new(dir, Revision))
            {
                PersistentStoreIdentity? before = reopened.GetStoreIdentity();
                Assert.Null(StoreIdentityGuard.Verify(reopened, raft, dir, NullLogger.Instance));
                Assert.Equal(before, reopened.GetStoreIdentity());
            }
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void StoreOfAnotherNode_IsRefused()
    {
        string dir = CreateTempDir("kahuna-identity-foreign-");
        try
        {
            TestBackupService.StubRaft raft = new(new InMemoryWAL(NullLogger<IRaft>.Instance), Partitions);

            using RocksDbPersistenceBackend backend = new(dir, Revision);
            Assert.True(backend.WriteStoreIdentity(PersistentStoreIdentity.Create("other-host:7000")));
            Assert.True(backend.StoreKeyValues([MakeItem("foreign/k", 1)]));

            StoreIdentityMismatchException thrown = Assert.Throws<StoreIdentityMismatchException>(
                () => StoreIdentityGuard.Verify(backend, raft, dir, NullLogger.Instance));

            Assert.Contains("other-host:7000", thrown.Message, StringComparison.Ordinal);
            Assert.Contains(raft.GetLocalEndpoint(), thrown.Message, StringComparison.Ordinal);

            // Refused, not rewritten: the foreign stamp stays for the operator to see.
            Assert.Equal("other-host:7000", backend.GetStoreIdentity()?.NodeEndpoint);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void StoreWrittenBeforeTheIdentityExisted_IsStampedAndUsed()
    {
        string dir = CreateTempDir("kahuna-identity-legacy-");
        try
        {
            InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
            WriteHistory(wal);
            TestBackupService.StubRaft raft = new(wal, Partitions);

            using RocksDbPersistenceBackend backend = new(dir, Revision);
            Assert.True(backend.StoreKeyValues([MakeItem("legacy/k", 1)]));
            Assert.True(backend.StoreDurabilityFloors([(1, 3L)]));
            Assert.True(backend.HoldsDurableState());

            // Rows and floors with no stamp: an upgrade, not a wipe, even though the log holds history.
            Assert.Null(StoreIdentityGuard.Verify(backend, raft, dir, NullLogger.Instance));
            Assert.Equal(raft.GetLocalEndpoint(), backend.GetStoreIdentity()?.NodeEndpoint);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void FloorsAloneCountAsDurableState()
    {
        string dir = CreateTempDir("kahuna-identity-floors-");
        try
        {
            using RocksDbPersistenceBackend backend = new(dir, Revision);
            Assert.False(backend.HoldsDurableState());
            Assert.True(backend.StoreDurabilityFloors([(2, 1L)]));
            Assert.True(backend.HoldsDurableState());
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void EmptyStoreUnderALogWithHistory_IsReportedAsReplaced_AndAnswersFloorZero()
    {
        string dir = CreateTempDir("kahuna-identity-wiped-");
        try
        {
            InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
            WriteHistory(wal);
            TestBackupService.StubRaft raft = new(wal, Partitions);

            using RocksDbPersistenceBackend backend = new(dir, Revision);

            string? notice = StoreIdentityGuard.Verify(backend, raft, dir, NullLogger.Instance);

            Assert.NotNull(notice);
            Assert.Contains("lost or replaced", notice, StringComparison.Ordinal);
            Assert.Equal(raft.GetLocalEndpoint(), backend.GetStoreIdentity()?.NodeEndpoint);

            // The backend itself reports "none recorded"; the provider turns that into the contract's
            // "nothing durably applied" for a data partition and keeps "no opinion" for the system partition,
            // whose consumer entries are not floor-tracked.
            Assert.Equal(-1L, backend.GetDurabilityFloor(1));
            Assert.True(backend.KeepsDurabilityFloors);

            KahunaDurabilityProvider provider = new(new PartitionDurabilityTracker(), backend);
            Assert.Equal(0L, provider.GetDurablyAppliedIndex(1));
            Assert.Equal(0L, provider.GetDurablyAppliedIndex(7));
            Assert.Equal(-1L, provider.GetDurablyAppliedIndex(RaftSystemConfig.SystemPartition));

            // A recorded floor is answered as is.
            Assert.True(backend.StoreDurabilityFloors([(3, 42L)]));
            Assert.Equal(42L, new KahunaDurabilityProvider(new PartitionDurabilityTracker(), backend).GetDurablyAppliedIndex(3));

            // Once stamped, a restart no longer reports the store as replaced; the floors keep saying it is empty.
            Assert.Null(StoreIdentityGuard.Verify(backend, raft, dir, NullLogger.Instance));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void BackendWithoutFloors_KeepsNoOpinion()
    {
        IPersistenceBackend backend = new MemoryPersistenceBackend();
        Assert.False(backend.KeepsDurabilityFloors);
        Assert.False(backend.SupportsStoreIdentity);

        KahunaDurabilityProvider provider = new(new PartitionDurabilityTracker(), backend);
        Assert.Equal(-1L, provider.GetDurablyAppliedIndex(1));

        // Nothing to check on a backend that keeps no identity: the guard is inert, whatever the log holds.
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        WriteHistory(wal);
        Assert.Null(StoreIdentityGuard.Verify(backend, new TestBackupService.StubRaft(wal, Partitions), "", NullLogger.Instance));
    }

    [Fact]
    public void Identity_RoundTripsThroughItsEncoding_AndRejectsDamage()
    {
        PersistentStoreIdentity identity = PersistentStoreIdentity.Create("node-a.example:8084");
        byte[] encoded = identity.Encode();

        Assert.True(PersistentStoreIdentity.TryDecode(encoded, out PersistentStoreIdentity? decoded));
        Assert.Equal(identity, decoded);

        Assert.False(PersistentStoreIdentity.TryDecode(encoded.AsSpan(0, encoded.Length - 1), out _));
        Assert.False(PersistentStoreIdentity.TryDecode([.. encoded, 0], out _));
        Assert.False(PersistentStoreIdentity.TryDecode(Encoding.UTF8.GetBytes("garbage"), out _));
        Assert.False(PersistentStoreIdentity.TryDecode([], out _));

        byte[] badMagic = (byte[])encoded.Clone();
        badMagic[0] ^= 0xFF;
        Assert.False(PersistentStoreIdentity.TryDecode(badMagic, out _));
    }

    [Fact]
    public void CheckpointImage_CarriesNoIdentity()
    {
        string dir = CreateTempDir("kahuna-identity-checkpoint-");
        string checkpoint = Path.Combine(dir, "image");
        try
        {
            using (RocksDbPersistenceBackend backend = new(dir, Revision))
            {
                Assert.True(backend.WriteStoreIdentity(PersistentStoreIdentity.Create("localhost:1")));
                Assert.True(backend.StoreKeyValues([MakeItem("image/k", 1)]));
                backend.CreateCheckpointAsOf(checkpoint, 10, new Kommander.Time.HLCTimestamp(0, long.MaxValue, 0), TestContext.Current.CancellationToken);
            }

            // Opened as a restored store: the rows are there, the source node's stamp is not, so the node that
            // restores it stamps its own instead of refusing it as foreign.
            using RocksDbPersistenceBackend restored = new(Path.GetDirectoryName(checkpoint)!, Path.GetFileName(checkpoint));
            Assert.NotNull(restored.GetKeyValue("image/k"));
            Assert.Null(restored.GetStoreIdentity());
            Assert.True(restored.HoldsDurableState());
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    // ── Scaffolding ──────────────────────────────────────────────────────────────────────

    /// <summary>One committed entry on the system partition: the witness that the node has run in a cluster before.</summary>
    private static void WriteHistory(InMemoryWAL wal)
    {
        RaftLog entry = new()
        {
            Id = 1,
            Term = 1,
            Type = RaftLogType.Committed,
            LogType = "test",
            LogData = [],
        };

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(RaftSystemConfig.SystemPartition, [entry])]));
        Assert.True(((IWAL)wal).GetMaxLog(RaftSystemConfig.SystemPartition) > 0);
    }

    private static PersistenceRequestItem MakeItem(string key, long revision) =>
        new(key,
            Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

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
