
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;
using Kommander.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The range-map store installs each committed meta entry at most once and never lets an older
/// entry overwrite a newer one. Two writers reach the in-memory map: the leader installs its own
/// entry as soon as the proposal commits, and the apply path installs every committed entry again
/// when the log applicator delivers it (the leader echo, or a follower apply). The echo can arrive
/// after a later mutation already installed a newer map; applying it then would regress the map and
/// the next mutation would rewrite the whole map from that stale base, discarding every change
/// committed in between.
/// </summary>
public sealed class TestRangeMapStoreApplyOrder : IDisposable
{
    private readonly string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-rangemap-order-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(storagePath))
                Directory.Delete(storagePath, recursive: true);
        }
        catch
        {
            // Best effort temp cleanup.
        }
    }

    private static RangeMapStore NewStore(ProposeOutcomeStubRaft raft, string? path = null) =>
        new(raft, path, path is null ? null : "n1", NullLogger<IKahuna>.Instance, checkpointEveryMutations: 0);

    private static RangeDescriptor FullRange(string keySpace, int partitionId) => new()
    {
        KeySpace = keySpace,
        StartKey = null,
        EndKey = null,
        PartitionId = partitionId,
        Generation = 1
    };

    private static RaftLog CommittedEntry(long logId, byte[] payload) => new()
    {
        Id = logId,
        Term = 2,
        Type = RaftLogType.Committed,
        LogType = ReplicationTypes.RangeMap,
        LogData = payload
    };

    private static byte[] Encode(RangeMap map)
    {
        // Round-trip through a scratch store so the payload is produced by the same codec the
        // leader uses when it proposes.
        ProposeOutcomeStubRaft raft = new();
        using RangeMapStore scratch = NewStore(raft);
        Assert.True(scratch.MutateMapAsync(_ => map).GetAwaiter().GetResult());
        return raft.ProposedPayloads[^1];
    }

    [Fact]
    public async Task LateEchoOfOlderEntry_DoesNotRegressNewerMap()
    {
        ProposeOutcomeStubRaft raft = new();
        using RangeMapStore store = NewStore(raft);

        Assert.True(await store.MutateAsync(current => [.. current, FullRange("ks0", 2)], TestContext.Current.CancellationToken));
        Assert.True(await store.MutateAsync(current => [.. current, FullRange("ks1", 3)], TestContext.Current.CancellationToken));

        long versionBefore = store.MapVersion;

        // The applicator delivers the first entry only now, after the second one was installed.
        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(1, raft.ProposedPayloads[0])));

        Assert.Equal(versionBefore, store.MapVersion);
        Assert.Equal(2, store.Current.Find("ks0", "x")!.PartitionId);
        Assert.Equal(3, store.Current.Find("ks1", "x")!.PartitionId);

        // The next mutation must build on the newest map, so the committed payload carries both ranges.
        Assert.True(await store.MutateAsync(current => [.. current, FullRange("ks2", 4)], TestContext.Current.CancellationToken));

        RangeMap committed = DecodeLast(raft);
        Assert.Equal(2, committed.Find("ks0", "x")!.PartitionId);
        Assert.Equal(3, committed.Find("ks1", "x")!.PartitionId);
        Assert.Equal(4, committed.Find("ks2", "x")!.PartitionId);
    }

    [Fact]
    public async Task LateEchoBeforeRetirement_KeepsPartitionRetired()
    {
        ProposeOutcomeStubRaft raft = new();
        using RangeMapStore store = NewStore(raft);

        Assert.True(await store.MutateAsync(current => [.. current, FullRange("ks0", 2)], TestContext.Current.CancellationToken));
        Assert.True(await store.MutateAsync(current => [.. current, FullRange("ks1", 5)], TestContext.Current.CancellationToken));

        // The merge cutover drops the range on partition 5 and records the partition as retired.
        Assert.True(await store.MutateMapAsync(current =>
            RangeMap.WithRetired([.. current.Descriptors.Where(d => d.PartitionId != 5)], current.RetiredPartitionIds, 5),
            TestContext.Current.CancellationToken));

        Assert.True(store.Current.IsRetired(5));

        // The echo of the pre-retirement entry arrives late.
        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(2, raft.ProposedPayloads[1])));

        Assert.True(store.Current.IsRetired(5));
        Assert.Null(store.Current.Find("ks1", "x"));

        // A mutation that routes to the retired partition again is still refused: the retirement was
        // not lost to the stale echo.
        Assert.False(await store.MutateAsync(current => [.. current, FullRange("ks1", 5)], TestContext.Current.CancellationToken));
        Assert.Equal(3, raft.ProposeCalls);
    }

    [Fact]
    public async Task EchoOfOwnEntry_IsIdempotent()
    {
        ProposeOutcomeStubRaft raft = new();
        using RangeMapStore store = NewStore(raft);

        Assert.True(await store.MutateAsync(current => [.. current, FullRange("ks0", 2)], TestContext.Current.CancellationToken));

        long versionBefore = store.MapVersion;

        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(1, raft.ProposedPayloads[0])));

        Assert.Equal(versionBefore, store.MapVersion);
        Assert.Equal(2, store.Current.Find("ks0", "x")!.PartitionId);
    }

    [Fact]
    public void FollowerApply_LowerIndexAfterHigherIsIgnored()
    {
        ProposeOutcomeStubRaft raft = new();
        using RangeMapStore store = NewStore(raft);

        byte[] newer = Encode(new RangeMap([FullRange("ks0", 2), FullRange("ks1", 3)]));
        byte[] older = Encode(new RangeMap([FullRange("ks0", 2)]));
        byte[] newest = Encode(new RangeMap([FullRange("ks0", 2), FullRange("ks1", 3), FullRange("ks2", 4)]));

        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(7, newer)));
        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(6, older)));

        Assert.Equal(3, store.Current.Find("ks1", "x")!.PartitionId);

        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(8, newest)));

        Assert.Equal(4, store.Current.Find("ks2", "x")!.PartitionId);
    }

    [Fact]
    public void RestoreReplay_AscendingEntriesConvergeOnTheLast()
    {
        ProposeOutcomeStubRaft raft = new();
        using RangeMapStore store = NewStore(raft);

        byte[] first = Encode(new RangeMap([FullRange("ks0", 2)]));
        byte[] second = Encode(new RangeMap([FullRange("ks0", 2), FullRange("ks1", 3)]));

        Assert.True(store.Restore(RangeMapStore.MetaPartitionId, CommittedEntry(1, first)));
        Assert.True(store.Restore(RangeMapStore.MetaPartitionId, CommittedEntry(2, second)));

        Assert.Equal(2, store.Current.Find("ks0", "x")!.PartitionId);
        Assert.Equal(3, store.Current.Find("ks1", "x")!.PartitionId);
    }

    [Fact]
    public async Task LateEcho_DoesNotRegressTheDurableSnapshot()
    {
        Directory.CreateDirectory(storagePath);

        ProposeOutcomeStubRaft raft = new();

        using (RangeMapStore store = NewStore(raft, storagePath))
        {
            Assert.True(await store.MutateAsync(current => [.. current, FullRange("ks0", 2)], TestContext.Current.CancellationToken));
            Assert.True(await store.MutateAsync(current => [.. current, FullRange("ks1", 3)], TestContext.Current.CancellationToken));

            Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(1, raft.ProposedPayloads[0])));
        }

        // A restart seeds from the durable snapshot before any WAL replay: it must hold the newest map.
        using RangeMapStore reopened = NewStore(new ProposeOutcomeStubRaft(), storagePath);

        Assert.Equal(2, reopened.Current.Find("ks0", "x")!.PartitionId);
        Assert.Equal(3, reopened.Current.Find("ks1", "x")!.PartitionId);
    }

    private static RangeMap DecodeLast(ProposeOutcomeStubRaft raft)
    {
        ProposeOutcomeStubRaft scratchRaft = new();
        using RangeMapStore scratch = NewStore(scratchRaft);
        Assert.True(scratch.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(1, raft.ProposedPayloads[^1])));
        return scratch.Current;
    }
}
