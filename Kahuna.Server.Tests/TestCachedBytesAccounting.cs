
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that KeyValueEntry.CachedBytes stays consistent with EstimateEntryBytes across
/// repeated MVCC and revision cycles — catching the dictionary-overhead drift bug where
/// MvccEntryRemovedBytes / EstimateRevisionRemovedBytes failed to refund DictionaryOverheadBytes
/// when the last entry was removed from the dict.
/// </summary>
[Collection("ClusterTests")]
public class TestCachedBytesAccounting : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestCachedBytesAccounting(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// Finds the KeyValueEntry for a key by scanning all actors in the given durability pool.
    /// Returns null if not found (key not yet cached in any actor).
    /// </summary>
    private static KeyValueEntry? FindEntry(KahunaManager manager, string key, KeyValueDurability durability)
    {
        var instances = durability == KeyValueDurability.Ephemeral
            ? manager.KeyValues.EphemeralInstances
            : manager.KeyValues.PersistentInstances;

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef in instances)
        {
            var concreteRef = (ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)actorRef;
            if (concreteRef.Runner.Actor is KeyValueActor actor)
            {
                KeyValueContext ctx = actor.GetContext();
                if (ctx.Store.TryGetValue(key, out KeyValueEntry? entry))
                    return entry;
            }
        }
        return null;
    }

    private static void AssertCachedBytesConsistent(KahunaManager manager, string key, KeyValueDurability durability)
    {
        KeyValueEntry? entry = FindEntry(manager, key, durability);
        Assert.NotNull(entry);

        long expected = KeyValueStoreAccounting.EstimateEntryBytes(key, entry) - (key.Length * sizeof(char));
        Assert.Equal(expected, entry.CachedBytes);
    }

    [Theory, CombinatorialData]
    public async Task CachedBytes_StaysConsistentAcrossRepeatedMvccCycles(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, _, _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string key = "t:acct/mvcc-cycle";
            const KeyValueDurability durability = KeyValueDurability.Ephemeral;
            KahunaManager manager = (KahunaManager)kahuna1;

            // Seed the key with no transaction so an in-memory entry exists.
            (KeyValueResponseType setType, _, _) = await kahuna1.TrySetKeyValue(
                HLCTimestamp.Zero, key, "initial"u8.ToArray(), null, 0,
                KeyValueFlags.Set, 0, durability);
            Assert.Equal(KeyValueResponseType.Set, setType);

            AssertCachedBytesConsistent(manager, key, durability);

            // Drive repeated MVCC add→remove cycles (set-within-tx → commit).
            for (int i = 0; i < 8; i++)
            {
                HLCTimestamp txId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

                // TrySetKeyValue with a transactionId creates an MVCC entry.
                (KeyValueResponseType prepType, _, _) = await kahuna1.TrySetKeyValue(
                    txId, key, Encoding.UTF8.GetBytes($"value-{i}"), null, 0,
                    KeyValueFlags.Set, 0, durability);
                Assert.Equal(KeyValueResponseType.Set, prepType);

                // Prepare mutations (creates WriteIntent + tickets the proposal).
                HLCTimestamp commitId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
                (KeyValueResponseType prType, HLCTimestamp ticketId, _, _) =
                    await kahuna1.TryPrepareMutations(txId, commitId, key, durability);
                Assert.Equal(KeyValueResponseType.Prepared, prType);

                // Commit — removes the MVCC entry and archives a revision.
                (KeyValueResponseType cmType, _) =
                    await kahuna1.TryCommitMutations(txId, key, ticketId, durability);
                Assert.Equal(KeyValueResponseType.Committed, cmType);

                // CachedBytes must exactly match EstimateEntryBytes after each drain.
                AssertCachedBytesConsistent(manager, key, durability);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task CachedBytes_StaysConsistentAcrossRepeatedRollbackCycles(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, _, _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string key = "t:acct/rollback-cycle";
            const KeyValueDurability durability = KeyValueDurability.Ephemeral;
            KahunaManager manager = (KahunaManager)kahuna1;

            // Seed the key.
            (KeyValueResponseType setType, _, _) = await kahuna1.TrySetKeyValue(
                HLCTimestamp.Zero, key, "initial"u8.ToArray(), null, 0,
                KeyValueFlags.Set, 0, durability);
            Assert.Equal(KeyValueResponseType.Set, setType);

            AssertCachedBytesConsistent(manager, key, durability);

            // Drive repeated MVCC add→remove cycles via rollback.
            for (int i = 0; i < 8; i++)
            {
                HLCTimestamp txId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

                (KeyValueResponseType prepType, _, _) = await kahuna1.TrySetKeyValue(
                    txId, key, Encoding.UTF8.GetBytes($"value-{i}"), null, 0,
                    KeyValueFlags.Set, 0, durability);
                Assert.Equal(KeyValueResponseType.Set, prepType);

                HLCTimestamp commitId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
                (KeyValueResponseType prType, HLCTimestamp ticketId, _, _) =
                    await kahuna1.TryPrepareMutations(txId, commitId, key, durability);
                Assert.Equal(KeyValueResponseType.Prepared, prType);

                // Rollback — also removes the MVCC entry without archiving a revision.
                (KeyValueResponseType rbType, _) =
                    await kahuna1.TryRollbackMutations(txId, key, ticketId, durability);
                Assert.Equal(KeyValueResponseType.RolledBack, rbType);

                AssertCachedBytesConsistent(manager, key, durability);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
