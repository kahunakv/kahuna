using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using System.Text;

namespace Kahuna.Server.Tests;

public class TestDeleteManyKeyValues : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestDeleteManyKeyValues(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Theory, CombinatorialData]
    public async Task TestLocateAndTryDeleteManyKeyValueAcrossPartitions(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            List<KahunaDeleteKeyValueRequestItem> deleteItems = [];
            List<string> keys = [];

            for (int i = 0; i < 4; i++)
            {
                string key = GetRandomKeyName();
                keys.Add(key);

                (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero,
                    key,
                    Encoding.UTF8.GetBytes($"value-{i}"),
                    null,
                    -1,
                    KeyValueFlags.Set,
                    0,
                    KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken
                );

                Assert.Equal(KeyValueResponseType.Set, setType);

                deleteItems.Add(new()
                {
                    TransactionId = HLCTimestamp.Zero,
                    Key = key,
                    Durability = KeyValueDurability.Persistent
                });
            }

            List<KahunaDeleteKeyValueResponseItem> responses = await kahuna2.LocateAndTryDeleteManyKeyValue(
                deleteItems,
                TestContext.Current.CancellationToken
            );

            Assert.Equal(deleteItems.Count, responses.Count);
            Assert.All(responses, response => Assert.Equal(KeyValueResponseType.Deleted, response.Type));

            foreach (string key in keys)
            {
                (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna3.LocateAndTryGetValue(
                    HLCTimestamp.Zero,
                    key,
                    -1,
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken
                );

                Assert.Equal(KeyValueResponseType.DoesNotExist, getType);
                Assert.NotNull(entry);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestDeleteManyNodeKeyValueReturnsOneResponsePerItem(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            KahunaManager km1 = (KahunaManager)kahuna1;

            (IKahuna leader, string existingKey, string missingKey) = await KeysLedByOneNode(
                [(node1, kahuna1), (node2, kahuna2), (node3, kahuna3)], km1);

            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                existingKey,
                Encoding.UTF8.GetBytes("value"),
                null,
                -1,
                KeyValueFlags.Set,
                0,
                KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken
            );

            Assert.Equal(KeyValueResponseType.Set, setType);

            List<KahunaDeleteKeyValueResponseItem> responses = await leader.DeleteManyNodeKeyValue(
            [
                new() { TransactionId = HLCTimestamp.Zero, Key = existingKey, Durability = KeyValueDurability.Persistent },
                new() { TransactionId = HLCTimestamp.Zero, Key = missingKey, Durability = KeyValueDurability.Persistent }
            ]);

            Assert.Equal(2, responses.Count);
            Assert.Contains(responses, response => response.Key == existingKey && response.Type == KeyValueResponseType.Deleted);
            Assert.Contains(responses, response => response.Key == missingKey && response.Type == KeyValueResponseType.DoesNotExist);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestTransactionScriptConsecutiveDeletesUsesDeleteManyCommand(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKeyName();

            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                key,
                Encoding.UTF8.GetBytes("value"),
                null,
                -1,
                KeyValueFlags.Set,
                0,
                KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken
            );

            Assert.Equal(KeyValueResponseType.Set, setType);

            string script = $@"
BEGIN (locking=""optimistic"")
 DELETE `{key}`
 DELETE @empty
 COMMIT
END
";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script),
                null,
                [new() { Key = "@empty", Value = "" }]
            );

            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero,
                key,
                -1,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken
            );

            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry);
            Assert.Equal("value", Encoding.UTF8.GetString(entry.Value!));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestTransactionScriptWithDuplicateDeleteKeysDoesNotBatchAndStillBehavesCorrectly(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKeyName();

            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                key,
                Encoding.UTF8.GetBytes("value"),
                null,
                -1,
                KeyValueFlags.Set,
                0,
                KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken
            );

            Assert.Equal(KeyValueResponseType.Set, setType);

            string script = $@"
BEGIN (locking=""optimistic"")
 DELETE `{key}`
 DELETE `{key}`
 DELETE @empty
 COMMIT
END
";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script),
                null,
                [new() { Key = "@empty", Value = "" }]
            );

            Assert.Equal(KeyValueResponseType.InvalidInput, resp.Type);

            (KeyValueResponseType getType, _) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero,
                key,
                -1,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken
            );

            Assert.Equal(KeyValueResponseType.DoesNotExist, getType);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestFailedDeleteManyItemCausesTransactionAbortAndRollback(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string keyA = GetRandomKeyName();
            string keyB = GetRandomKeyName();

            Assert.Equal(KeyValueResponseType.Set, (await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, keyA, Encoding.UTF8.GetBytes("a"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Item1);

            Assert.Equal(KeyValueResponseType.Set, (await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, keyB, Encoding.UTF8.GetBytes("b"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Item1);

            string script = $@"
BEGIN (locking=""optimistic"")
 DELETE `{keyA}`
 DELETE @empty
 DELETE `{keyB}`
 COMMIT
END
";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script),
                null,
                [new() { Key = "@empty", Value = "" }]
            );

            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);

            Assert.Equal(KeyValueResponseType.Get, (await kahuna3.LocateAndTryGetValue(
                HLCTimestamp.Zero, keyA, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Item1);
            Assert.Equal(KeyValueResponseType.Get, (await kahuna3.LocateAndTryGetValue(
                HLCTimestamp.Zero, keyB, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Item1);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    private static string GetRandomKeyName()
    {
        return $"srv-delete-many-{Guid.NewGuid():N}";
    }

    /// <summary>
    /// Picks a random key and resolves which node currently leads its partition, then derives a
    /// second key routed to the same partition. Demanding leadership from one specific node can
    /// never be satisfied when that node happens to lead no partition, and polling AmILeader
    /// without a delay starves the election machinery it is waiting on, so this iterates all
    /// nodes and backs off between rounds.
    /// </summary>
    private static async Task<(IKahuna Leader, string ExistingKey, string MissingKey)> KeysLedByOneNode(
        (IRaft Raft, IKahuna Kahuna)[] nodes, KahunaManager router)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string existingKey = GetRandomKeyName();
        int partitionId = router.GetDataPartitionForKey(existingKey);

        while (true)
        {
            foreach ((IRaft raft, IKahuna kahuna) in nodes)
            {
                if (await raft.AmILeader(partitionId, ct))
                {
                    string missingKey;

                    do
                    {
                        missingKey = GetRandomKeyName();
                    } while (router.GetDataPartitionForKey(missingKey) != partitionId);

                    return (kahuna, existingKey, missingKey);
                }
            }

            await Task.Delay(50, ct);
        }
    }
}
