using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests that a run of adjacent writes collapsed into one batched call produces the same result as running
/// the same statements one at a time. The interesting case is two statements whose keys are written
/// differently in the script but resolve to the same key.
/// </summary>
public class TestKeyValueScriptBatching : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptBatching(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string GetRandomKey()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }

    /// <summary>
    /// Two writes whose keys resolve to the same key must produce the same committed value and revision
    /// whether or not the pair is collapsed into one batched call. The baseline is the same script with a
    /// statement between the two writes, which breaks the batchable run and forces them to execute one at a
    /// time.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestDuplicateKeyWritesMatchSequential([CombinatorialValues("memory")] string storage, [CombinatorialValues(1, 4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string batched = """
            BEGIN
                SET @first 'first'
                SET @second 'second'
                COMMIT
            END
            """;

            // The LET between the writes ends the batchable run, so this pair executes one statement at a
            // time and gives the behaviour the batched form has to match.
            const string sequential = """
            BEGIN
                SET @first 'first'
                LET spacer = 1
                SET @second 'second'
                COMMIT
            END
            """;

            // Repeated, because a batch fans its items out together: a single run can agree by luck.
            for (int attempt = 0; attempt < 15; attempt++)
            {
                (string batchedValue, long batchedRevision) = await RunAndRead(kahuna1, batched);
                (string sequentialValue, long sequentialRevision) = await RunAndRead(kahuna1, sequential);

                Assert.Equal(sequentialValue, batchedValue);
                Assert.Equal(sequentialRevision, batchedRevision);
                Assert.Equal("second", batchedValue);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A statement list that opens with a batched run must then execute every remaining statement in
    /// order, run a nested list (with its own batched run) to completion, and execute nothing after the
    /// statement that stops the script.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestStatementsAfterBatchedRunExecuteInOrder([CombinatorialValues("memory")] string storage, [CombinatorialValues(1, 4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string script = """
            BEGIN
                SET @a 'one'
                SET @b 'two'
                SET @c 'three'
                LET spacer = 1
                SET @a 'one-again'
                IF spacer = 1 THEN
                    SET @d 'four'
                    SET @e 'five'
                END
                COMMIT
                SET @f 'never'
            END
            """;

            string prefix = GetRandomKey();

            List<KeyValueParameter> parameters =
            [
                new() { Key = "@a", Value = prefix + "/a" },
                new() { Key = "@b", Value = prefix + "/b" },
                new() { Key = "@c", Value = prefix + "/c" },
                new() { Key = "@d", Value = prefix + "/d" },
                new() { Key = "@e", Value = prefix + "/e" },
                new() { Key = "@f", Value = prefix + "/f" }
            ];

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, parameters);

            Assert.True(resp.Type == KeyValueResponseType.Set, "unexpected response " + resp.Type + " " + resp.Reason);

            Assert.Equal("one-again", await ReadValue(kahuna1, prefix + "/a"));
            Assert.Equal("two", await ReadValue(kahuna1, prefix + "/b"));
            Assert.Equal("three", await ReadValue(kahuna1, prefix + "/c"));
            Assert.Equal("four", await ReadValue(kahuna1, prefix + "/d"));
            Assert.Equal("five", await ReadValue(kahuna1, prefix + "/e"));
            Assert.Null(await ReadValue(kahuna1, prefix + "/f"));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    private static async Task<string?> ReadValue(IKahuna kahuna, string key)
    {
        KeyValueTransactionResult read = await kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("GET @key"),
            null,
            [new() { Key = "@key", Value = key }]);

        if (read.Type == KeyValueResponseType.DoesNotExist)
            return null;

        Assert.Equal(KeyValueResponseType.Get, read.Type);

        return Encoding.UTF8.GetString(read.Value ?? []);
    }

    private static async Task<(string Value, long Revision)> RunAndRead(IKahuna kahuna, string script)
    {
        string key = GetRandomKey();

        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes(script),
            null,
            [new() { Key = "@first", Value = key }, new() { Key = "@second", Value = key }]);

        Assert.True(
            resp.Type is KeyValueResponseType.Set or KeyValueResponseType.Get,
            "unexpected response " + resp.Type + " " + resp.Reason);

        KeyValueTransactionResult read = await kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("GET @first"),
            null,
            [new() { Key = "@first", Value = key }]);

        Assert.Equal(KeyValueResponseType.Get, read.Type);

        return (Encoding.UTF8.GetString(read.Value ?? []), read.Revision);
    }

    /// <summary>
    /// The delete equivalent of the test above. Deletes already resolved their keys before deciding whether
    /// to batch, so this pair has always run one statement at a time.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestDuplicateKeyDeletesAgreeWithSequential([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            for (int attempt = 0; attempt < 10; attempt++)
            {
                string key = GetRandomKey();

                KeyValueTransactionResult seed = await kahuna1.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes("SET @k 'value'"), null, [new() { Key = "@k", Value = key }]);

                Assert.Equal(KeyValueResponseType.Set, seed.Type);

                const string script = """
                BEGIN
                    DELETE @first
                    DELETE @second
                    COMMIT
                END
                """;

                KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes(script),
                    null,
                    [new() { Key = "@first", Value = key }, new() { Key = "@second", Value = key }]);

                Assert.True(
                    resp.Type is KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist,
                    "unexpected response " + resp.Type + " " + resp.Reason);

                KeyValueTransactionResult read = await kahuna1.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes("EXISTS @k"), null, [new() { Key = "@k", Value = key }]);

                Assert.Equal(KeyValueResponseType.DoesNotExist, read.Type);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
