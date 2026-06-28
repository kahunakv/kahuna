using System.Text;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

public sealed class TestSequences
{
    private readonly ILoggerFactory loggerFactory;

    public TestSequences(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task TestCreateAndAllocateSequenceValues()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "orders/" + Guid.NewGuid().ToString("N");

        (SequenceResponseType response, long revision) = await node.Kahuna.LocateAndCreateSequence(
            name,
            0,
            1,
            null,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(0, revision);

        (response, SequenceAllocation allocation) = await node.Kahuna.LocateAndNextSequenceValue(
            name,
            null,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(1, allocation.Start);
        Assert.Equal(1, allocation.End);

        (response, allocation) = await node.Kahuna.LocateAndReserveSequenceRange(
            name,
            3,
            null,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(2, allocation.Start);
        Assert.Equal(4, allocation.End);
        Assert.Equal(3, allocation.Count);

        (response, ReadOnlySequenceEntry? sequence) = await node.Kahuna.LocateAndGetSequence(
            name,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.NotNull(sequence);
        Assert.Equal(4, sequence.CurrentValue);
    }

    [Fact]
    public async Task TestSequenceIdempotencyAndMaxValue()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "invoice/" + Guid.NewGuid().ToString("N");

        (SequenceResponseType response, _) = await node.Kahuna.LocateAndCreateSequence(
            name,
            0,
            1,
            2,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);

        (response, SequenceAllocation first) = await node.Kahuna.LocateAndNextSequenceValue(
            name,
            "retry-key",
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(1, first.Start);

        (response, SequenceAllocation replayed) = await node.Kahuna.LocateAndNextSequenceValue(
            name,
            "retry-key",
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(first, replayed);

        (response, _) = await node.Kahuna.LocateAndReserveSequenceRange(
            name,
            2,
            null,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.MaxValueExceeded, response);
    }

    [Fact]
    public async Task TestConcurrentSequenceAllocationsAreUnique()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "concurrent/" + Guid.NewGuid().ToString("N");

        (SequenceResponseType response, _) = await node.Kahuna.LocateAndCreateSequence(
            name,
            0,
            1,
            null,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);

        Task<long>[] tasks = Enumerable.Range(0, 100)
            .Select(_ => AllocateNext(node, name))
            .ToArray();

        long[] values = await Task.WhenAll(tasks);

        Assert.Equal(Enumerable.Range(1, 100).Select(x => (long)x), values.Order());
    }

    [Fact]
    public async Task TestReadsLegacyJsonRecordAndUpgradesToBinary()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "legacy/" + Guid.NewGuid().ToString("N");

        // Seed a record in the pre-binary JSON format (System.Text.Json Web shape) directly at the
        // sequencer's storage key, including a populated idempotency map. New nodes never write this
        // format, so only a hand-seeded blob exercises the Deserialize JSON branch (value[0] == '{').
        string legacyJson =
            """
            {"name":"NAME","currentValue":10,"initialValue":0,"increment":1,"maxValue":null,"createdAt":{"n":0,"l":0,"c":0},"updatedAt":{"n":0,"l":0,"c":0},"idempotency":{"reserve:legacy-key":{"name":"NAME","start":5,"end":5,"count":1,"revision":0}}}
            """.Replace("NAME", name);

        (KeyValueResponseType setResponse, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            "__kahuna:sequences:" + name,
            Encoding.UTF8.GetBytes(legacyJson),
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, setResponse);

        // Reading the sequence deserializes the legacy JSON scalar fields.
        (SequenceResponseType getResponse, ReadOnlySequenceEntry? sequence) = await node.Kahuna.LocateAndGetSequence(
            name,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, getResponse);
        Assert.NotNull(sequence);
        Assert.Equal(10, sequence.CurrentValue);

        // Idempotency replay returns the allocation seeded in the legacy map (no write occurs), proving
        // the legacy idempotency dictionary deserialized correctly.
        (SequenceResponseType replayResponse, SequenceAllocation replayed) = await node.Kahuna.LocateAndNextSequenceValue(
            name,
            "legacy-key",
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, replayResponse);
        Assert.Equal(5, replayed.Start);
        Assert.Equal(5, replayed.End);

        // A fresh allocation reads the legacy record, allocates from CurrentValue=10, and rewrites it in
        // the new binary format via CAS — the read-legacy → write-binary upgrade path.
        (SequenceResponseType nextResponse, SequenceAllocation allocated) = await node.Kahuna.LocateAndNextSequenceValue(
            name,
            null,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, nextResponse);
        Assert.Equal(11, allocated.Start);
        Assert.Equal(11, allocated.End);

        // The upgraded record (now binary) round-trips on a subsequent read.
        (SequenceResponseType finalResponse, ReadOnlySequenceEntry? upgraded) = await node.Kahuna.LocateAndGetSequence(
            name,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, finalResponse);
        Assert.NotNull(upgraded);
        Assert.Equal(11, upgraded.CurrentValue);
    }

    private async Task<EmbeddedKahunaNode> StartNode()
    {
        EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);
        return node;
    }

    private static async Task<long> AllocateNext(EmbeddedKahunaNode node, string name)
    {
        (SequenceResponseType response, SequenceAllocation allocation) = await node.Kahuna.LocateAndNextSequenceValue(
            name,
            null,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, response);
        return allocation.Start;
    }
}
