using Kahuna.Shared.Sequences;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public sealed class TestSequences
{
    private readonly ILoggerFactory loggerFactory;

    public TestSequences(ITestOutputHelper outputHelper)
    {
        loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .SetMinimumLevel(LogLevel.Debug);
        });
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
