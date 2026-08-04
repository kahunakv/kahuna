using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Sequencer.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Behaviour of the sequencer on a single embedded node: the semantics callers depend on (uniqueness,
/// monotonicity, boundaries, idempotent replay, record compatibility) and the block reservation that
/// makes allocation cheap.
///
/// <para>Several tests count the Raft entries a run actually proposes, by decoding the key-value
/// replication payloads a recording batch executor sees. That is what proves values are being served
/// from memory rather than from a per-value compare-and-swap — a green functional test cannot.</para>
/// </summary>
public sealed class TestSequences
{
    private readonly ILoggerFactory loggerFactory;

    public TestSequences(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    // ── semantics ───────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TestCreateAndAllocateSequenceValues()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "orders/" + Guid.NewGuid().ToString("N");

        (SequenceResponseType response, long revision) = await Create(node, name);

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(0, revision);

        (response, SequenceAllocation allocation) = await Next(node, name);

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(1, allocation.Start);
        Assert.Equal(1, allocation.End);

        (response, allocation) = await AllocateRangeRaw(node, name, 3);

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(2, allocation.Start);
        Assert.Equal(4, allocation.End);
        Assert.Equal(3, allocation.Count);

        (response, ReadOnlySequenceEntry? sequence) = await node.Kahuna.LocateAndGetSequence(
            name,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        // CurrentValue is the reserved high-water mark, not the last value handed out: four values have
        // been issued, but a whole block was reserved to issue them from.
        Assert.Equal(SequenceResponseType.Success, response);
        Assert.NotNull(sequence);
        Assert.Equal(DefaultBlockSize, sequence.CurrentValue);
        Assert.Equal(1, sequence.Increment);
        Assert.Equal(0, sequence.InitialValue);
    }

    [Fact]
    public async Task TestDuplicateCreateReportsAlreadyExists()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "dup/" + Guid.NewGuid().ToString("N");

        (SequenceResponseType first, _) = await Create(node, name);
        Assert.Equal(SequenceResponseType.Success, first);

        (SequenceResponseType second, _) = await Create(node, name);
        Assert.Equal(SequenceResponseType.AlreadyExists, second);
    }

    [Fact]
    public async Task TestOperationsOnMissingSequenceReportNotFound()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "missing/" + Guid.NewGuid().ToString("N");

        (SequenceResponseType next, _) = await Next(node, name);
        Assert.Equal(SequenceResponseType.NotFound, next);

        (SequenceResponseType get, _) = await node.Kahuna.LocateAndGetSequence(
            name, SequenceDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(SequenceResponseType.NotFound, get);

        SequenceResponseType delete = await node.Kahuna.LocateAndDeleteSequence(
            name, SequenceDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(SequenceResponseType.NotFound, delete);
    }

    [Fact]
    public async Task TestDeleteDiscardsTheReservedBlock()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "recreate/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        (SequenceResponseType response, SequenceAllocation first) = await Next(node, name);
        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(1, first.Start);

        Assert.Equal(SequenceResponseType.Success, await node.Kahuna.LocateAndDeleteSequence(
            name, SequenceDurability.Persistent, TestContext.Current.CancellationToken));

        // The actor still held a block spanning 2..DefaultBlockSize. Recreating the name must restart at
        // the new record's initial value rather than drain the deleted incarnation's block.
        Assert.Equal(SequenceResponseType.Success, (await Create(node, name, initialValue: 100)).Item1);

        (response, SequenceAllocation afterRecreate) = await Next(node, name);
        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(101, afterRecreate.Start);
    }

    [Fact]
    public async Task TestNonDefaultIncrementIsHonouredAcrossBlockEdge()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 4);
        string name = "step/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name, initialValue: 0, increment: 5)).Item1);

        List<long> issued = [];

        for (int i = 0; i < 10; i++)
        {
            (SequenceResponseType response, SequenceAllocation allocation) = await Next(node, name);
            Assert.Equal(SequenceResponseType.Success, response);
            issued.Add(allocation.Start);
        }

        Assert.Equal(Enumerable.Range(1, 10).Select(x => (long)(x * 5)), issued);
    }

    [Fact]
    public async Task TestConcurrentSequenceAllocationsAreUnique()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "concurrent/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        Task<long>[] tasks = Enumerable.Range(0, 100)
            .Select(_ => AllocateNext(node, name))
            .ToArray();

        long[] values = await Task.WhenAll(tasks);

        Assert.Equal(Enumerable.Range(1, 100).Select(x => (long)x), values.Order());
    }

    /// <summary>
    /// Mixed single and multi-value reserves racing block exhaustion. A run that does not fit in what is
    /// left of a block takes a whole new one — never a split allocation — so values may be skipped, but
    /// no value may be issued twice and every reserve must be internally contiguous.
    /// </summary>
    [Fact]
    public async Task TestMixedReservesRacingBlockExhaustionStayUnique()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 4);
        string name = "mixed/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        Task<SequenceAllocation>[] tasks = Enumerable.Range(0, 60)
            .Select(i => AllocateRange(node, name, i % 3 == 0 ? 3 : 1))
            .ToArray();

        SequenceAllocation[] allocations = await Task.WhenAll(tasks);

        HashSet<long> seen = [];

        foreach (SequenceAllocation allocation in allocations)
        {
            Assert.Equal(allocation.Start + allocation.Count - 1, allocation.End);

            for (long value = allocation.Start; value <= allocation.End; value++)
                Assert.True(seen.Add(value), $"value {value} was issued more than once");
        }

        Assert.Equal(allocations.Sum(a => a.Count), seen.Count);
    }

    // ── boundaries ──────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TestMaxValueLandingExactlyOnABlockEdge()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 4);
        string name = "edge/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name, maxValue: 4)).Item1);

        for (int i = 1; i <= 4; i++)
        {
            (SequenceResponseType response, SequenceAllocation allocation) = await Next(node, name);
            Assert.Equal(SequenceResponseType.Success, response);
            Assert.Equal(i, allocation.Start);
        }

        (SequenceResponseType exhausted, _) = await Next(node, name);
        Assert.Equal(SequenceResponseType.MaxValueExceeded, exhausted);
    }

    [Fact]
    public async Task TestMaxValueInsideABlockTruncatesTheReservation()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "truncated/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name, maxValue: 3)).Item1);

        for (int i = 1; i <= 3; i++)
            Assert.Equal(i, (await Next(node, name)).Item2.Start);

        (SequenceResponseType exhausted, _) = await Next(node, name);
        Assert.Equal(SequenceResponseType.MaxValueExceeded, exhausted);

        // The reservation was clamped to the ceiling the maximum allows, never past it.
        (_, ReadOnlySequenceEntry? sequence) = await node.Kahuna.LocateAndGetSequence(
            name, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.NotNull(sequence);
        Assert.Equal(3, sequence.CurrentValue);
    }

    [Fact]
    public async Task TestExhaustionMidBlockCostsNoWrite()
    {
        SequenceWriteRecorder recorder = new();
        await using EmbeddedKahunaNode node = await StartNode(recorder: recorder);
        string name = "nowrite/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name, maxValue: 2)).Item1);

        Assert.Equal(1, (await Next(node, name)).Item2.Start);
        Assert.Equal(2, (await Next(node, name)).Item2.Start);

        int before = recorder.CountFor(name);

        Assert.Equal(SequenceResponseType.MaxValueExceeded, (await Next(node, name)).Item1);
        Assert.Equal(SequenceResponseType.MaxValueExceeded, (await AllocateRangeRaw(node, name, 5)).Item1);

        Assert.Equal(before, recorder.CountFor(name));
    }

    /// <summary>
    /// A sequence approaching <see cref="long.MaxValue"/> must clamp its reservation rather than wrap.
    /// </summary>
    [Fact]
    public async Task TestOverflowNearLongMaxValueIsReportedNotWrapped()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "overflow/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success,
            (await Create(node, name, initialValue: long.MaxValue - 3)).Item1);

        for (int i = 3; i >= 1; i--)
        {
            (SequenceResponseType response, SequenceAllocation allocation) = await Next(node, name);
            Assert.Equal(SequenceResponseType.Success, response);
            Assert.Equal(long.MaxValue - i + 1, allocation.Start);
        }

        (SequenceResponseType exhausted, _) = await Next(node, name);
        Assert.Equal(SequenceResponseType.MaxValueExceeded, exhausted);
    }

    [Fact]
    public async Task TestReserveLargerThanABlockIsServedContiguously()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 4);
        string name = "wide/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        (SequenceResponseType response, SequenceAllocation allocation) = await AllocateRangeRaw(node, name, 25);

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(1, allocation.Start);
        Assert.Equal(25, allocation.End);
        Assert.Equal(25, allocation.Count);
    }

    [Fact]
    public async Task TestInvalidInputIsRejected()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "invalid/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        Assert.Equal(SequenceResponseType.InvalidInput, (await AllocateRangeRaw(node, name, 0)).Item1);
        Assert.Equal(SequenceResponseType.InvalidInput, (await AllocateRangeRaw(node, name, -1)).Item1);

        // A reserved-prefix name is not addressable as a sequence.
        (SequenceResponseType reserved, _) = await Next(node, "__kahuna:sequences:whatever");
        Assert.Equal(SequenceResponseType.InvalidInput, reserved);

        // Non-positive increments and a maximum below the initial value are refused at create.
        Assert.Equal(SequenceResponseType.InvalidInput, (await Create(node, name + "-b", increment: 0)).Item1);
        Assert.Equal(SequenceResponseType.InvalidInput, (await Create(node, name + "-c", initialValue: 10, maxValue: 5)).Item1);

        // An idempotency key too large to length-prefix in the record is refused.
        (SequenceResponseType oversized, _) = await node.Kahuna.LocateAndReserveSequenceRange(
            name, 1, new string('k', 2000), SequenceDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(SequenceResponseType.InvalidInput, oversized);
    }

    // ── block reservation ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The point of the whole design: N allocations cost ceil(N / blockSize) writes, not N.
    /// </summary>
    [Fact]
    public async Task TestAllocationsAreAmortizedOverOneWritePerBlock()
    {
        SequenceWriteRecorder recorder = new();
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 10, recorder: recorder);
        string name = "amortized/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        int afterCreate = recorder.CountFor(name);

        for (int i = 0; i < 30; i++)
            Assert.Equal(SequenceResponseType.Success, (await Next(node, name)).Item1);

        Assert.Equal(3, recorder.CountFor(name) - afterCreate);
    }

    /// <summary>
    /// A block size of one is the escape hatch for callers that cannot tolerate gaps: every value is
    /// durably reserved before it is handed out, exactly as the pre-block implementation behaved.
    /// </summary>
    [Fact]
    public async Task TestBlockSizeOneWritesOncePerValueAndLeavesNoGaps()
    {
        SequenceWriteRecorder recorder = new();
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 1, recorder: recorder);
        string name = "gapfree/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        int afterCreate = recorder.CountFor(name);

        for (int i = 1; i <= 12; i++)
            Assert.Equal(i, (await Next(node, name)).Item2.Start);

        Assert.Equal(12, recorder.CountFor(name) - afterCreate);

        // With no block held, the record never runs ahead of the values actually issued.
        (_, ReadOnlySequenceEntry? sequence) = await node.Kahuna.LocateAndGetSequence(
            name, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.NotNull(sequence);
        Assert.Equal(12, sequence.CurrentValue);
    }

    [Fact]
    public async Task TestPlainAllocationsInsideABlockTouchNoStorage()
    {
        SequenceWriteRecorder recorder = new();
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 100, recorder: recorder);
        string name = "quiet/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        // The first allocation reserves the block.
        Assert.Equal(SequenceResponseType.Success, (await Next(node, name)).Item1);

        int afterFirstBlock = recorder.CountFor(name);

        for (int i = 0; i < 50; i++)
            Assert.Equal(SequenceResponseType.Success, (await Next(node, name)).Item1);

        Assert.Equal(afterFirstBlock, recorder.CountFor(name));
    }

    /// <summary>
    /// Surrendering leadership abandons the block. The next allocation restarts from the durable mark,
    /// which produces a gap — and never a value the abandoned block could also have issued.
    /// </summary>
    [Fact]
    public async Task TestLeadershipChangeAbandonsTheBlockLeavingAGap()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 100);
        string name = "surrender/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);
        Assert.Equal(1, (await Next(node, name)).Item2.Start);

        await node.Kahuna.OnLeaderChanged(1, "someone-else");

        (SequenceResponseType response, SequenceAllocation afterChange) = await Next(node, name);

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(101, afterChange.Start);
    }

    // ── idempotency ─────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TestSequenceIdempotencyAndMaxValue()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "invoice/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name, maxValue: 2)).Item1);

        (SequenceResponseType response, SequenceAllocation first) = await Next(node, name, "retry-key");

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(1, first.Start);

        (response, SequenceAllocation replayed) = await Next(node, name, "retry-key");

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(first, replayed);

        (response, _) = await AllocateRangeRaw(node, name, 2);

        Assert.Equal(SequenceResponseType.MaxValueExceeded, response);
    }

    /// <summary>
    /// A keyed reserve is replayable across the block it was issued from being replaced, and across the
    /// actor losing its state entirely — the record, not memory, is what makes the replay work.
    /// </summary>
    [Fact]
    public async Task TestIdempotentReplaySurvivesBlockTurnoverAndActorLoss()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 4);
        string name = "replay/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        (SequenceResponseType response, SequenceAllocation keyed) = await Next(node, name, "order-42");
        Assert.Equal(SequenceResponseType.Success, response);

        // Drain past the block edge so the record is rewritten several times over.
        for (int i = 0; i < 12; i++)
            Assert.Equal(SequenceResponseType.Success, (await Next(node, name)).Item1);

        (response, SequenceAllocation afterBumps) = await Next(node, name, "order-42");
        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(keyed, afterBumps);

        // Dropping every resident block is what a leadership change or an eviction does; the replay must
        // then be answered from the durable record.
        await node.Kahuna.OnLeaderChanged(1, "someone-else");

        (response, SequenceAllocation afterActorLoss) = await Next(node, name, "order-42");
        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(keyed, afterActorLoss);
    }

    [Fact]
    public async Task TestKeyedReserveIsDurableBeforeItIsAnswered()
    {
        SequenceWriteRecorder recorder = new();
        await using EmbeddedKahunaNode node = await StartNode(recorder: recorder);
        string name = "durable/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);
        Assert.Equal(SequenceResponseType.Success, (await Next(node, name)).Item1);

        int afterBlock = recorder.CountFor(name);

        Assert.Equal(SequenceResponseType.Success, (await Next(node, name, "keyed-1")).Item1);

        // Mid-block, with no ceiling to move, the keyed request still writes: that write is what makes
        // the allocation replayable after this node forgets it.
        Assert.Equal(1, recorder.CountFor(name) - afterBlock);

        // The replay is answered from memory and writes nothing further.
        int afterKeyed = recorder.CountFor(name);
        Assert.Equal(SequenceResponseType.Success, (await Next(node, name, "keyed-1")).Item1);
        Assert.Equal(afterKeyed, recorder.CountFor(name));
    }

    /// <summary>
    /// Idempotency entries are bounded, so a client that never reuses a key cannot grow the record
    /// without limit — every keyed reserve used to be retained forever and re-serialized on every
    /// subsequent operation.
    /// </summary>
    [Fact]
    public async Task TestIdempotencyRetentionIsBounded()
    {
        await using EmbeddedKahunaNode node = await StartNode(idempotencyRetentionMax: 8);
        string name = "retention/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        for (int i = 0; i < 40; i++)
            Assert.Equal(SequenceResponseType.Success, (await Next(node, name, "key-" + i)).Item1);

        (KeyValueResponseType getResponse, ReadOnlyKeyValueEntry? entry) = await SystemKeyValues(node).SystemGetKeyValue(
            "__kahuna:sequences:" + name,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Get, getResponse);
        Assert.NotNull(entry?.Value);

        // Each retained entry costs roughly 100 bytes here (its key, the sequence name, four numbers and
        // a timestamp), so 8 of them fit comfortably under 1.5 KB while the 40 an unbounded record would
        // hold could not.
        Assert.True(entry.Value.Length < 1_500, $"record grew to {entry.Value.Length} bytes despite the retention cap");

        // The most recent key still replays; the oldest has been reclaimed and allocates fresh values.
        (SequenceResponseType recent, SequenceAllocation recentAllocation) = await Next(node, name, "key-39");
        Assert.Equal(SequenceResponseType.Success, recent);
        Assert.Equal(40, recentAllocation.Start);

        (SequenceResponseType evicted, SequenceAllocation evictedAllocation) = await Next(node, name, "key-0");
        Assert.Equal(SequenceResponseType.Success, evicted);
        Assert.Equal(41, evictedAllocation.Start);
    }

    // ── record compatibility ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TestReadsLegacyJsonRecordAndUpgradesToBinary()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "legacy/" + Guid.NewGuid().ToString("N");

        // Seed a record in the pre-binary JSON format (System.Text.Json Web shape) directly at the
        // sequencer's storage key, including a populated idempotency map. New nodes never write this
        // format, so only a hand-seeded blob exercises the JSON branch (value[0] == '{').
        string legacyJson =
            """
            {"name":"NAME","currentValue":10,"initialValue":0,"increment":1,"maxValue":null,"createdAt":{"n":0,"l":0,"c":0},"updatedAt":{"n":0,"l":0,"c":0},"idempotency":{"reserve:legacy-key":{"name":"NAME","start":5,"end":5,"count":1,"revision":0}}}
            """.Replace("NAME", name);

        (KeyValueResponseType setResponse, _, _) = await SystemKeyValues(node).SystemSetKeyValue(
            "__kahuna:sequences:" + name,
            Encoding.UTF8.GetBytes(legacyJson),
            -1,
            KeyValueFlags.Set,
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

        // Idempotency replay returns the allocation seeded in the legacy map (no allocation occurs),
        // proving the legacy idempotency dictionary deserialized correctly.
        (SequenceResponseType replayResponse, SequenceAllocation replayed) = await Next(node, name, "legacy-key");

        Assert.Equal(SequenceResponseType.Success, replayResponse);
        Assert.Equal(5, replayed.Start);
        Assert.Equal(5, replayed.End);

        // A fresh allocation reads the legacy record, reserves a block from CurrentValue=10, and rewrites
        // it in the current binary format via compare-and-swap — the read-legacy → write-current upgrade.
        (SequenceResponseType nextResponse, SequenceAllocation allocated) = await Next(node, name);

        Assert.Equal(SequenceResponseType.Success, nextResponse);
        Assert.Equal(11, allocated.Start);
        Assert.Equal(11, allocated.End);

        (SequenceResponseType finalResponse, ReadOnlySequenceEntry? upgraded) = await node.Kahuna.LocateAndGetSequence(
            name,
            SequenceDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(SequenceResponseType.Success, finalResponse);
        Assert.NotNull(upgraded);
        Assert.Equal(10 + DefaultBlockSize, upgraded.CurrentValue);
    }

    /// <summary>
    /// Records written by the first binary format carried no per-entry timestamp. They must still read,
    /// replay, and migrate forward on the next write.
    /// </summary>
    [Fact]
    public async Task TestReadsBinaryRecordWithoutEntryTimestamps()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "binaryv1/" + Guid.NewGuid().ToString("N");

        (KeyValueResponseType setResponse, _, _) = await SystemKeyValues(node).SystemSetKeyValue(
            "__kahuna:sequences:" + name,
            BinaryRecordWithoutEntryTimestamps(name, currentValue: 20, idempotencyKey: "reserve:v1-key", allocated: 7),
            -1,
            KeyValueFlags.Set,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, setResponse);

        (SequenceResponseType getResponse, ReadOnlySequenceEntry? sequence) = await node.Kahuna.LocateAndGetSequence(
            name, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(SequenceResponseType.Success, getResponse);
        Assert.NotNull(sequence);
        Assert.Equal(20, sequence.CurrentValue);

        (SequenceResponseType replayResponse, SequenceAllocation replayed) = await Next(node, name, "v1-key");
        Assert.Equal(SequenceResponseType.Success, replayResponse);
        Assert.Equal(7, replayed.Start);

        (SequenceResponseType nextResponse, SequenceAllocation allocated) = await Next(node, name);
        Assert.Equal(SequenceResponseType.Success, nextResponse);
        Assert.Equal(21, allocated.Start);
    }

    // ── protection of the reserved keyspace ─────────────────────────────────────────────────────

    /// <summary>
    /// The <c>__kahuna:</c> namespace holds system records (the sequence records live under it), so
    /// the public key-value entry points must refuse it: a client write, delete, expiry extension, or
    /// lock there could corrupt a sequence out from under its owning actor.
    /// </summary>
    [Fact]
    public async Task TestReservedKeySpaceIsRejectedByTheKeyValueApi()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        CancellationToken ct = TestContext.Current.CancellationToken;

        string name = "protected/" + Guid.NewGuid().ToString("N");
        string storageKey = "__kahuna:sequences:" + name;

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        (KeyValueResponseType setResponse, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, storageKey, [1, 2, 3], null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, setResponse);

        (KeyValueResponseType deleteResponse, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, storageKey, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, deleteResponse);

        (KeyValueResponseType extendResponse, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
            HLCTimestamp.Zero, storageKey, 10, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, extendResponse);

        (KeyValueResponseType getResponse, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, storageKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, getResponse);

        (KeyValueResponseType lockResponse, _, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            HLCTimestamp.Zero, storageKey, 1000, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, lockResponse);

        // The record itself was untouched by any of the rejected calls.
        (SequenceResponseType next, SequenceAllocation allocation) = await Next(node, name);
        Assert.Equal(SequenceResponseType.Success, next);
        Assert.Equal(1, allocation.Start);
    }

    /// <summary>
    /// An idempotency key must always describe the same request. Replaying a recorded allocation under
    /// a different count would silently hand the caller a range of the wrong size, so it is rejected.
    /// </summary>
    [Fact]
    public async Task TestIdempotentReplayWithDifferentCountIsRejected()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "countcheck/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        (SequenceResponseType first, SequenceAllocation original) = await node.Kahuna.LocateAndReserveSequenceRange(
            name, 3, "batch-1", SequenceDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(SequenceResponseType.Success, first);
        Assert.Equal(3, original.Count);

        // Same key, same count: the identical allocation replays.
        (SequenceResponseType replay, SequenceAllocation replayed) = await node.Kahuna.LocateAndReserveSequenceRange(
            name, 3, "batch-1", SequenceDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(SequenceResponseType.Success, replay);
        Assert.Equal(original, replayed);

        // Same key, different count: rejected rather than silently mis-sized.
        (SequenceResponseType mismatched, _) = await node.Kahuna.LocateAndReserveSequenceRange(
            name, 1, "batch-1", SequenceDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(SequenceResponseType.InvalidInput, mismatched);
    }

    /// <summary>
    /// A reserved block is normally served from memory with no storage traffic, so a node serving a
    /// block could miss that its sequence was deleted and recreated by another owner. The block lease
    /// bounds that: once it expires, the next allocation revalidates against the durable record,
    /// detects the new incarnation, voids the stale window, and serves from the new record — instead
    /// of handing out old-incarnation values that would collide with the new one's.
    /// </summary>
    [Fact]
    public async Task TestExpiredLeaseDetectsARecreatedSequence()
    {
        // A one-tick lease makes every allocation after the first revalidate.
        await using EmbeddedKahunaNode node = await StartNode(blockLease: TimeSpan.FromTicks(1));
        CancellationToken ct = TestContext.Current.CancellationToken;

        string name = "lease/" + Guid.NewGuid().ToString("N");
        string storageKey = "__kahuna:sequences:" + name;

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        (SequenceResponseType first, SequenceAllocation allocation) = await Next(node, name);
        Assert.Equal(SequenceResponseType.Success, first);
        Assert.Equal(1, allocation.Start);

        // Simulate a different owner deleting and recreating the sequence behind this node's resident
        // block, writing the record directly through the same system accessors another node would use.
        (KeyValueResponseType deleted, _, _) = await SystemKeyValues(node).SystemDeleteKeyValue(storageKey, ct);
        Assert.Equal(KeyValueResponseType.Deleted, deleted);

        SequenceState recreated = new()
        {
            Name = name,
            CurrentValue = 5_000,
            InitialValue = 5_000,
            Increment = 1,
            CreatedAt = new HLCTimestamp(0, 123, 0),
            UpdatedAt = new HLCTimestamp(0, 123, 0)
        };

        (KeyValueResponseType reseeded, _, _) = await SystemKeyValues(node).SystemSetKeyValue(
            storageKey, SequenceStateCodec.Serialize(recreated), -1, KeyValueFlags.Set, ct);
        Assert.Equal(KeyValueResponseType.Set, reseeded);

        // The resident block still held 999 old-incarnation values; the expired lease forces a
        // revalidation that voids them and allocates from the recreated record instead.
        (SequenceResponseType next, SequenceAllocation fresh) = await Next(node, name);

        Assert.Equal(SequenceResponseType.Success, next);
        Assert.Equal(5_001, fresh.Start);
    }

    // ── harness ─────────────────────────────────────────────────────────────────────────────────

    private const int DefaultBlockSize = 1000;

    /// <summary>
    /// Counts the Raft entries a run proposes against a sequence's storage key, by decoding the key-value
    /// replication payloads the batch executor is handed.
    /// </summary>
    private sealed class SequenceWriteRecorder : IPartitionBatchExecutor
    {
        private IPartitionBatchExecutor inner = null!;

        private readonly ConcurrentDictionary<string, int> writes = new();

        public IPartitionBatchExecutor Wrap(IPartitionBatchExecutor real)
        {
            inner = real;
            return this;
        }

        public int CountFor(string sequenceName) =>
            writes.GetValueOrDefault("__kahuna:sequences:" + sequenceName);

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            foreach (RaftProposalEntry entry in entries)
            {
                if (entry.Type != ReplicationTypes.KeyValues)
                    continue;

                string key = ReplicationSerializer.UnserializeKeyValueMessage(entry.Data).Key;

                if (key.StartsWith("__kahuna:sequences:", StringComparison.Ordinal))
                    writes.AddOrUpdate(key, 1, static (_, current) => current + 1);
            }

            return inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    private async Task<EmbeddedKahunaNode> StartNode(
        int blockSize = DefaultBlockSize,
        int idempotencyRetentionMax = 256,
        SequenceWriteRecorder? recorder = null,
        TimeSpan? blockLease = null
    )
    {
        EmbeddedKahunaOptions options = new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            SequencerBlockSize = blockSize,
            SequencerIdempotencyRetentionMax = idempotencyRetentionMax,
            SequencerBlockLease = blockLease ?? TimeSpan.FromSeconds(5)
        };

        if (recorder is not null)
            options.WriteBatchExecutorDecorator = recorder.Wrap;

        EmbeddedKahunaNode node = new(options, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);
        return node;
    }

    private static KeyValuesManager SystemKeyValues(EmbeddedKahunaNode node) =>
        ((KahunaManager)node.Kahuna).KeyValues;

    private static Task<(SequenceResponseType, long)> Create(
        EmbeddedKahunaNode node,
        string name,
        long initialValue = 0,
        long increment = 1,
        long? maxValue = null
    ) => node.Kahuna.LocateAndCreateSequence(
        name, initialValue, increment, maxValue, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

    private static Task<(SequenceResponseType, SequenceAllocation)> Next(
        EmbeddedKahunaNode node,
        string name,
        string? idempotencyKey = null
    ) => node.Kahuna.LocateAndNextSequenceValue(
        name, idempotencyKey, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

    private static Task<(SequenceResponseType, SequenceAllocation)> AllocateRangeRaw(
        EmbeddedKahunaNode node,
        string name,
        int count
    ) => node.Kahuna.LocateAndReserveSequenceRange(
        name, count, null, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

    private static async Task<long> AllocateNext(EmbeddedKahunaNode node, string name)
    {
        (SequenceResponseType response, SequenceAllocation allocation) = await Next(node, name);

        Assert.Equal(SequenceResponseType.Success, response);
        return allocation.Start;
    }

    private static async Task<SequenceAllocation> AllocateRange(EmbeddedKahunaNode node, string name, int count)
    {
        (SequenceResponseType response, SequenceAllocation allocation) = await AllocateRangeRaw(node, name, count);

        Assert.Equal(SequenceResponseType.Success, response);
        return allocation;
    }

    /// <summary>
    /// Builds a record in the first binary format: version byte 1, and idempotency entries carrying no
    /// timestamp. Hand-built because nothing writes this shape any more.
    /// </summary>
    private static byte[] BinaryRecordWithoutEntryTimestamps(string name, long currentValue, string idempotencyKey, long allocated)
    {
        using MemoryStream stream = new();
        using BinaryWriter writer = new(stream);

        void WriteString(string value)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            writer.Write((ushort)bytes.Length);
            writer.Write(bytes);
        }

        void WriteTimestamp()
        {
            writer.Write(0);
            writer.Write(0L);
            writer.Write(0u);
        }

        writer.Write((byte)1);
        WriteString(name);
        writer.Write(currentValue);
        writer.Write(0L);           // InitialValue
        writer.Write(1L);           // Increment
        writer.Write((byte)0);      // no MaxValue
        WriteTimestamp();           // CreatedAt
        WriteTimestamp();           // UpdatedAt
        writer.Write(1);            // idempotency entry count
        WriteString(idempotencyKey);
        WriteString(name);
        writer.Write(allocated);    // Start
        writer.Write(allocated);    // End
        writer.Write(1);            // Count
        writer.Write(0L);           // Revision

        writer.Flush();
        return stream.ToArray();
    }
}
