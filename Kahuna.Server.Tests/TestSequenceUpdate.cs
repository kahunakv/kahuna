using System.Collections.Concurrent;
using System.Diagnostics;
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
/// The sequence update operation: setting the current value, changing the parameters, and — the reason
/// the operation is shaped the way it is — doing so without letting a value be issued twice by accident.
///
/// <para>A reserved block is served with no storage traffic at all, so a node that has lost the
/// sequence's partition without noticing keeps issuing from the window it already holds. An update that
/// answered immediately would report a guarantee that does not hold for another lease period. Three
/// tests here pin that down separately rather than as one racy scenario: the window exists and an
/// instant update would reissue; a bumped incarnation voids a resident window that differs in nothing
/// else; and the real operation outlives the window on both sides before anything new is issued.</para>
/// </summary>
public sealed class TestSequenceUpdate
{
    private readonly ILoggerFactory loggerFactory;

    public TestSequenceUpdate(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    // ── the operation ───────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TestUpdateSetsTheCurrentValue()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "setval/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);
        Assert.Equal(1, (await AllocateNext(node, name)));

        (SequenceResponseType response, long revision) = await Update(node, name, new SequenceUpdate(CurrentValue: 5_000));

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.True(revision >= 0);

        Assert.Equal(5_001, await AllocateNext(node, name));

        ReadOnlySequenceEntry sequence = await Get(node, name);
        Assert.Equal(1, sequence.Incarnation);
    }

    /// <summary>
    /// Lowering the current value makes the sequence reissue values it has already handed out. That is
    /// what <c>setval</c> is for, and the guarantee this subsystem makes is scoped to one incarnation
    /// rather than to the name — so the reissue is the documented outcome, not a defect.
    /// </summary>
    [Fact]
    public async Task TestUpdateDownwardsDeliberatelyReissuesValues()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "restart/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        Assert.Equal(1, await AllocateNext(node, name));
        Assert.Equal(2, await AllocateNext(node, name));

        Assert.Equal(SequenceResponseType.Success, (await Update(node, name, new SequenceUpdate(CurrentValue: 0))).Item1);

        Assert.Equal(1, await AllocateNext(node, name));
    }

    [Fact]
    public async Task TestUpdateChangesIncrementAndMaximum()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 4);
        string name = "params/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);
        Assert.Equal(1, await AllocateNext(node, name));

        Assert.Equal(SequenceResponseType.Success, (await Update(node, name, new SequenceUpdate(
            CurrentValue: 100,
            Increment: 10,
            MaxValue: 130))).Item1);

        ReadOnlySequenceEntry sequence = await Get(node, name);
        Assert.Equal(10, sequence.Increment);
        Assert.Equal(130, sequence.MaxValue);

        Assert.Equal(110, await AllocateNext(node, name));
        Assert.Equal(120, await AllocateNext(node, name));
        Assert.Equal(130, await AllocateNext(node, name));

        // The maximum is now reached, so the next value would pass it.
        (SequenceResponseType exhausted, _) = await Next(node, name);
        Assert.Equal(SequenceResponseType.MaxValueExceeded, exhausted);
    }

    [Fact]
    public async Task TestUpdateRemovesTheMaximum()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 2);
        string name = "unbounded/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name, maxValue: 2)).Item1);

        Assert.Equal(1, await AllocateNext(node, name));
        Assert.Equal(2, await AllocateNext(node, name));
        Assert.Equal(SequenceResponseType.MaxValueExceeded, (await Next(node, name)).Item1);

        Assert.Equal(SequenceResponseType.Success, (await Update(node, name, new SequenceUpdate(RemoveMaxValue: true))).Item1);

        ReadOnlySequenceEntry sequence = await Get(node, name);
        Assert.Null(sequence.MaxValue);

        Assert.Equal(3, await AllocateNext(node, name));
    }

    [Fact]
    public async Task TestUpdateOnAMissingSequenceReportsNotFound()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "absent/" + Guid.NewGuid().ToString("N");

        (SequenceResponseType response, _) = await Update(node, name, new SequenceUpdate(CurrentValue: 10));

        Assert.Equal(SequenceResponseType.NotFound, response);
    }

    /// <summary>
    /// Everything an update can be refused for, in one place. Nothing is written by any of them: the
    /// record is unchanged and still allocates from where it was.
    /// </summary>
    [Fact]
    public async Task TestUpdateRejectsUnusableChangeSets()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "reject/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name, maxValue: 1_000)).Item1);

        SequenceUpdate[] refused =
        [
            // Nothing to change: an update that writes nothing would still break the incarnation and
            // cost the sequence a lease period of refusals for no reason.
            new(),
            new(Increment: 0),
            new(Increment: -1),
            new(BlockSize: 0),
            // Contradiction: the caller both removed the setting and supplied a value for it.
            new(MaxValue: 10, RemoveMaxValue: true),
            new(BlockSize: 10, RemoveBlockSize: true),
            // Decidable only against the folded record — the caller left the current value alone.
            new(MaxValue: -5)
        ];

        foreach (SequenceUpdate update in refused)
            Assert.Equal(SequenceResponseType.InvalidInput, (await Update(node, name, update)).Item1);

        // Untouched: still the original stream, still allocating from the original value.
        ReadOnlySequenceEntry sequence = await Get(node, name);
        Assert.Equal(0, sequence.Incarnation);
        Assert.Equal(1_000, sequence.MaxValue);
        Assert.Equal(1, await AllocateNext(node, name));
    }

    // ── the stale window ────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The hazard itself, asserted rather than described. A node holding a reserved window serves it
    /// from memory with no storage traffic, so nothing tells it the record changed until its lease
    /// forces a revalidation. Here another owner's update lands durably — written straight through the
    /// system accessor, which is exactly what an update that answered immediately would leave behind —
    /// and the node hands out a value the new record has not reserved.
    ///
    /// <para>This is why the real operation withholds success for a lease period. An implementation
    /// that answered at once would be no safer than the delete-and-recreate it replaces.</para>
    /// </summary>
    [Fact]
    public async Task TestAResidentWindowStillIssuesUntilItsLeaseExpires()
    {
        // A long lease keeps the block purely in memory for the whole test, which is the condition
        // being demonstrated rather than an accident of timing.
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 1_000, blockLease: TimeSpan.FromSeconds(30));

        string name = "hazard/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);
        Assert.Equal(1, await AllocateNext(node, name));

        // Another owner performs an update and reports success at once: the record now says value 1 is
        // the high-water mark, so the next value the sequence owes anyone is 2.
        SequenceState replaced = await ReadRecord(node, name);
        replaced.CurrentValue = 1;
        replaced.Incarnation = 1;
        replaced.IncarnatedAt = new HLCTimestamp(0, 1, 0);
        await WriteRecord(node, name, replaced);

        (SequenceResponseType response, SequenceAllocation allocation) = await Next(node, name);

        // It issues 2 out of the window it reserved before the update — the same 2 the new incarnation
        // will hand to its own next caller. One value, two callers.
        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(2, allocation.Start);

        // And it did so without touching storage: the record still carries the replacement's mark.
        Assert.Equal(1, (await ReadRecord(node, name)).CurrentValue);
    }

    /// <summary>
    /// The incarnation counter is what makes an update visible to a block holder. The record written
    /// here differs from the resident one in <c>Incarnation</c> and in nothing else — same
    /// <c>CreatedAt</c>, <c>InitialValue</c> and <c>Increment</c>, which is exactly the shape a
    /// <c>setval</c> produces. Without the counter the revalidation would read it as the same stream
    /// and keep draining the pre-update window.
    /// </summary>
    [Fact]
    public async Task TestABumpedIncarnationVoidsAResidentWindow()
    {
        // One tick of lease makes every allocation after the first revalidate.
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 1_000, blockLease: TimeSpan.FromTicks(1));

        string name = "incarnation/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);
        Assert.Equal(1, await AllocateNext(node, name));

        SequenceState replaced = await ReadRecord(node, name);
        Assert.Equal(0, replaced.Incarnation);

        replaced.CurrentValue = 1;
        replaced.Incarnation = 1;
        // Stamped far enough in the past that the allocation hold has already expired: this test is
        // about the window being voided, and the hold has its own test below.
        replaced.IncarnatedAt = new HLCTimestamp(0, 1, 0);
        await WriteRecord(node, name, replaced);

        (SequenceResponseType response, SequenceAllocation allocation) = await Next(node, name);

        // The resident window still held 999 values of the replaced stream. They are gone; the value
        // comes from the record, and the block was re-reserved above it.
        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(2, allocation.Start);
        Assert.Equal(1_001, (await ReadRecord(node, name)).CurrentValue);
    }

    /// <summary>
    /// Both halves of the closure, on the real entry points.
    ///
    /// <para>Half one: the update does not answer until a window reserved from the replaced incarnation
    /// can no longer be served anywhere. Half two: for that same interval no caller gets a value out of
    /// the new incarnation either — which is the half that matters, because the workload's allocations
    /// arrive long before the operator's update returns.</para>
    ///
    /// <para>Also pinned here: the wait does not occupy the actor. The refusal below is answered while
    /// the update is still in flight, and answered promptly.</para>
    /// </summary>
    [Fact]
    public async Task TestUpdateOutlivesTheWindowAndHoldsAllocationsOffMeanwhile()
    {
        TimeSpan lease = TimeSpan.FromSeconds(1);

        await using EmbeddedKahunaNode node = await StartNode(blockLease: lease);
        CancellationToken ct = TestContext.Current.CancellationToken;

        string name = "closure/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);
        Assert.Equal(1, await AllocateNext(node, name));

        long startedAt = Stopwatch.GetTimestamp();

        Task<(SequenceResponseType, long)> update = node.Kahuna.LocateAndUpdateSequence(
            name, new SequenceUpdate(CurrentValue: 5_000), SequenceDurability.Persistent, ct);

        // Well inside the lease, and long after the durable write itself has landed.
        await Task.Delay(TimeSpan.FromMilliseconds(300), ct);

        long probeStartedAt = Stopwatch.GetTimestamp();
        (SequenceResponseType duringWait, _) = await Next(node, name);
        TimeSpan probeTook = Stopwatch.GetElapsedTime(probeStartedAt);

        Assert.False(update.IsCompleted, "the update answered before the stale window could have closed");
        Assert.Equal(SequenceResponseType.MustRetry, duringWait);

        // Answered out of the actor while the update's wait was still running, so the wait is not
        // holding the actor's mailbox and unrelated sequences on it keep allocating.
        Assert.True(probeTook < TimeSpan.FromMilliseconds(250), $"the refusal took {probeTook.TotalMilliseconds} ms, which suggests the actor was blocked");

        (SequenceResponseType response, _) = await update;
        TimeSpan updateTook = Stopwatch.GetElapsedTime(startedAt);

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.True(updateTook >= lease, $"the update answered after {updateTook.TotalMilliseconds} ms, inside the {lease.TotalMilliseconds} ms window");

        // Once it has answered, the sequence allocates again — from the new stream.
        Assert.Equal(5_001, await AllocateNext(node, name));
    }

    /// <summary>
    /// Revalidation is the only thing that ever voids a stale window, and this setting turns it off. No
    /// wait is long enough on such a node, so the operation is refused instead of answered with a
    /// guarantee the node cannot keep.
    /// </summary>
    [Fact]
    public async Task TestUpdateIsRefusedWhenTheBlockLeaseIsDisabled()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockLease: TimeSpan.Zero);
        string name = "nolease/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        (SequenceResponseType response, _) = await Update(node, name, new SequenceUpdate(CurrentValue: 10));

        Assert.Equal(SequenceResponseType.InvalidInput, response);

        // Refused, not half-applied: the record is still the original stream.
        Assert.Equal(0, (await Get(node, name)).Incarnation);
    }

    // ── incarnation accounting ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// One update is one break. The actor re-reads and re-folds the record on a lost compare-and-swap,
    /// so a retry inside the operation must not carry a bumped counter forward from the attempt that
    /// failed — a second bump would void a window a legitimate owner had just re-established.
    /// </summary>
    [Fact]
    public async Task TestOneUpdateBreaksTheIncarnationExactlyOnce()
    {
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 2);
        string name = "counter/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        // Traffic against the same record while the update runs, so its compare-and-swap has something
        // to lose to and the retry path is the one under test.
        Task<long>[] contention =
        [
            AllocateNextTolerating(node, name),
            AllocateNextTolerating(node, name),
            AllocateNextTolerating(node, name)
        ];

        (SequenceResponseType response, _) = await Update(node, name, new SequenceUpdate(CurrentValue: 900));
        await Task.WhenAll(contention);

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.Equal(1, (await Get(node, name)).Incarnation);

        Assert.Equal(SequenceResponseType.Success, (await Update(node, name, new SequenceUpdate(CurrentValue: 950))).Item1);
        Assert.Equal(2, (await Get(node, name)).Incarnation);
    }

    /// <summary>
    /// Recorded allocations belong to the incarnation being replaced. Replaying one after a restart
    /// would hand back values the new incarnation never reserved — a duplicate by a different route —
    /// so the map does not survive the update.
    /// </summary>
    [Fact]
    public async Task TestIdempotencyDoesNotSurviveAnUpdate()
    {
        await using EmbeddedKahunaNode node = await StartNode();
        string name = "keyed/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);

        (SequenceResponseType first, SequenceAllocation recorded) = await Next(node, name, "charge-7");
        Assert.Equal(SequenceResponseType.Success, first);
        Assert.Equal(1, recorded.Start);

        // Replays before the update.
        Assert.Equal(recorded, (await Next(node, name, "charge-7")).Item2);

        Assert.Equal(SequenceResponseType.Success, (await Update(node, name, new SequenceUpdate(CurrentValue: 7_000))).Item1);

        (SequenceResponseType afterUpdate, SequenceAllocation fresh) = await NextTolerating(node, name, "charge-7");

        Assert.Equal(SequenceResponseType.Success, afterUpdate);
        Assert.Equal(7_001, fresh.Start);
    }

    // ── per-sequence block size ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The server-wide block size is what a node amortizes its commits over, and it is the wrong lever
    /// for one gap-free register: lowering it to 1 costs every sequence on the node a commit per value.
    /// A per-sequence size lets one sequence pay that price alone.
    /// </summary>
    [Fact]
    public async Task TestPerSequenceBlockSizeOverridesTheServerSetting()
    {
        SequenceWriteRecorder recorder = new();
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 1_000, recorder: recorder);

        string gapFree = "invoices/" + Guid.NewGuid().ToString("N");
        string amortized = "events/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, gapFree, blockSize: 1)).Item1);
        Assert.Equal(SequenceResponseType.Success, (await Create(node, amortized)).Item1);

        // Creating a sequence writes its record, so the allocations are counted from there rather than
        // from zero.
        int gapFreeBaseline = recorder.CountFor(gapFree);
        int amortizedBaseline = recorder.CountFor(amortized);

        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(i + 1, await AllocateNext(node, gapFree));
            Assert.Equal(i + 1, await AllocateNext(node, amortized));
        }

        // One commit per value against its own record, versus one commit covering all five.
        Assert.Equal(gapFreeBaseline + 5, recorder.CountFor(gapFree));
        Assert.Equal(amortizedBaseline + 1, recorder.CountFor(amortized));

        // Reserved exactly what was issued: nothing is left to abandon, so nothing becomes a gap.
        Assert.Equal(5, (await Get(node, gapFree)).CurrentValue);
        Assert.Equal(1, (await Get(node, gapFree)).BlockSize);
        Assert.Equal(1_000, (await Get(node, amortized)).CurrentValue);
        Assert.Null((await Get(node, amortized)).BlockSize);
    }

    /// <summary>
    /// The point of the setting: a restart is what turns an unissued block tail into a gap, and a size
    /// of one leaves no tail to lose.
    /// </summary>
    [Fact]
    public async Task TestBlockSizeOfOneIsGapFreeAcrossARestart()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-seq-gapfree-store-");
        string walPath = CreateTempDir("kahuna-seq-gapfree-wal-");

        try
        {
            string name = "ledger/" + Guid.NewGuid().ToString("N");
            List<long> issued = [];

            await using (EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory))
            {
                await node.StartAsync(ct);

                Assert.Equal(SequenceResponseType.Success, (await Create(node, name, blockSize: 1)).Item1);

                for (int i = 0; i < 3; i++)
                    issued.Add(await AllocateNext(node, name));
            }

            await using (EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory))
            {
                await node.StartAsync(ct);

                for (int i = 0; i < 3; i++)
                    issued.Add(await AllocateNext(node, name));
            }

            // The server-wide size on these options is 100. A sequence following it would resume at 101.
            Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], issued);
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    /// <summary>
    /// A null block size is not "the server setting as it was at create time" — it is resolved on every
    /// reservation. Freezing it would leave an operator who retunes the node with old sequences quietly
    /// ignoring the change.
    /// </summary>
    [Fact]
    public async Task TestUpdateSetsAndRemovesThePerSequenceBlockSize()
    {
        SequenceWriteRecorder recorder = new();
        await using EmbeddedKahunaNode node = await StartNode(blockSize: 1_000, recorder: recorder);

        string name = "cache/" + Guid.NewGuid().ToString("N");

        Assert.Equal(SequenceResponseType.Success, (await Create(node, name)).Item1);
        Assert.Equal(1, await AllocateNext(node, name));

        Assert.Equal(SequenceResponseType.Success, (await Update(node, name, new SequenceUpdate(BlockSize: 1))).Item1);
        Assert.Equal(1, (await Get(node, name)).BlockSize);

        int beforeGapFree = recorder.CountFor(name);

        for (int i = 0; i < 4; i++)
            await AllocateNext(node, name);

        Assert.Equal(beforeGapFree + 4, recorder.CountFor(name));

        Assert.Equal(SequenceResponseType.Success, (await Update(node, name, new SequenceUpdate(RemoveBlockSize: true))).Item1);
        Assert.Null((await Get(node, name)).BlockSize);

        int beforeAmortized = recorder.CountFor(name);

        for (int i = 0; i < 4; i++)
            await AllocateNext(node, name);

        // Back on the server-wide size of 1000: one bump covers the run.
        Assert.Equal(beforeAmortized + 1, recorder.CountFor(name));
    }

    // ── record compatibility ────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The three older record formats carry no incarnation, which reads correctly as "never updated".
    /// Each must load, update, and be rewritten in the current format.
    /// </summary>
    [Theory]
    [InlineData("json")]
    [InlineData("binary-v1")]
    [InlineData("binary-v2")]
    public async Task TestUpdateMigratesAnOlderRecordForward(string format)
    {
        await using EmbeddedKahunaNode node = await StartNode();
        CancellationToken ct = TestContext.Current.CancellationToken;

        string name = "legacy-" + format + "/" + Guid.NewGuid().ToString("N");

        byte[] seeded = format switch
        {
            "json" => Encoding.UTF8.GetBytes(LegacyJsonRecord(name, currentValue: 10)),
            "binary-v1" => BinaryRecordWithoutEntryTimestamps(name, currentValue: 10),
            _ => BinaryRecordWithoutIncarnation(name, currentValue: 10)
        };

        (KeyValueResponseType setResponse, _, _) = await SystemKeyValues(node).SystemSetKeyValue(
            StorageKey(name), seeded, -1, KeyValueFlags.Set, ct);

        Assert.Equal(KeyValueResponseType.Set, setResponse);

        ReadOnlySequenceEntry before = await Get(node, name);
        Assert.Equal(10, before.CurrentValue);
        Assert.Equal(0, before.Incarnation);
        Assert.Null(before.BlockSize);

        Assert.Equal(SequenceResponseType.Success, (await Update(node, name, new SequenceUpdate(CurrentValue: 400, BlockSize: 7))).Item1);

        // Rewritten in the current format, which is the only one that can carry these fields at all.
        SequenceState rewritten = await ReadRecord(node, name);
        Assert.Equal(400, rewritten.CurrentValue);
        Assert.Equal(1, rewritten.Incarnation);
        Assert.Equal(7, rewritten.BlockSize);
        Assert.NotEqual(HLCTimestamp.Zero, rewritten.IncarnatedAt);
        Assert.Empty(rewritten.Idempotency);

        Assert.Equal(401, await AllocateNext(node, name));
    }

    [Fact]
    public void TestCurrentRecordFormatRoundTripsTheNewFields()
    {
        SequenceState state = new()
        {
            Name = "round/trip",
            CurrentValue = 42,
            InitialValue = 5,
            Increment = 3,
            MaxValue = 900,
            BlockSize = 17,
            Incarnation = 4,
            IncarnatedAt = new HLCTimestamp(2, 1_700_000_000_123, 9),
            CreatedAt = new HLCTimestamp(1, 111, 2),
            UpdatedAt = new HLCTimestamp(1, 222, 3)
        };

        state.Idempotency["reserve:k"] = new(new SequenceAllocation("round/trip", 40, 42, 1, 7), new HLCTimestamp(1, 200, 0));

        SequenceState? decoded = SequenceStateCodec.Deserialize(SequenceStateCodec.Serialize(state));

        Assert.NotNull(decoded);
        Assert.Equal(state.Name, decoded.Name);
        Assert.Equal(state.CurrentValue, decoded.CurrentValue);
        Assert.Equal(state.InitialValue, decoded.InitialValue);
        Assert.Equal(state.Increment, decoded.Increment);
        Assert.Equal(state.MaxValue, decoded.MaxValue);
        Assert.Equal(state.BlockSize, decoded.BlockSize);
        Assert.Equal(state.Incarnation, decoded.Incarnation);
        Assert.Equal(state.IncarnatedAt, decoded.IncarnatedAt);
        Assert.Equal(state.CreatedAt, decoded.CreatedAt);
        Assert.Equal(state.UpdatedAt, decoded.UpdatedAt);
        Assert.Single(decoded.Idempotency);
        Assert.Equal(state.Idempotency["reserve:k"], decoded.Idempotency["reserve:k"]);

        // A record with no per-sequence block size encodes the absence rather than a sentinel.
        state.BlockSize = null;
        Assert.Null(SequenceStateCodec.Deserialize(SequenceStateCodec.Serialize(state))!.BlockSize);
    }

    // ── harness ─────────────────────────────────────────────────────────────────────────────────

    private const int DefaultBlockSize = 1000;

    /// <summary>
    /// Counts the Raft entries a run proposes against a sequence's storage key, by decoding the
    /// key-value replication payloads the batch executor is handed. A green functional test cannot tell
    /// a block size of 1 from a block size of 1000; the commit count can.
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

        public int CountFor(string sequenceName) => writes.GetValueOrDefault(StorageKey(sequenceName));

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
            // Short by default: an update withholds its answer for a whole lease, so the server default
            // of five seconds would make this file's runtime the lease rather than the work.
            SequencerBlockLease = blockLease ?? TimeSpan.FromMilliseconds(150)
        };

        if (recorder is not null)
            options.WriteBatchExecutorDecorator = recorder.Wrap;

        EmbeddedKahunaNode node = new(options, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);
        return node;
    }

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        // Stable, non-empty revisions so the reconstructed node reopens the same database and WAL files.
        StorageRevision = "sequence-update-restart",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "sequence-update-restart-wal",
        WalSyncWrites = true,
        SequencerBlockSize = 100,
        SequencerBlockLease = TimeSpan.FromMilliseconds(150)
    };

    private static string StorageKey(string name) => "__kahuna:sequences:" + name;

    private static KeyValuesManager SystemKeyValues(EmbeddedKahunaNode node) => ((KahunaManager)node.Kahuna).KeyValues;

    private static Task<(SequenceResponseType, long)> Create(
        EmbeddedKahunaNode node,
        string name,
        long initialValue = 0,
        long increment = 1,
        long? maxValue = null,
        int? blockSize = null
    ) => node.Kahuna.LocateAndCreateSequence(
        name, initialValue, increment, maxValue, blockSize, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

    private static Task<(SequenceResponseType, long)> Update(EmbeddedKahunaNode node, string name, SequenceUpdate update) =>
        node.Kahuna.LocateAndUpdateSequence(name, update, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

    private static Task<(SequenceResponseType, SequenceAllocation)> Next(
        EmbeddedKahunaNode node,
        string name,
        string? idempotencyKey = null
    ) => node.Kahuna.LocateAndNextSequenceValue(
        name, idempotencyKey, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

    private static async Task<ReadOnlySequenceEntry> Get(EmbeddedKahunaNode node, string name)
    {
        (SequenceResponseType response, ReadOnlySequenceEntry? sequence) = await node.Kahuna.LocateAndGetSequence(
            name, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(SequenceResponseType.Success, response);
        Assert.NotNull(sequence);
        return sequence;
    }

    /// <summary>
    /// Allocates, absorbing the retryable refusal an allocation gets while a recently updated sequence
    /// is holding its new incarnation quiet. That refusal is the contract, not a flake: a client is told
    /// the attempt consumed nothing durable and may be repeated as it is.
    /// </summary>
    private static async Task<long> AllocateNext(EmbeddedKahunaNode node, string name)
    {
        (SequenceResponseType response, SequenceAllocation allocation) = await NextTolerating(node, name, null);

        Assert.Equal(SequenceResponseType.Success, response);
        return allocation.Start;
    }

    private static async Task<(SequenceResponseType, SequenceAllocation)> NextTolerating(
        EmbeddedKahunaNode node,
        string name,
        string? idempotencyKey
    )
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        for (int attempt = 0; attempt < 100; attempt++)
        {
            (SequenceResponseType response, SequenceAllocation allocation) = await Next(node, name, idempotencyKey);

            if (response != SequenceResponseType.MustRetry)
                return (response, allocation);

            await Task.Delay(20, ct);
        }

        return (SequenceResponseType.MustRetry, default);
    }

    /// <summary>Background allocation for contention tests; the value itself is not asserted on.</summary>
    private static Task<long> AllocateNextTolerating(EmbeddedKahunaNode node, string name) =>
        Task.Run(async () => (await NextTolerating(node, name, null)).Item2.Start);

    private static async Task<SequenceState> ReadRecord(EmbeddedKahunaNode node, string name)
    {
        (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await SystemKeyValues(node).SystemGetKeyValue(
            StorageKey(name), TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, response);
        Assert.NotNull(entry?.Value);

        SequenceState? state = SequenceStateCodec.Deserialize(entry.Value);
        Assert.NotNull(state);
        return state;
    }

    /// <summary>
    /// Writes the record behind the sequence's back, through the same system accessor another node's
    /// owning actor would use. This is what a peer's mutation looks like from the point of view of a
    /// node that holds a block and has not revalidated it.
    /// </summary>
    private static async Task WriteRecord(EmbeddedKahunaNode node, string name, SequenceState state)
    {
        (KeyValueResponseType response, _, _) = await SystemKeyValues(node).SystemSetKeyValue(
            StorageKey(name), SequenceStateCodec.Serialize(state), -1, KeyValueFlags.Set, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Set, response);
    }

    private static string LegacyJsonRecord(string name, long currentValue) =>
        """
        {"name":"NAME","currentValue":VALUE,"initialValue":0,"increment":1,"maxValue":null,"createdAt":{"n":0,"l":0,"c":0},"updatedAt":{"n":0,"l":0,"c":0},"idempotency":{}}
        """
        .Replace("NAME", name)
        .Replace("VALUE", currentValue.ToString());

    /// <summary>
    /// Builds a record in the first binary format: version byte 1, idempotency entries carrying no
    /// timestamp, and no incarnation. Hand-built because nothing writes this shape any more.
    /// </summary>
    private static byte[] BinaryRecordWithoutEntryTimestamps(string name, long currentValue) =>
        LegacyBinaryRecord(name, currentValue, version: 1, entryTimestamps: false);

    /// <summary>
    /// Builds a record in the second binary format: idempotency entries carry a timestamp, but the
    /// record has no incarnation and no per-sequence block size.
    /// </summary>
    private static byte[] BinaryRecordWithoutIncarnation(string name, long currentValue) =>
        LegacyBinaryRecord(name, currentValue, version: 2, entryTimestamps: true);

    private static byte[] LegacyBinaryRecord(string name, long currentValue, byte version, bool entryTimestamps)
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

        writer.Write(version);
        WriteString(name);
        writer.Write(currentValue);
        writer.Write(0L);           // InitialValue
        writer.Write(1L);           // Increment
        writer.Write((byte)0);      // no MaxValue
        WriteTimestamp();           // CreatedAt
        WriteTimestamp();           // UpdatedAt
        writer.Write(1);            // idempotency entry count
        WriteString("reserve:legacy-key");
        WriteString(name);
        writer.Write(5L);           // Start
        writer.Write(5L);           // End
        writer.Write(1);            // Count
        writer.Write(0L);           // Revision

        if (entryTimestamps)
            WriteTimestamp();

        writer.Flush();
        return stream.ToArray();
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort test cleanup.
        }
    }
}
