
using Kahuna;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Proves that the interactive-transaction concurrency policies — locking mode, read validation,
/// snapshot read timestamp, decision durability, and the Begin-time validity check — change the
/// observable behavior of a real client session driven over the in-process transport, rather than
/// merely round-tripping through the option models. Each policy is exercised through the public
/// client API against a single embedded node.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTransactionConcurrencyPolicy
{
    private readonly ILoggerFactory loggerFactory;

    public TestTransactionConcurrencyPolicy(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory lf) => new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, lf);

    private static KahunaClient Connect(EmbeddedKahunaNode node) =>
        new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

    /// <summary>
    /// A pessimistic transaction takes an exclusive write intent on the key it touches. A second
    /// pessimistic transaction that tries to touch the same key is refused (an aborting conflict),
    /// and the intent is released when the first transaction finalizes, so a later transaction can
    /// take the key again.
    /// </summary>
    [Fact]
    public async Task PessimisticLock_BlocksCompetingTransaction_ReleasedOnFinalize()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        string key = "cp/pessimistic/" + Guid.NewGuid().ToString("N")[..8];

        await using (KahunaTransactionSession holder = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct))
        {
            await holder.SetKeyValue(key, "holder", durability: KeyValueDurability.Persistent, cancellationToken: ct);

            // A competing pessimistic transaction cannot take the same key while the holder is live.
            await using (KahunaTransactionSession competitor = await client.StartTransactionSession(
                             new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct))
            {
                KahunaException blocked = await Assert.ThrowsAsync<KahunaException>(async () =>
                    await competitor.SetKeyValue(key, "competitor", durability: KeyValueDurability.Persistent, cancellationToken: ct));
                Assert.Equal(KeyValueResponseType.Aborted, blocked.KeyValueErrorCode);

                await competitor.Rollback(ct);
            }

            // Finalizing the holder releases the intent.
            Assert.True(await holder.Commit(ct));
        }

        // With the intent released, a fresh pessimistic transaction can take the key and commit.
        await using (KahunaTransactionSession next = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct))
        {
            await next.SetKeyValue(key, "next", durability: KeyValueDurability.Persistent, cancellationToken: ct);
            Assert.True(await next.Commit(ct));
        }

        KahunaKeyValue after = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: ct);
        Assert.True(after.Success);
        Assert.Equal("next", after.ValueAsString());
    }

    /// <summary>
    /// An optimistic transaction records a read dependency on the latest committed value it observed.
    /// If a concurrent transaction changes that key before commit, the read is invalidated and the
    /// commit aborts rather than committing on a stale read.
    /// </summary>
    [Fact]
    public async Task OptimisticRead_InvalidatedByConcurrentWrite_AbortsCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        string read = "cp/optimistic/read/" + Guid.NewGuid().ToString("N")[..8];
        string write = "cp/optimistic/write/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue(read, "v0", cancellationToken: ct);

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

        // Record a read dependency on the latest committed value.
        KahunaKeyValue observed = await tx.GetKeyValue(read, KeyValueDurability.Persistent, ct);
        Assert.True(observed.Success);
        Assert.Equal("v0", observed.ValueAsString());

        // A concurrent committed write moves the observed key forward.
        await client.SetKeyValue(read, "v1", cancellationToken: ct);

        // Give the transaction a mutation so commit runs prepare + read-set validation.
        await tx.SetKeyValue(write, "w", durability: KeyValueDurability.Persistent, cancellationToken: ct);

        KahunaException aborted = await Assert.ThrowsAsync<KahunaException>(async () => await tx.Commit(ct));
        Assert.Equal(KeyValueResponseType.Aborted, aborted.KeyValueErrorCode);
        Assert.Equal(KahunaTransactionStatus.Aborted, tx.Status);

        // The aborted transaction left no write behind.
        KahunaKeyValue writeAfter = await client.GetKeyValue(write, KeyValueDurability.Persistent, cancellationToken: ct);
        Assert.False(writeAfter.Success);
    }

    /// <summary>
    /// The concurrent write-intent plus read-validation check prevents write skew: two transactions
    /// that each read both keys and, on the strength of the other key still being "on", switch their
    /// own key "off" cannot both commit that change — the invariant "at least one stays on" survives.
    /// </summary>
    [Fact]
    public async Task WriteIntentCheck_PreventsWriteSkew_AcrossConcurrentTransactions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        string a = "cp/skew/a/" + Guid.NewGuid().ToString("N")[..8];
        string b = "cp/skew/b/" + Guid.NewGuid().ToString("N")[..8];

        // Both "on".
        await client.SetKeyValue(a, "1", cancellationToken: ct);
        await client.SetKeyValue(b, "1", cancellationToken: ct);

        // Each transaction may switch its own key off only while the other is still on.
        async Task Attempt(string self, string other)
        {
            try
            {
                await using KahunaTransactionSession tx = await client.StartTransactionSession(
                    new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

                await tx.GetKeyValue(self, KeyValueDurability.Persistent, ct);
                KahunaKeyValue otherValue = await tx.GetKeyValue(other, KeyValueDurability.Persistent, ct);

                if (otherValue.Success && otherValue.ValueAsString() == "1")
                    await tx.SetKeyValue(self, "0", durability: KeyValueDurability.Persistent, cancellationToken: ct);

                await tx.Commit(ct);
            }
            catch (KahunaException)
            {
                // A definite abort or a transient retry both leave the anomaly unrealized; the
                // invariant assertion below is what proves write skew was prevented.
            }
        }

        await Task.WhenAll(Attempt(a, b), Attempt(b, a));

        KahunaKeyValue finalA = await client.GetKeyValue(a, KeyValueDurability.Persistent, cancellationToken: ct);
        KahunaKeyValue finalB = await client.GetKeyValue(b, KeyValueDurability.Persistent, cancellationToken: ct);

        // Write skew would leave both off; a serializable outcome keeps at least one on.
        Assert.True(finalA.ValueAsString() == "1" || finalB.ValueAsString() == "1");
    }

    /// <summary>
    /// A transaction opened with a caller-supplied snapshot timestamp reads the state as of that
    /// timestamp and records no read dependency, so a concurrent write after the snapshot does not
    /// invalidate the transaction and it still commits.
    /// </summary>
    [Fact]
    public async Task SnapshotRead_UsesCallerTimestamp_AddsNoReadDependency()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        string read = "cp/snapshot/read/" + Guid.NewGuid().ToString("N")[..8];
        string write = "cp/snapshot/write/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue(read, "v0", cancellationToken: ct);

        // A snapshot pinned to "now" — after v0, before v1.
        HLCTimestamp snapshot = node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());

        await client.SetKeyValue(read, "v1", cancellationToken: ct);

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic, ReadTimestamp = snapshot }, ct);

        // The snapshot read observes the value as of the pinned timestamp, not the later write.
        KahunaKeyValue observed = await tx.GetKeyValue(read, KeyValueDurability.Persistent, ct);
        Assert.True(observed.Success);
        Assert.Equal("v0", observed.ValueAsString());

        // A mutation forces 2PC. Because the snapshot read recorded no dependency, the concurrent
        // change to the read key does not invalidate the commit.
        await tx.SetKeyValue(write, "w", durability: KeyValueDurability.Persistent, cancellationToken: ct);
        Assert.True(await tx.Commit(ct));

        KahunaKeyValue writeAfter = await client.GetKeyValue(write, KeyValueDurability.Persistent, cancellationToken: ct);
        Assert.True(writeAfter.Success);
    }

    /// <summary>
    /// A snapshot read timestamp cannot be combined with tracked read validation — pinning reads to a
    /// past snapshot cannot detect writes that land after it. Begin rejects the combination up front.
    /// </summary>
    [Fact]
    public async Task InvalidPolicyCombination_SnapshotWithReadValidation_RejectedAtBegin()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        HLCTimestamp snapshot = node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());

        KahunaException rejected = await Assert.ThrowsAsync<KahunaException>(async () =>
            await client.StartTransactionSession(new()
            {
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadTimestamp = snapshot,
                ReadValidation = ReadValidation.TrackAndValidate
            }, ct));

        Assert.Equal(KeyValueResponseType.InvalidInput, rejected.KeyValueErrorCode);
    }

    /// <summary>
    /// The decision-durability policy is captured at Begin: a default session records BestEffort while
    /// an explicit Durable session records Durable. (Durable has no runtime effect yet; this confirms
    /// the policy is carried through the coordinator's session state.)
    /// </summary>
    [Fact]
    public async Task DecisionDurability_DefaultRecordsBestEffort_ExplicitRecordsDurable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        await using (KahunaTransactionSession defaulted = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct))
        {
            Assert.Equal(DecisionDurability.BestEffort, manager.GetRecordedDecisionDurability(defaulted.TransactionId));
            await defaulted.Rollback(ct);
        }

        await using (KahunaTransactionSession durable = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic, DecisionDurability = DecisionDurability.Durable }, ct))
        {
            Assert.Equal(DecisionDurability.Durable, manager.GetRecordedDecisionDurability(durable.TransactionId));
            await durable.Rollback(ct);
        }
    }
}
