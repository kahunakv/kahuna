
using System.Text;
using Kahuna;
using Kahuna.Client;
using Kahuna.Server.KeyValues.Transactions.Data;
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
    /// A transaction with <see cref="ReadValidation.TrackAndValidate"/> records a read dependency on the
    /// latest committed value it observed. If a concurrent transaction changes that key before commit, the read
    /// is invalidated and the commit aborts rather than committing on a stale read. Read-set validation is
    /// driven by the read-validation policy, not the locking mode, so the transaction opts into it explicitly.
    /// </summary>
    [Fact]
    public async Task TrackedRead_InvalidatedByConcurrentWrite_AbortsCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        string read = "cp/optimistic/read/" + Guid.NewGuid().ToString("N")[..8];
        string write = "cp/optimistic/write/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue(read, "v0", cancellationToken: ct);

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic, ReadValidation = ReadValidation.TrackAndValidate }, ct);

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
                    new() { Locking = KeyValueTransactionLocking.Optimistic, ReadValidation = ReadValidation.TrackAndValidate }, ct);

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
    /// Read-set validation is decoupled from the locking mode: an optimistic transaction with
    /// <see cref="ReadValidation.None"/> takes last-value reads without an OCC check, so a concurrent write to
    /// a key it read does not invalidate the commit. This is the guarantee the earlier coupling denied —
    /// optimistic locking used to force validation regardless of the declared read policy.
    /// </summary>
    [Fact]
    public async Task OptimisticReadValidationNone_CommitsDespiteConcurrentWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        string read = "cp/none/read/" + Guid.NewGuid().ToString("N")[..8];
        string write = "cp/none/write/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue(read, "v0", cancellationToken: ct);

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic, ReadValidation = ReadValidation.None }, ct);

        // Observe the latest value; with ReadValidation.None this records no validated read dependency.
        KahunaKeyValue observed = await tx.GetKeyValue(read, KeyValueDurability.Persistent, ct);
        Assert.Equal("v0", observed.ValueAsString());

        // A concurrent committed write moves the observed key forward.
        await client.SetKeyValue(read, "v1", cancellationToken: ct);

        // A mutation forces 2PC. Because no read-set validation runs, the concurrent change does not abort.
        await tx.SetKeyValue(write, "w", durability: KeyValueDurability.Persistent, cancellationToken: ct);
        Assert.True(await tx.Commit(ct));

        KahunaKeyValue writeAfter = await client.GetKeyValue(write, KeyValueDurability.Persistent, cancellationToken: ct);
        Assert.True(writeAfter.Success);
    }

    /// <summary>
    /// The read-validation policy is honored independently of the locking mode in the other direction too: a
    /// pessimistic transaction with <see cref="ReadValidation.TrackAndValidate"/> is a valid combination that
    /// runs read-set validation and commits when its reads did not change.
    /// </summary>
    [Fact]
    public async Task PessimisticTrackAndValidate_ValidatesReadSet_AndCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        string read = "cp/pv/read/" + Guid.NewGuid().ToString("N")[..8];
        string write = "cp/pv/write/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue(read, "v0", cancellationToken: ct);

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic, ReadValidation = ReadValidation.TrackAndValidate }, ct);

        KahunaKeyValue observed = await tx.GetKeyValue(read, KeyValueDurability.Persistent, ct);
        Assert.Equal("v0", observed.ValueAsString());

        await tx.SetKeyValue(write, "w", durability: KeyValueDurability.Persistent, cancellationToken: ct);
        Assert.True(await tx.Commit(ct));
    }

    /// <summary>
    /// Begin range-validates every policy enum. An out-of-range ordinal — which an external Begin could produce
    /// by casting an unknown numeric wire value — is rejected with <see cref="KeyValueResponseType.InvalidInput"/>
    /// rather than falling into whichever equality branch happens to match.
    /// </summary>
    [Fact]
    public async Task UnknownPolicyEnum_RejectedAtBegin()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        async Task<KeyValueResponseType> Begin(KeyValueTransactionOptions options)
        {
            (KeyValueResponseType type, _) = await manager.LocateAndStartTransaction(options, ct);
            return type;
        }

        // A well-formed baseline is accepted (anything other than InvalidInput).
        Assert.NotEqual(KeyValueResponseType.InvalidInput,
            await Begin(new() { CoordinatorKey = "cp/enum/ok/" + Guid.NewGuid().ToString("N")[..8] }));

        // Each policy enum, out of range, is rejected.
        Assert.Equal(KeyValueResponseType.InvalidInput, await Begin(new()
        {
            CoordinatorKey = "cp/enum/lock/" + Guid.NewGuid().ToString("N")[..8],
            Locking = (KeyValueTransactionLocking)99
        }));
        Assert.Equal(KeyValueResponseType.InvalidInput, await Begin(new()
        {
            CoordinatorKey = "cp/enum/read/" + Guid.NewGuid().ToString("N")[..8],
            ReadValidation = (ReadValidation)99
        }));
        Assert.Equal(KeyValueResponseType.InvalidInput, await Begin(new()
        {
            CoordinatorKey = "cp/enum/dur/" + Guid.NewGuid().ToString("N")[..8],
            DecisionDurability = (DecisionDurability)99
        }));
    }

    /// <summary>
    /// A transaction's read timestamp is a per-transaction property, not a per-call one: a session opened with a
    /// caller snapshot observes that one pinned snapshot through every read path — point reads, bucket reads,
    /// full range scans, and paginated range scans — never a mix of pinned and latest state.
    /// </summary>
    [Fact]
    public async Task SessionSnapshot_ConsistentAcrossPointBucketAndRangeScans()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        KahunaClient client = Connect(node);

        string prefix = "cp/snap/" + Guid.NewGuid().ToString("N")[..8];
        string ka = prefix + "/a";
        string kb = prefix + "/b";

        await client.SetKeyValue(ka, "v0", cancellationToken: ct);
        await client.SetKeyValue(kb, "v0", cancellationToken: ct);

        // Pin a snapshot after v0, then move both keys to v1.
        HLCTimestamp snapshot = node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
        await client.SetKeyValue(ka, "v1", cancellationToken: ct);
        await client.SetKeyValue(kb, "v1", cancellationToken: ct);

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic, ReadTimestamp = snapshot, ReadValidation = ReadValidation.None }, ct);

        // Point read observes the pinned snapshot.
        Assert.Equal("v0", (await tx.GetKeyValue(ka, KeyValueDurability.Persistent, ct)).ValueAsString());

        // Bucket read observes the pinned snapshot for every key.
        List<KahunaKeyValue> bucket = await tx.GetByBucket(prefix, KeyValueDurability.Persistent, ct);
        Assert.Equal(2, bucket.Count);
        Assert.All(bucket, kv => Assert.Equal("v0", kv.ValueAsString()));

        // Full range scan observes the pinned snapshot — before the fix this used the latest state (v1).
        KeyValueGetByRangePageResult range = await tx.GetByRange(
            prefix, null, true, null, false, 0, KeyValueDurability.Persistent, RangeLockMode.Shared, ct);
        Assert.Equal(2, range.Items.Count);
        Assert.All(range.Items, it => Assert.Equal("v0", Encoding.UTF8.GetString(it.Value!)));

        // A paginated range scan (one item per page, continued by moving the start key) stays on the same
        // snapshot across pages.
        KeyValueGetByRangePageResult page1 = await tx.GetByRange(
            prefix, null, true, null, false, 1, KeyValueDurability.Persistent, RangeLockMode.Shared, ct);
        Assert.Single(page1.Items);
        Assert.Equal("v0", Encoding.UTF8.GetString(page1.Items[0].Value!));

        KeyValueGetByRangePageResult page2 = await tx.GetByRange(
            prefix, page1.Items[^1].Key, false, null, false, 1, KeyValueDurability.Persistent, RangeLockMode.Shared, ct);
        Assert.Single(page2.Items);
        Assert.Equal("v0", Encoding.UTF8.GetString(page2.Items[0].Value!));
    }

    /// <summary>
    /// The decision-durability policy is captured at Begin: a default session records BestEffort while an
    /// explicit Durable session records Durable. Durable drives real behavior at commit — an anchored, replicated
    /// coordinator decision, per-partition-leader recovery, and admission bounding — so this asserts the policy is
    /// carried through the coordinator's session state where that behavior keys off it.
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
