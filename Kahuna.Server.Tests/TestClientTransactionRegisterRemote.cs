
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Drives a real client transaction session end to end over the in-process transport. Every
/// point operation issued inside a session now carries a coordinator key and a per-operation id
/// and is routed through the register-remote path: the key's partition leader records the
/// operation's confirmed effect on the coordinator before returning. These tests confirm that
/// threading is correct — writes are observable within the session (read-your-writes), the
/// coordinator's working set drives the commit, and the committed state is visible afterwards.
/// The wire serialization of the operation identity is only exercisable against the Docker
/// cluster; here the identity flows through the in-process IKahuna, so the routing and recording
/// logic is covered without the gRPC codec.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestClientTransactionRegisterRemote
{
    private readonly ILoggerFactory loggerFactory;

    public TestClientTransactionRegisterRemote(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory lf) => new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, lf);

    /// <summary>
    /// A write issued inside a session is visible to a later read in the same session, and the
    /// value survives commit — proving the register-remote operation recorded the mutation on the
    /// coordinator and the coordinator's working set carried it through 2PC.
    /// </summary>
    [Fact]
    public async Task SessionSetThenGet_CommitsAndPersistsThroughRegisterRemote()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "rr/set-get/" + Guid.NewGuid().ToString("N")[..8];

        await using (KahunaTransactionSession tx = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic },
                         TestContext.Current.CancellationToken))
        {
            await tx.SetKeyValue(key, "written-in-tx", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);

            // Read-your-writes inside the same session.
            KahunaKeyValue inTx = await tx.GetKeyValue(key, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.True(inTx.Success);
            Assert.Equal("written-in-tx", inTx.ValueAsString());

            Assert.True(await tx.Commit(TestContext.Current.CancellationToken));
        }

        // The committed value is visible outside the transaction.
        KahunaKeyValue after = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(after.Success);
        Assert.Equal("written-in-tx", after.ValueAsString());
    }

    /// <summary>
    /// A session that rolls back leaves no committed state — the register-remote mutation was
    /// recorded on the coordinator but never applied, so the key remains absent afterwards.
    /// </summary>
    [Fact]
    public async Task SessionSetThenRollback_LeavesNoCommittedState()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "rr/rollback/" + Guid.NewGuid().ToString("N")[..8];

        await using (KahunaTransactionSession tx = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic },
                         TestContext.Current.CancellationToken))
        {
            await tx.SetKeyValue(key, "discarded", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(await tx.Rollback(TestContext.Current.CancellationToken));
        }

        KahunaKeyValue after = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(after.Success);
    }

    /// <summary>
    /// On commit, the coordinator's canonical record anchor (the first confirmed persistent modified key)
    /// is folded back into the session handle, so <c>session.Handle.RecordAnchorKey</c> names that key.
    /// </summary>
    [Fact]
    public async Task SessionCommit_FoldsCanonicalRecordAnchorIntoHandle()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string first = "rr/anchor/a/" + Guid.NewGuid().ToString("N")[..8];
        string second = "rr/anchor/b/" + Guid.NewGuid().ToString("N")[..8];

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            TestContext.Current.CancellationToken);

        // Two persistent writes; the first one is the anchor.
        await tx.SetKeyValue(first, "v1", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        await tx.SetKeyValue(second, "v2", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);

        // Before commit the SDK has not yet been told the anchor.
        Assert.Null(tx.RecordAnchorKey);

        Assert.True(await tx.Commit(TestContext.Current.CancellationToken));

        // Commit folds the coordinator's canonical anchor into the handle.
        Assert.Equal(first, tx.RecordAnchorKey);
        Assert.Equal(first, tx.Handle.RecordAnchorKey);
        Assert.Equal(tx.TransactionId, tx.Handle.TransactionId);
    }

    /// <summary>
    /// A transaction that confirms no persistent write has no anchor: the folded handle carries a null
    /// <c>RecordAnchorKey</c> after commit.
    /// </summary>
    [Fact]
    public async Task SessionCommit_NoPersistentWrite_HasNoAnchor()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "rr/anchor/eph/" + Guid.NewGuid().ToString("N")[..8];

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            TestContext.Current.CancellationToken);

        await tx.SetKeyValue(key, "ephemeral", durability: KeyValueDurability.Ephemeral, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(await tx.Commit(TestContext.Current.CancellationToken));

        Assert.Null(tx.RecordAnchorKey);
        Assert.Null(tx.Handle.RecordAnchorKey);
    }

    /// <summary>
    /// An empty read-only transaction — one that reads but never writes — is a valid commit. Two-phase
    /// commit is a no-op with no modified keys, and finalize still cleans the read's MVCC snapshot.
    /// </summary>
    [Fact]
    public async Task EmptyReadOnlyTransaction_Commits()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string seeded = "rr/ro/seed/" + Guid.NewGuid().ToString("N")[..8];
        string absent = "rr/ro/absent/" + Guid.NewGuid().ToString("N")[..8];
        await client.SetKeyValue(seeded, "committed", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);

        await using (KahunaTransactionSession tx = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Optimistic },
                         TestContext.Current.CancellationToken))
        {
            KahunaKeyValue present = await tx.GetKeyValue(seeded, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.True(present.Success);

            KahunaKeyValue missing = await tx.GetKeyValue(absent, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.False(missing.Success);

            // No writes: the transaction commits validly.
            Assert.True(await tx.Commit(TestContext.Current.CancellationToken));
        }
    }

    /// <summary>
    /// An optimistic transaction stages a write with no point lock (the modified key is not in the acquired
    /// lock set), then rolls back. Finalize must clean the staged write from the modified-key set — clearing
    /// the write intent — so a following transaction can write the same key without contending with a leaked
    /// intent. This exercises the modified-key cleanup path that the acquired-lock release alone would miss.
    /// </summary>
    [Fact]
    public async Task OptimisticRollback_ClearsStagedWrite_AllowingImmediateRewrite()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "rr/opt-rollback/" + Guid.NewGuid().ToString("N")[..8];

        await using (KahunaTransactionSession tx1 = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Optimistic },
                         TestContext.Current.CancellationToken))
        {
            await tx1.SetKeyValue(key, "staged", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(await tx1.Rollback(TestContext.Current.CancellationToken));
        }

        // A second transaction writes the same key. If tx1's staged write (and its write intent) had leaked,
        // this write would contend with the lingering intent; the rollback cleanup must have cleared it.
        await using (KahunaTransactionSession tx2 = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Optimistic },
                         TestContext.Current.CancellationToken))
        {
            KahunaKeyValue write = await tx2.SetKeyValue(key, "rewritten", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(write.Success);
            Assert.True(await tx2.Commit(TestContext.Current.CancellationToken));
        }

        KahunaKeyValue after = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(after.Success);
        Assert.Equal("rewritten", after.ValueAsString());
    }

    /// <summary>
    /// After a session commits and is removed from the active map, a duplicate commit for the same handle
    /// replays the retained terminal outcome (Committed with the same record anchor) from the idempotency
    /// window, rather than reporting the transaction unknown.
    /// </summary>
    [Fact]
    public async Task DuplicateCommit_AfterSessionRemoved_ReplaysCommittedFromRetention()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "rr/retain-commit/" + Guid.NewGuid().ToString("N")[..8];

        Kahuna.Shared.KeyValue.TransactionHandle handle;

        await using (KahunaTransactionSession tx = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic },
                         TestContext.Current.CancellationToken))
        {
            await tx.SetKeyValue(key, "v", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(await tx.Commit(TestContext.Current.CancellationToken));
            handle = tx.Handle;
        }

        // The session is gone from the active map, but its outcome is still within the idempotency window.
        (KeyValueResponseType type, string? anchor) = await node.Kahuna.LocateAndCommitTransaction(handle, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Committed, type);
        Assert.Equal(key, anchor);
    }

    /// <summary>
    /// After a session rolls back and is removed, a duplicate rollback replays the retained RolledBack outcome
    /// from the idempotency window.
    /// </summary>
    [Fact]
    public async Task DuplicateRollback_AfterSessionRemoved_ReplaysRolledBackFromRetention()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "rr/retain-rollback/" + Guid.NewGuid().ToString("N")[..8];

        Kahuna.Shared.KeyValue.TransactionHandle handle;

        await using (KahunaTransactionSession tx = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic },
                         TestContext.Current.CancellationToken))
        {
            await tx.SetKeyValue(key, "v", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(await tx.Rollback(TestContext.Current.CancellationToken));
            handle = tx.Handle;
        }

        KeyValueResponseType type = await node.Kahuna.LocateAndRollbackTransaction(handle, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.RolledBack, type);
    }

    /// <summary>
    /// A finalize for a transaction the coordinator has never seen (and that is not in the idempotency window)
    /// is reported as unknown <c>Errored</c> — never a conflict <c>Aborted</c>, which would falsely imply a
    /// serialization failure on a transaction that never existed.
    /// </summary>
    [Fact]
    public async Task Finalize_ForUnknownTransaction_IsErrored_NotAborted()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        Kahuna.Shared.KeyValue.TransactionHandle unknown = new(new Kommander.Time.HLCTimestamp(1, 999, 0), Guid.NewGuid().ToString("N"));

        (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(unknown, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Errored, commitType);

        KeyValueResponseType rollbackType = await node.Kahuna.LocateAndRollbackTransaction(unknown, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Errored, rollbackType);
    }

    /// <summary>
    /// A prefix lock a pessimistic session acquires through the SDK (here via a bucket scan) now registers on
    /// the coordinator, so finalize releases it. It is genuinely held while the session is open — a second
    /// session cannot take it — and a third session can take it only after the first commits, proving the
    /// registered lock was released by finalize (before this fix it was unregistered and had no release path).
    /// </summary>
    [Fact]
    public async Task SdkPrefixLock_RegistersAndIsReleasedByFinalize()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));
        string prefix = "rr/plock/" + Guid.NewGuid().ToString("N")[..8];

        await using KahunaTransactionSession txA = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

        // Pessimistic bucket scan acquires (and now registers) an exclusive prefix lock.
        await txA.GetByBucket(prefix, KeyValueDurability.Persistent, ct);

        // While A holds the prefix lock, a second session cannot acquire the same one — proving it is held.
        await using (KahunaTransactionSession txB = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct))
        {
            await Assert.ThrowsAsync<KahunaException>(() => txB.GetByBucket(prefix, KeyValueDurability.Persistent, ct));
            await txB.Rollback(ct);
        }

        // Commit releases the registered prefix lock.
        Assert.True(await txA.Commit(ct));

        // A later session can now acquire the same prefix lock — proving finalize released it.
        await using KahunaTransactionSession txC = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        await txC.GetByBucket(prefix, KeyValueDurability.Persistent, ct);
        Assert.True(await txC.Commit(ct));
    }

    /// <summary>
    /// The same guarantee for a range lock acquired through the SDK (via a pessimistic range scan): registered
    /// on the coordinator and released by rollback, so another session can re-acquire the overlapping range.
    /// </summary>
    [Fact]
    public async Task SdkRangeLock_RegistersAndIsReleasedByFinalize()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));
        string prefix = "rr/rlock/" + Guid.NewGuid().ToString("N")[..8];

        await using KahunaTransactionSession txA = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

        // Pessimistic range scan acquires (and now registers) an exclusive range lock over the prefix.
        await txA.GetByRange(prefix, null, false, null, false, 0, default, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct);

        await using (KahunaTransactionSession txB = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct))
        {
            await Assert.ThrowsAsync<KahunaException>(() => txB.GetByRange(prefix, null, false, null, false, 0, default, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct));
            await txB.Rollback(ct);
        }

        // Rollback releases the registered range lock.
        Assert.True(await txA.Rollback(ct));

        await using KahunaTransactionSession txC = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        await txC.GetByRange(prefix, null, false, null, false, 0, default, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct);
        Assert.True(await txC.Commit(ct));
    }
}
