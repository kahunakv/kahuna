using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using Xunit.Sdk;

namespace Kahuna.Tests.Client;

public class TestTransactionLockingAnomalies
{
    private const string Url = "https://localhost:8082";

    private static readonly string[] Urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    [Theory, CombinatorialData]
    public async Task TestLostUpdateOnOneKey(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}";
        string key = $"{prefix}/counter";

        KahunaKeyValue initialValue = await client.SetKeyValue(
            key,
            "0",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );

        Assert.True(initialValue.Success);
        Assert.Equal(0, initialValue.Revision);

        TxSchedule schedule = new();

        Task<TransactionAttempt> firstAttempt = ExecuteLostUpdateAttempt(client, key, locking, durability, schedule, cancellationToken);
        Task<TransactionAttempt> secondAttempt = ExecuteLostUpdateAttempt(client, key, locking, durability, schedule, cancellationToken);

        if (locking == KeyValueTransactionLocking.Optimistic)
        {
            await schedule.WaitForBothReads(cancellationToken);
        }
        else
        {
            await schedule.TryWaitForBothReads(TimeSpan.FromMilliseconds(250), cancellationToken);
        }

        schedule.AllowWrites();

        TransactionAttempt[] attempts = await Task.WhenAll(firstAttempt, secondAttempt);

        int committedCount = attempts.Count(attempt => attempt.Committed);
        int abortedCount = attempts.Count(attempt => attempt.ErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry);

        KahunaKeyValue finalValue = await client.GetKeyValue(key, durability, cancellationToken);

        Assert.True(finalValue.Success);
        Assert.Equal(committedCount, finalValue.ValueAsLong());
        Assert.Equal(committedCount, finalValue.Revision);

        if (locking == KeyValueTransactionLocking.Optimistic)
        {
            Assert.All(attempts, attempt => Assert.Equal(0, attempt.ReadValue));
            Assert.Equal(1, committedCount);
            Assert.Equal(1, abortedCount);
            Assert.Equal(1, finalValue.ValueAsLong());
        }
        else
        {
            Assert.InRange(committedCount, 1, 2);
            Assert.InRange(finalValue.ValueAsLong(), 1, 2);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestStaleReadFollowedByWrite(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}";
        string keyA = $"{prefix}/a";
        string keyB = $"{prefix}/b";

        KahunaKeyValue initialA = await client.SetKeyValue(
            keyA,
            "10",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        KahunaKeyValue initialB = await client.SetKeyValue(
            keyB,
            "0",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );

        Assert.True(initialA.Success);
        Assert.True(initialB.Success);

        StaleReadSchedule schedule = new();

        Task<StaleReadAttempt> staleWriterTask = ExecuteStaleReadAttempt(
            client,
            keyA,
            keyB,
            locking,
            durability,
            schedule,
            cancellationToken
        );

        await schedule.WaitForRead(cancellationToken);

        Task<SimpleCommitAttempt> freshWriterTask = ExecuteFreshWriterAttempt(
            client,
            keyA,
            locking,
            durability,
            cancellationToken
        );

        bool freshWriterCommittedBeforeStaleWrite = await TryAwaitSuccess(freshWriterTask, TimeSpan.FromMilliseconds(250), cancellationToken);

        schedule.AllowWrite();

        StaleReadAttempt staleWriter = await staleWriterTask;
        SimpleCommitAttempt freshWriter = await freshWriterTask;

        KahunaKeyValue finalA = await client.GetKeyValue(keyA, durability, cancellationToken);
        KahunaKeyValue finalB = await client.GetKeyValue(keyB, durability, cancellationToken);

        Assert.True(finalA.Success);
        Assert.True(finalB.Success);

        if (locking == KeyValueTransactionLocking.Optimistic)
        {
            Assert.Equal(10, staleWriter.ReadValue);
            Assert.True(freshWriterCommittedBeforeStaleWrite);
            Assert.True(freshWriter.Committed);
            Assert.False(staleWriter.Committed);
            Assert.Equal(20, finalA.ValueAsLong());
            Assert.Equal(0, finalB.ValueAsLong());
        }
        else
        {
            Assert.True(staleWriter.ReadValue is 10 or 20);

            if (freshWriterCommittedBeforeStaleWrite)
            {
                Assert.False(staleWriter.Committed);
            }
            else if (staleWriter.Committed)
            {
                Assert.Equal(11, finalB.ValueAsLong());
            }
        }

        Assert.False(freshWriterCommittedBeforeStaleWrite && staleWriter.Committed && staleWriter.ReadValue == 10);
        Assert.False(finalA.ValueAsLong() == 20 && finalB.ValueAsLong() == 11 && freshWriterCommittedBeforeStaleWrite && staleWriter.Committed);
    }

    [Theory, CombinatorialData]
    public async Task TestWriteSkewOnConcreteKeysInteractive(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/doctors";
        string aliceKey = $"{prefix}/alice";
        string bobKey = $"{prefix}/bob";

        await SeedDoctors(client, aliceKey, bobKey, durability, cancellationToken);

        TxSchedule schedule = new();

        Task<WriteSkewAttempt> aliceAttemptTask = ExecuteInteractiveWriteSkewAttempt(
            client,
            aliceKey,
            bobKey,
            aliceKey,
            locking,
            durability,
            schedule,
            cancellationToken
        );
        Task<WriteSkewAttempt> bobAttemptTask = ExecuteInteractiveWriteSkewAttempt(
            client,
            aliceKey,
            bobKey,
            bobKey,
            locking,
            durability,
            schedule,
            cancellationToken
        );

        if (locking == KeyValueTransactionLocking.Optimistic)
        {
            await schedule.WaitForBothReads(cancellationToken);
        }
        else
        {
            await schedule.TryWaitForBothReads(TimeSpan.FromMilliseconds(250), cancellationToken);
        }

        schedule.AllowWrites();

        WriteSkewAttempt[] attempts = await Task.WhenAll(aliceAttemptTask, bobAttemptTask);
        int committedCount = attempts.Count(attempt => attempt.Committed);
        int abortedCount = attempts.Count(attempt => attempt.ErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry);

        KahunaKeyValue finalAlice = await client.GetKeyValue(aliceKey, durability, cancellationToken);
        KahunaKeyValue finalBob = await client.GetKeyValue(bobKey, durability, cancellationToken);

        Assert.True(finalAlice.Success);
        Assert.True(finalBob.Success);

        bool aliceOnCall = finalAlice.ValueAsBool();
        bool bobOnCall = finalBob.ValueAsBool();

        Assert.True(aliceOnCall || bobOnCall);

        // Only assert exactly-one-off-call when someone actually committed a write;
        // when both abort (mutual write-intent conflict), both remain on-call which is still safe.
        if (attempts.Any(a => a.Committed && a.AttemptedWrite))
            Assert.NotEqual(aliceOnCall, bobOnCall);

        if (locking == KeyValueTransactionLocking.Optimistic)
        {
            Assert.All(attempts, attempt =>
            {
                Assert.True(attempt.AliceWasTrue);
                Assert.True(attempt.BobWasTrue);
                Assert.True(attempt.AttemptedWrite);
            });

            // Mutual write-intent conflict may cause both transactions to abort (0 commits); that is
            // still correct isolation — the invariant above confirms no write skew occurred.
            Assert.InRange(committedCount, 0, 1);
            Assert.InRange(abortedCount, 1, 2);
        }
        else
        {
            if (committedCount == 2)
            {
                Assert.Contains(attempts, attempt => attempt.AttemptedWrite);
                Assert.Contains(attempts, attempt => !attempt.AttemptedWrite);
            }
        }

        Assert.False(attempts.All(attempt => attempt.Committed && attempt.AttemptedWrite));
    }

    [Theory, CombinatorialData]
    public async Task TestWriteSkewOnConcreteKeysScriptGetByBucket(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/doctors";
        string aliceKey = $"{prefix}/alice";
        string bobKey = $"{prefix}/bob";

        await SeedDoctors(client, aliceKey, bobKey, durability, cancellationToken);

        string script = GetWriteSkewBucketScript(locking, durability);

        Task<ScriptAttempt> aliceScriptTask = ExecuteWriteSkewScriptAttempt(
            client,
            script,
            prefix,
            aliceKey,
            cancellationToken
        );
        Task<ScriptAttempt> bobScriptTask = ExecuteWriteSkewScriptAttempt(
            client,
            script,
            prefix,
            bobKey,
            cancellationToken
        );

        ScriptAttempt[] attempts = await Task.WhenAll(aliceScriptTask, bobScriptTask);
        int committedCount = attempts.Count(attempt => attempt.Committed);
        int abortedCount = attempts.Count(attempt => attempt.ErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry);

        KahunaKeyValue finalAlice = await client.GetKeyValue(aliceKey, durability, cancellationToken);
        KahunaKeyValue finalBob = await client.GetKeyValue(bobKey, durability, cancellationToken);

        Assert.True(finalAlice.Success);
        Assert.True(finalBob.Success);

        bool aliceOnCall = finalAlice.ValueAsBool();
        bool bobOnCall = finalBob.ValueAsBool();

        Assert.True(aliceOnCall || bobOnCall);

        // Only assert exactly-one-off-call when someone actually committed a write;
        // when both abort (write-intent conflict during validation), both remain on-call which is still safe.
        if (committedCount > 0)
            Assert.NotEqual(aliceOnCall, bobOnCall);

        if (locking == KeyValueTransactionLocking.Optimistic)
        {
            // Mutual write-intent conflict may cause both transactions to abort (0 commits); that is
            // still correct isolation — the invariant above confirms no write skew occurred.
            Assert.InRange(committedCount, 0, 1);
            Assert.InRange(abortedCount, 1, 2);

            // Verify the anomaly was prevented: both committing with writes would be write skew.
            Assert.False(attempts.All(attempt => attempt.Committed));
        }
        else
        {
            Assert.InRange(committedCount, 1, 2);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestBucketMvccVisibilityAndReturnedKeyConflicts(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/items";
        string keyA = $"{prefix}/a";
        string keyB = $"{prefix}/b";
        string keyDeleted = $"{prefix}/deleted";
        string keyExpired = $"{prefix}/expired";
        string keyInflight = $"{prefix}/inflight";
        string summaryKey = $"{prefix}/summary";

        await SeedBucketVisibilityState(client, keyA, keyB, keyDeleted, keyExpired, durability, cancellationToken);

        await using KahunaTransactionSession inflightSession = await client.StartTransactionSession(
            new()
            {
                Locking = KeyValueTransactionLocking.Optimistic,
                Timeout = 5000
            },
            cancellationToken
        );

        KahunaKeyValue inflightWrite = await inflightSession.SetKeyValue(
            keyInflight,
            "hidden",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(inflightWrite.Success);

        await using KahunaTransactionSession bucketSession = await client.StartTransactionSession(
            new()
            {
                Locking = locking,
                Timeout = 5000
            },
            cancellationToken
        );

        List<KahunaKeyValue> bucketItems = await bucketSession.GetByBucket(prefix, durability, cancellationToken);
        string[] bucketKeys = bucketItems
            .Select(item => item.Key)
            .OrderBy(key => key, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal([keyA, keyB], bucketKeys);
        Assert.DoesNotContain(bucketItems, item => item.Key == keyDeleted);
        Assert.DoesNotContain(bucketItems, item => item.Key == keyExpired);
        Assert.DoesNotContain(bucketItems, item => item.Key == keyInflight);

        Task<SimpleCommitAttempt> updaterTask = ExecuteValueUpdateAttempt(
            client,
            keyA,
            "new-a",
            locking,
            durability,
            cancellationToken
        );

        bool updaterCommittedBeforeBucketCommit = await TryAwaitSuccess(
            updaterTask,
            TimeSpan.FromMilliseconds(250),
            cancellationToken
        );

        KahunaKeyValue summaryWrite = await bucketSession.SetKeyValue(
            summaryKey,
            bucketItems.Count.ToString(),
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(summaryWrite.Success);

        CommitAttempt bucketCommit = await CommitTransactionAttempt(bucketSession, cancellationToken);
        SimpleCommitAttempt updater = await updaterTask;

        KahunaKeyValue finalA = await client.GetKeyValue(keyA, durability, cancellationToken);
        KahunaKeyValue finalB = await client.GetKeyValue(keyB, durability, cancellationToken);
        KahunaKeyValue finalSummary = await client.GetKeyValue(summaryKey, durability, cancellationToken);
        KahunaKeyValue finalInflight = await client.GetKeyValue(keyInflight, durability, cancellationToken);

        Assert.True(finalA.Success);
        Assert.True(finalB.Success);
        Assert.Equal("old-b", finalB.ValueAsString());
        Assert.False(finalInflight.Success);

        if (locking == KeyValueTransactionLocking.Optimistic)
        {
            Assert.True(updaterCommittedBeforeBucketCommit);
            Assert.True(updater.Committed);
            Assert.False(bucketCommit.Committed);
            Assert.Equal("new-a", finalA.ValueAsString());
            Assert.False(finalSummary.Success);
        }
        else
        {
            Assert.False(updaterCommittedBeforeBucketCommit && updater.Committed);

            if (bucketCommit.Committed)
            {
                Assert.True(finalSummary.Success);
                Assert.Equal("2", finalSummary.ValueAsString());
            }

            if (updater.Committed)
                Assert.Equal("new-a", finalA.ValueAsString());
            else
                Assert.Equal("old-a", finalA.ValueAsString());
        }

        Assert.False(bucketCommit.Committed && updaterCommittedBeforeBucketCommit && updater.Committed);

        await inflightSession.Rollback(cancellationToken);
    }

    /// <summary>
    /// Verifies that a pessimistic GetByBucket acquires a prefix lock that blocks
    /// phantom inserts (new keys under the same prefix) until the transaction commits.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestPrefixLockBlocksPhantomInsert(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/phantom";
        string keyA = $"{prefix}/a";
        string keyB = $"{prefix}/b";
        string phantomKey = $"{prefix}/c";

        Assert.True((await client.SetKeyValue(keyA, "1", 10000, durability: durability, cancellationToken: cancellationToken)).Success);
        Assert.True((await client.SetKeyValue(keyB, "2", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession bucketSession = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 },
            cancellationToken
        );

        // Acquiring the prefix lock via GetByBucket must happen before the concurrent write attempt.
        List<KahunaKeyValue> bucketItems = await bucketSession.GetByBucket(prefix, durability, cancellationToken);
        Assert.Equal(2, bucketItems.Count);

        // Concurrent tx tries to insert a brand-new key under the locked prefix.
        Task<SimpleCommitAttempt> phantomWriteTask = ExecutePhantomInsertAttempt(
            client, phantomKey, "phantom", durability, cancellationToken
        );

        // Give the concurrent tx time to attempt and fail (or complete).
        bool phantomCommittedWhileLocked = await TryAwaitSuccess(
            phantomWriteTask, TimeSpan.FromMilliseconds(500), cancellationToken
        );

        CommitAttempt bucketCommit = await CommitTransactionAttempt(bucketSession, cancellationToken);
        SimpleCommitAttempt phantomWrite = await phantomWriteTask;

        Assert.True(bucketCommit.Committed);
        // The phantom must not have committed while the prefix lock was held.
        Assert.False(phantomCommittedWhileLocked);
        // The phantom write should have been rejected (not silently succeeded with no write).
        Assert.False(phantomWrite.Committed);

        // After the prefix lock is released, a fresh unconditional write must succeed.
        KahunaKeyValue afterRelease = await client.SetKeyValue(
            phantomKey, "phantom-after", 10000, durability: durability, cancellationToken: cancellationToken
        );
        Assert.True(afterRelease.Success);
    }

    /// <summary>
    /// Verifies that calling GetByBucket twice with the same prefix inside a single pessimistic
    /// transaction is idempotent — the prefix lock is not acquired twice and the transaction
    /// commits cleanly.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestPrefixLockIdempotentForSamePrefix(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/idempotent";
        string keyA = $"{prefix}/a";

        Assert.True((await client.SetKeyValue(keyA, "hello", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 },
            cancellationToken
        );

        List<KahunaKeyValue> firstScan = await session.GetByBucket(prefix, durability, cancellationToken);
        List<KahunaKeyValue> secondScan = await session.GetByBucket(prefix, durability, cancellationToken);

        Assert.Single(firstScan);
        Assert.Single(secondScan);
        Assert.Equal(firstScan[0].Key, secondScan[0].Key);

        CommitAttempt commit = await CommitTransactionAttempt(session, cancellationToken);
        Assert.True(commit.Committed);
    }

    /// <summary>
    /// Verifies that rolling back a pessimistic transaction releases the prefix lock so that
    /// subsequent transactions can write freely under the same prefix.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestPrefixLockReleasedAfterRollback(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/rollback";
        string keyA = $"{prefix}/a";
        string keyB = $"{prefix}/b";

        Assert.True((await client.SetKeyValue(keyA, "original", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 },
            cancellationToken
        );

        List<KahunaKeyValue> bucketItems = await session.GetByBucket(prefix, durability, cancellationToken);
        Assert.Single(bucketItems);

        bool rolled = await session.Rollback(cancellationToken);
        Assert.True(rolled);

        // After rollback the prefix lock must be released so writes under the prefix succeed.
        KahunaKeyValue newEntry = await client.SetKeyValue(
            keyB, "new-entry", 10000, durability: durability, cancellationToken: cancellationToken
        );
        Assert.True(newEntry.Success);

        // The original key must be unchanged (rollback did not write anything).
        KahunaKeyValue finalA = await client.GetKeyValue(keyA, durability, cancellationToken);
        Assert.True(finalA.Success);
        Assert.Equal("original", finalA.ValueAsString());
    }

    private static async Task<SimpleCommitAttempt> ExecutePhantomInsertAttempt(
        KahunaClient client,
        string key,
        string value,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await using KahunaTransactionSession session = await client.StartTransactionSession(
                new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 },
                cancellationToken
            );

            KahunaKeyValue setResult = await session.SetKeyValue(
                key, value, 10000, durability: durability, cancellationToken: cancellationToken
            );
            Assert.True(setResult.Success);

            bool committed = await session.Commit(cancellationToken);
            return new(committed, committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is
            KeyValueResponseType.Aborted or
            KeyValueResponseType.MustRetry or
            KeyValueResponseType.AlreadyLocked)
        {
            return new(false, exception.KeyValueErrorCode);
        }
    }

    private static async Task<TransactionAttempt> ExecuteLostUpdateAttempt(
        KahunaClient client,
        string key,
        KeyValueTransactionLocking locking,
        KeyValueDurability durability,
        TxSchedule schedule,
        CancellationToken cancellationToken
    )
    {
        long? readValue = null;

        try
        {
            await using KahunaTransactionSession session = await client.StartTransactionSession(
                new()
                {
                    Locking = locking,
                    Timeout = 5000
                },
                cancellationToken
            );

            KahunaKeyValue currentValue = await session.GetKeyValue(key, durability, cancellationToken);

            Assert.True(currentValue.Success);

            readValue = currentValue.ValueAsLong();

            schedule.MarkReadComplete();
            await schedule.ReleaseWrites(cancellationToken);

            KahunaKeyValue setResult = await session.SetKeyValue(
                key,
                (readValue.Value + 1).ToString(),
                10000,
                durability: durability,
                cancellationToken: cancellationToken
            );

            Assert.True(setResult.Success);

            bool committed = await session.Commit(cancellationToken);

            return new(readValue, Committed: committed, ErrorCode: committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(readValue, Committed: false, ErrorCode: exception.KeyValueErrorCode);
        }
    }

    private static async Task<StaleReadAttempt> ExecuteStaleReadAttempt(
        KahunaClient client,
        string keyA,
        string keyB,
        KeyValueTransactionLocking locking,
        KeyValueDurability durability,
        StaleReadSchedule schedule,
        CancellationToken cancellationToken
    )
    {
        long? readValue = null;

        try
        {
            await using KahunaTransactionSession session = await client.StartTransactionSession(
                new()
                {
                    Locking = locking,
                    Timeout = 5000
                },
                cancellationToken
            );

            KahunaKeyValue currentA = await session.GetKeyValue(keyA, durability, cancellationToken);
            Assert.True(currentA.Success);

            readValue = currentA.ValueAsLong();

            schedule.MarkReadComplete();
            await schedule.WaitForWriteRelease(cancellationToken);

            KahunaKeyValue setResult = await session.SetKeyValue(
                keyB,
                (readValue.Value + 1).ToString(),
                10000,
                durability: durability,
                cancellationToken: cancellationToken
            );

            Assert.True(setResult.Success);

            bool committed = await session.Commit(cancellationToken);

            return new(readValue.Value, committed, committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(readValue ?? -1, false, exception.KeyValueErrorCode);
        }
    }

    private static async Task<WriteSkewAttempt> ExecuteInteractiveWriteSkewAttempt(
        KahunaClient client,
        string aliceKey,
        string bobKey,
        string doctorToDisableKey,
        KeyValueTransactionLocking locking,
        KeyValueDurability durability,
        TxSchedule schedule,
        CancellationToken cancellationToken
    )
    {
        bool aliceWasTrue = false;
        bool bobWasTrue = false;
        bool attemptedWrite = false;

        try
        {
            await using KahunaTransactionSession session = await client.StartTransactionSession(
                new()
                {
                    Locking = locking,
                    Timeout = 5000
                },
                cancellationToken
            );

            KahunaKeyValue alice = await session.GetKeyValue(aliceKey, durability, cancellationToken);
            KahunaKeyValue bob = await session.GetKeyValue(bobKey, durability, cancellationToken);

            Assert.True(alice.Success);
            Assert.True(bob.Success);

            aliceWasTrue = alice.ValueAsBool();
            bobWasTrue = bob.ValueAsBool();

            schedule.MarkReadComplete();
            await schedule.ReleaseWrites(cancellationToken);

            if (aliceWasTrue && bobWasTrue)
            {
                attemptedWrite = true;

                KahunaKeyValue setResult = await session.SetKeyValue(
                    doctorToDisableKey,
                    "false",
                    10000,
                    durability: durability,
                    cancellationToken: cancellationToken
                );

                Assert.True(setResult.Success);
            }

            bool committed = await session.Commit(cancellationToken);

            return new(aliceWasTrue, bobWasTrue, attemptedWrite, committed, committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(aliceWasTrue, bobWasTrue, attemptedWrite, false, exception.KeyValueErrorCode);
        }
    }

    private static async Task<ScriptAttempt> ExecuteWriteSkewScriptAttempt(
        KahunaClient client,
        string script,
        string prefix,
        string doctorKey,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await client.ExecuteKeyValueTransactionScript(
                script,
                null,
                [
                    new() { Key = "@prefix", Value = prefix },
                    new() { Key = "@doctor", Value = doctorKey }
                ],
                cancellationToken
            );

            return new(true, null);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(false, exception.KeyValueErrorCode);
        }
    }

    private static async Task<SimpleCommitAttempt> ExecuteValueUpdateAttempt(
        KahunaClient client,
        string key,
        string value,
        KeyValueTransactionLocking locking,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await using KahunaTransactionSession session = await client.StartTransactionSession(
                new()
                {
                    Locking = locking,
                    Timeout = 5000
                },
                cancellationToken
            );

            KahunaKeyValue setResult = await session.SetKeyValue(
                key,
                value,
                10000,
                durability: durability,
                cancellationToken: cancellationToken
            );
            Assert.True(setResult.Success);

            bool committed = await session.Commit(cancellationToken);
            return new(committed, committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(false, exception.KeyValueErrorCode);
        }
    }

    private static async Task<CommitAttempt> CommitTransactionAttempt(
        KahunaTransactionSession session,
        CancellationToken cancellationToken
    )
    {
        try
        {
            bool committed = await session.Commit(cancellationToken);
            return new(committed, committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(false, exception.KeyValueErrorCode);
        }
    }

    private static string GetWriteSkewBucketScript(KeyValueTransactionLocking locking, KeyValueDurability durability)
    {
        string lockingValue = locking == KeyValueTransactionLocking.Optimistic ? "optimistic" : "pessimistic";
        string getCommand = durability == KeyValueDurability.Persistent ? "GET BY BUCKET" : "EGET BY BUCKET";
        string setCommand = durability == KeyValueDurability.Persistent ? "SET" : "ESET";

        return $$"""
        BEGIN (locking="{{lockingValue}}")
         LET oncall = {{getCommand}} @prefix
         SLEEP 200
         IF count(oncall) = 2 THEN
          IF to_bool(oncall[0]) THEN
           IF to_bool(oncall[1]) THEN
            {{setCommand}} @doctor false EX 10000
           END
          END
         END
         COMMIT
        END
        """;
    }

    private static async Task SeedDoctors(
        KahunaClient client,
        string aliceKey,
        string bobKey,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        KahunaKeyValue alice = await client.SetKeyValue(
            aliceKey,
            "true",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        KahunaKeyValue bob = await client.SetKeyValue(
            bobKey,
            "true",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );

        Assert.True(alice.Success);
        Assert.True(bob.Success);
    }

    private static async Task SeedBucketVisibilityState(
        KahunaClient client,
        string keyA,
        string keyB,
        string keyDeleted,
        string keyExpired,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        Assert.True((await client.SetKeyValue(
            keyA,
            "old-a",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        )).Success);
        Assert.True((await client.SetKeyValue(
            keyB,
            "old-b",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        )).Success);

        KahunaKeyValue deleted = await client.SetKeyValue(
            keyDeleted,
            "to-delete",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(deleted.Success);
        Assert.True((await client.DeleteKeyValue(keyDeleted, durability, cancellationToken)).Success);

        KahunaKeyValue expired = await client.SetKeyValue(
            keyExpired,
            "to-expire",
            150,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(expired.Success);

        await Task.Delay(300, cancellationToken);
    }

    private static async Task<SimpleCommitAttempt> ExecuteFreshWriterAttempt(
        KahunaClient client,
        string keyA,
        KeyValueTransactionLocking locking,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await using KahunaTransactionSession session = await client.StartTransactionSession(
                new()
                {
                    Locking = locking,
                    Timeout = 5000
                },
                cancellationToken
            );

            KahunaKeyValue setResult = await session.SetKeyValue(
                keyA,
                "20",
                10000,
                durability: durability,
                cancellationToken: cancellationToken
            );

            Assert.True(setResult.Success);

            bool committed = await session.Commit(cancellationToken);

            return new(committed, committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(false, exception.KeyValueErrorCode);
        }
    }

    private static async Task<bool> TryAwaitSuccess(Task<SimpleCommitAttempt> task, TimeSpan timeout, CancellationToken cancellationToken)
    {
        try
        {
            SimpleCommitAttempt result = await task.WaitAsync(timeout, cancellationToken);
            return result.Committed;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    private static async Task AssertClusterAvailable(KahunaClient client, CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(2));

        try
        {
            await client.GetKeyValue($"tx-anomaly/probe/{Guid.NewGuid():N}", cancellationToken: timeout.Token);
        }
        catch (Exception exception)
        {
            throw new XunitException(
                "Client anomaly tests require the Docker Kahuna cluster on https://localhost:8082, https://localhost:8084, and https://localhost:8086. " +
                $"Cluster availability probe failed: {exception.GetType().Name}: {exception.Message}"
            );
        }
    }

    private static KahunaClient GetClientByType(KahunaClientType clientType)
    {
        return clientType switch
        {
            KahunaClientType.SingleEndpoint => new(Url, null, GetCommunication()),
            KahunaClientType.PoolOfEndpoints => new(Urls, null, GetCommunication()),
            _ => throw new ArgumentOutOfRangeException(nameof(clientType), clientType, null)
        };
    }

    private static IKahunaCommunication GetCommunication()
    {
        return new GrpcCommunication(null, null);
    }

    private sealed class TxSchedule
    {
        private readonly TaskCompletionSource bothReads = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource writesReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int readsCompleted;

        public void MarkReadComplete()
        {
            if (Interlocked.Increment(ref readsCompleted) == 2)
                bothReads.TrySetResult();
        }

        public Task WaitForBothReads(CancellationToken cancellationToken)
        {
            return bothReads.Task.WaitAsync(cancellationToken);
        }

        public async Task<bool> TryWaitForBothReads(TimeSpan timeout, CancellationToken cancellationToken)
        {
            try
            {
                await bothReads.Task.WaitAsync(timeout, cancellationToken);
                return true;
            }
            catch (TimeoutException)
            {
                return false;
            }
        }

        public Task ReleaseWrites(CancellationToken cancellationToken)
        {
            return writesReleased.Task.WaitAsync(cancellationToken);
        }

        public void AllowWrites()
        {
            writesReleased.TrySetResult();
        }
    }

    private sealed class StaleReadSchedule
    {
        private readonly TaskCompletionSource readCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource writeReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void MarkReadComplete()
        {
            readCompleted.TrySetResult();
        }

        public Task WaitForRead(CancellationToken cancellationToken)
        {
            return readCompleted.Task.WaitAsync(cancellationToken);
        }

        public Task WaitForWriteRelease(CancellationToken cancellationToken)
        {
            return writeReleased.Task.WaitAsync(cancellationToken);
        }

        public void AllowWrite()
        {
            writeReleased.TrySetResult();
        }
    }

    private sealed record TransactionAttempt(long? ReadValue, bool Committed, KeyValueResponseType? ErrorCode);

    private sealed record StaleReadAttempt(long ReadValue, bool Committed, KeyValueResponseType? ErrorCode);

    private sealed record SimpleCommitAttempt(bool Committed, KeyValueResponseType? ErrorCode);

    private sealed record WriteSkewAttempt(
        bool AliceWasTrue,
        bool BobWasTrue,
        bool AttemptedWrite,
        bool Committed,
        KeyValueResponseType? ErrorCode
    );

    private sealed record ScriptAttempt(bool Committed, KeyValueResponseType? ErrorCode);

    private sealed record CommitAttempt(bool Committed, KeyValueResponseType? ErrorCode);
}
