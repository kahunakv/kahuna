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

        KahunaKeyValue finalValue = await client.GetKeyValue(key, durability, cancellationToken: cancellationToken);

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

        KahunaKeyValue finalA = await client.GetKeyValue(keyA, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalB = await client.GetKeyValue(keyB, durability, cancellationToken: cancellationToken);

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

        KahunaKeyValue finalAlice = await client.GetKeyValue(aliceKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalBob = await client.GetKeyValue(bobKey, durability, cancellationToken: cancellationToken);

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

        KahunaKeyValue finalAlice = await client.GetKeyValue(aliceKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalBob = await client.GetKeyValue(bobKey, durability, cancellationToken: cancellationToken);

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

        List<KahunaKeyValue> bucketItems = await bucketSession.GetByBucket(prefix, durability, cancellationToken: cancellationToken);
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

        KahunaKeyValue finalA = await client.GetKeyValue(keyA, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalB = await client.GetKeyValue(keyB, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalSummary = await client.GetKeyValue(summaryKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalInflight = await client.GetKeyValue(keyInflight, durability, cancellationToken: cancellationToken);

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

    [Theory, CombinatorialData]
    public async Task TestPhantomInsertUnderBucketRead(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/users/active";
        string existingKey = $"{prefix}/1";
        string concurrentInsertKey = $"{prefix}/2";
        string staleInsertKey = $"{prefix}/3";

        KahunaKeyValue initial = await client.SetKeyValue(
            existingKey,
            "existing",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(initial.Success);

        await using KahunaTransactionSession staleSession = await client.StartTransactionSession(
            new()
            {
                Locking = locking,
                Timeout = 5000
            },
            cancellationToken
        );

        List<KahunaKeyValue> visibleUsers = await staleSession.GetByBucket(prefix, durability, cancellationToken: cancellationToken);
        Assert.Single(visibleUsers);
        Assert.Equal(existingKey, visibleUsers[0].Key);

        Task<SimpleCommitAttempt> concurrentInsertTask = ExecuteInsertAttempt(
            client,
            concurrentInsertKey,
            "concurrent",
            locking,
            durability,
            cancellationToken
        );

        bool concurrentCommittedBeforeStaleCommit = await TryAwaitSuccess(
            concurrentInsertTask,
            TimeSpan.FromMilliseconds(250),
            cancellationToken
        );

        KahunaKeyValue staleInsert = await staleSession.SetKeyValue(
            staleInsertKey,
            "stale",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(staleInsert.Success);

        CommitAttempt staleCommit = await CommitTransactionAttempt(staleSession, cancellationToken);
        SimpleCommitAttempt concurrentInsert = await concurrentInsertTask;

        List<KahunaKeyValue> finalUsers = await client.GetByBucket(prefix, durability, cancellationToken: cancellationToken);
        string[] finalKeys = finalUsers
            .Select(item => item.Key)
            .OrderBy(key => key, StringComparer.Ordinal)
            .ToArray();

        if (locking == KeyValueTransactionLocking.Pessimistic)
        {
            Assert.False(concurrentCommittedBeforeStaleCommit && staleCommit.Committed);
            Assert.True(finalUsers.Count <= 2);
            Assert.DoesNotContain(finalKeys, key => key == concurrentInsertKey && finalKeys.Contains(staleInsertKey));
        }
        else
        {
            // Optimistic bucket reads currently validate returned keys, not unseen future inserts.
            // If predicate dependencies are later tracked, this branch can be tightened.
            if (staleCommit.Committed && concurrentInsert.Committed)
                Assert.Equal([existingKey, concurrentInsertKey, staleInsertKey], finalKeys);
            else
                Assert.True(finalUsers.Count <= 2);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestNonRepeatableReadInsideOneTransaction(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}";
        string accountKey = $"{prefix}/account";
        string observedKey = $"{prefix}/observed";

        KahunaKeyValue seeded = await client.SetKeyValue(
            accountKey,
            "100",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(seeded.Success);

        NonRepeatableReadSchedule schedule = new();

        Task<NonRepeatableReadAttempt> readerTask = ExecuteNonRepeatableReadAttempt(
            client,
            accountKey,
            observedKey,
            locking,
            durability,
            schedule,
            cancellationToken
        );

        await schedule.WaitForFirstRead(cancellationToken);

        Task<SimpleCommitAttempt> writerTask = ExecuteValueUpdateAttempt(
            client,
            accountKey,
            "200",
            locking,
            durability,
            cancellationToken
        );

        bool writerCommittedBeforeSecondRead = await TryAwaitSuccess(
            writerTask,
            TimeSpan.FromMilliseconds(250),
            cancellationToken
        );

        schedule.AllowSecondRead();

        NonRepeatableReadAttempt reader = await readerTask;
        SimpleCommitAttempt writer = await writerTask;

        KahunaKeyValue finalAccount = await client.GetKeyValue(accountKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalObserved = await client.GetKeyValue(observedKey, durability, cancellationToken: cancellationToken);

        Assert.True(finalAccount.Success);

        if (locking == KeyValueTransactionLocking.Pessimistic)
        {
            Assert.False(writerCommittedBeforeSecondRead);
            Assert.True(reader.Committed);
            Assert.Equal(100, reader.FirstRead);
            Assert.Equal(100, reader.SecondRead);
            Assert.True(finalObserved.Success);
            Assert.Equal("100:100", finalObserved.ValueAsString());
        }
        else
        {
            Assert.True(writerCommittedBeforeSecondRead);
            Assert.True(writer.Committed);
            Assert.False(reader.Committed);
            Assert.Equal(100, reader.FirstRead);
            Assert.Equal("200", finalAccount.ValueAsString());
            Assert.False(finalObserved.Success);
        }

        Assert.False(reader.Committed && reader.FirstRead != reader.SecondRead);
    }

    [Theory, CombinatorialData]
    public async Task TestDirtyReadAndUncommittedWriteVisibility(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/dirty";
        string keyX = $"{prefix}/x";
        string keyY = $"{prefix}/y";

        Assert.True((await client.SetKeyValue(
            keyX,
            "committed",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        )).Success);
        Assert.True((await client.SetKeyValue(
            keyY,
            "stable",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        )).Success);

        await using KahunaTransactionSession writerSession = await client.StartTransactionSession(
            new() { Locking = locking, Timeout = 5000 },
            cancellationToken
        );

        KahunaKeyValue uncommittedWrite = await writerSession.SetKeyValue(
            keyX,
            "uncommitted",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(uncommittedWrite.Success);

        KahunaKeyValue outsideRead = await client.GetKeyValue(keyX, durability, cancellationToken: cancellationToken);
        Assert.True(outsideRead.Success);

        await using KahunaTransactionSession readerSession = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic, Timeout = 5000 },
            cancellationToken
        );
        KahunaKeyValue transactionalRead = await readerSession.GetKeyValue(keyX, durability, cancellationToken: cancellationToken);
        Assert.True(transactionalRead.Success);

        KahunaKeyValueTransactionResult scriptRead = await client.ExecuteKeyValueTransactionScript(
            GetSingleReadScript(durability, keyX),
            cancellationToken: cancellationToken
        );
        Assert.Equal(KeyValueResponseType.Get, scriptRead.Type);

        List<KahunaKeyValue> bucketItems = await client.GetByBucket(prefix, durability, cancellationToken: cancellationToken);
        List<KahunaKeyValue> prefixItems = await client.ScanAllByPrefix(prefix, durability, cancellationToken: cancellationToken);

        KahunaKeyValue bucketX = Assert.Single(bucketItems, item => item.Key == keyX);
        KahunaKeyValue prefixX = Assert.Single(prefixItems, item => item.Key == keyX);

        Assert.Equal("committed", outsideRead.ValueAsString());
        Assert.Equal("committed", transactionalRead.ValueAsString());
        Assert.Equal("committed", scriptRead.FirstValueAsString);
        Assert.Equal("committed", bucketX.ValueAsString());
        Assert.Equal("committed", prefixX.ValueAsString());

        bool rolledBack = await writerSession.Rollback(cancellationToken);
        Assert.True(rolledBack);

        KahunaKeyValue finalX = await client.GetKeyValue(keyX, durability, cancellationToken: cancellationToken);
        Assert.True(finalX.Success);
        Assert.Equal("committed", finalX.ValueAsString());
    }

    [Theory, CombinatorialData]
    public async Task TestLockReleaseAfterAbortRollbackTimeoutAndFailedPrepare(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability,
        [CombinatorialValues(false, true)] bool asyncRelease,
        [CombinatorialValues(
            LockReleaseScenario.ExplicitRollback,
            LockReleaseScenario.ScriptError,
            LockReleaseScenario.Timeout,
            LockReleaseScenario.FailedPrepareConflict
        )] LockReleaseScenario scenario
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/release";
        string lockedKey = $"{prefix}/locked-key";

        if (scenario == LockReleaseScenario.FailedPrepareConflict)
        {
            KahunaKeyValue seeded = await client.SetKeyValue(
                lockedKey,
                "0",
                10000,
                durability: durability,
                cancellationToken: cancellationToken
            );
            Assert.True(seeded.Success);
        }

        switch (scenario)
        {
            case LockReleaseScenario.ExplicitRollback:
                await ExecuteExplicitRollbackScenario(client, lockedKey, durability, asyncRelease, cancellationToken);
                break;
            case LockReleaseScenario.ScriptError:
                await ExecuteScriptErrorScenario(client, lockedKey, durability, asyncRelease, cancellationToken);
                break;
            case LockReleaseScenario.Timeout:
                await ExecuteTimeoutReleaseScenario(client, lockedKey, durability, asyncRelease, cancellationToken);
                return;
            case LockReleaseScenario.FailedPrepareConflict:
                await ExecuteFailedPrepareConflictScenario(client, lockedKey, durability, asyncRelease, cancellationToken);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null);
        }

        await AssertSubsequentPessimisticWriteSucceeds(client, lockedKey, durability, asyncRelease, cancellationToken);
    }

    [Theory, CombinatorialData]
    public async Task TestMultiKeyAtomicityUnderConflict(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/atomic";
        string keyA = $"{prefix}/a";
        string keyB = $"{prefix}/b";
        string keyC = $"{prefix}/c";

        Assert.True((await client.SetKeyValue(keyA, "0", 10000, durability: durability, cancellationToken: cancellationToken)).Success);
        Assert.True((await client.SetKeyValue(keyB, "0", 10000, durability: durability, cancellationToken: cancellationToken)).Success);
        Assert.True((await client.SetKeyValue(keyC, "0", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession tx1 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic, Timeout = 5000 },
            cancellationToken
        );

        KahunaKeyValue initialA = await tx1.GetKeyValue(keyA, durability, cancellationToken: cancellationToken);
        KahunaKeyValue initialB = await tx1.GetKeyValue(keyB, durability, cancellationToken: cancellationToken);
        KahunaKeyValue initialC = await tx1.GetKeyValue(keyC, durability, cancellationToken: cancellationToken);

        Assert.True(initialA.Success);
        Assert.True(initialB.Success);
        Assert.True(initialC.Success);
        Assert.Equal(0, initialA.ValueAsLong());
        Assert.Equal(0, initialB.ValueAsLong());
        Assert.Equal(0, initialC.ValueAsLong());

        SimpleCommitAttempt conflictWriter = await ExecuteValueUpdateAttempt(
            client,
            keyB,
            "2",
            KeyValueTransactionLocking.Pessimistic,
            durability,
            cancellationToken
        );
        Assert.True(conflictWriter.Committed);

        CommitAttempt tx1Commit;

        try
        {
            Assert.True((await tx1.SetKeyValue(keyA, "1", 10000, durability: durability, cancellationToken: cancellationToken)).Success);
            Assert.True((await tx1.SetKeyValue(keyB, "1", 10000, durability: durability, cancellationToken: cancellationToken)).Success);
            Assert.True((await tx1.SetKeyValue(keyC, "1", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

            tx1Commit = await CommitTransactionAttempt(tx1, cancellationToken);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            tx1Commit = new(false, exception.KeyValueErrorCode);
        }

        Assert.False(tx1Commit.Committed);
        Assert.True(tx1Commit.ErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry);

        KahunaKeyValue finalA = await client.GetKeyValue(keyA, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalB = await client.GetKeyValue(keyB, durability, cancellationToken: cancellationToken);
        KahunaKeyValue finalC = await client.GetKeyValue(keyC, durability, cancellationToken: cancellationToken);

        Assert.True(finalA.Success);
        Assert.True(finalB.Success);
        Assert.True(finalC.Success);

        Assert.Equal("0", finalA.ValueAsString());
        Assert.Equal("2", finalB.ValueAsString());
        Assert.Equal("0", finalC.ValueAsString());
    }

    [Theory, CombinatorialData]
    public async Task TestReadYourOwnWritesWithinTransaction(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)] KeyValueTransactionLocking locking,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/ryow";
        string updatedKey = $"{prefix}/updated";
        string insertedKey = $"{prefix}/inserted";
        string deletedKey = $"{prefix}/deleted";

        Assert.True((await client.SetKeyValue(updatedKey, "before", 10000, durability: durability, cancellationToken: cancellationToken)).Success);
        Assert.True((await client.SetKeyValue(deletedKey, "to-delete", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = locking, Timeout = 5000 },
            cancellationToken
        );

        KahunaKeyValue updatedWrite = await session.SetKeyValue(
            updatedKey,
            "after",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        KahunaKeyValue insertedWrite = await session.SetKeyValue(
            insertedKey,
            "created",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        KahunaKeyValue deletedWrite = await session.DeleteKeyValue(
            deletedKey,
            durability,
            cancellationToken
        );

        Assert.True(updatedWrite.Success);
        Assert.True(insertedWrite.Success);
        Assert.True(deletedWrite.Success);

        KahunaKeyValue updatedInTx = await session.GetKeyValue(updatedKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue insertedInTx = await session.GetKeyValue(insertedKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue deletedInTx = await session.GetKeyValue(deletedKey, durability, cancellationToken: cancellationToken);

        Assert.True(updatedInTx.Success);
        Assert.True(insertedInTx.Success);
        Assert.False(deletedInTx.Success);
        Assert.Equal("after", updatedInTx.ValueAsString());
        Assert.Equal("created", insertedInTx.ValueAsString());

        KeyValueGetByRangePageResult rangeInTx = await session.GetByRange(
            prefix,
            null,
            true,
            null,
            true,
            durability: durability,
            cancellationToken: cancellationToken
        );

        KahunaKeyValueTransactionResult scriptRead = await client.ExecuteKeyValueTransactionScript(
            GetSingleReadScript(durability, updatedKey),
            cancellationToken: cancellationToken
        );

        KahunaKeyValue updatedOutsideBeforeCommit = await client.GetKeyValue(updatedKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue insertedOutsideBeforeCommit = await client.GetKeyValue(insertedKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue deletedOutsideBeforeCommit = await client.GetKeyValue(deletedKey, durability, cancellationToken: cancellationToken);

        Assert.Equal(KeyValueResponseType.Get, scriptRead.Type);
        Assert.Equal("before", scriptRead.FirstValueAsString);

        Assert.True(updatedOutsideBeforeCommit.Success);
        Assert.False(insertedOutsideBeforeCommit.Success);
        Assert.True(deletedOutsideBeforeCommit.Success);
        Assert.Equal("before", updatedOutsideBeforeCommit.ValueAsString());
        Assert.Equal("to-delete", deletedOutsideBeforeCommit.ValueAsString());

        KeyValueGetByBucketItem updatedInRange = Assert.Single(rangeInTx.Items, item => item.Key == updatedKey);
        KeyValueGetByBucketItem insertedInRange = Assert.Single(rangeInTx.Items, item => item.Key == insertedKey);
        Assert.DoesNotContain(rangeInTx.Items, item => item.Key == deletedKey);
        Assert.Equal("after", updatedInRange.Value is not null ? System.Text.Encoding.UTF8.GetString(updatedInRange.Value) : null);
        Assert.Equal("created", insertedInRange.Value is not null ? System.Text.Encoding.UTF8.GetString(insertedInRange.Value) : null);

        CommitAttempt commit = await CommitTransactionAttempt(session, cancellationToken);
        Assert.True(commit.Committed);

        KahunaKeyValue updatedAfterCommit = await client.GetKeyValue(updatedKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue insertedAfterCommit = await client.GetKeyValue(insertedKey, durability, cancellationToken: cancellationToken);
        KahunaKeyValue deletedAfterCommit = await client.GetKeyValue(deletedKey, durability, cancellationToken: cancellationToken);

        Assert.True(updatedAfterCommit.Success);
        Assert.True(insertedAfterCommit.Success);
        Assert.False(deletedAfterCommit.Success);
        Assert.Equal("after", updatedAfterCommit.ValueAsString());
        Assert.Equal("created", insertedAfterCommit.ValueAsString());
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
        List<KahunaKeyValue> bucketItems = await bucketSession.GetByBucket(prefix, durability, cancellationToken: cancellationToken);
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

        List<KahunaKeyValue> firstScan = await session.GetByBucket(prefix, durability, cancellationToken: cancellationToken);
        List<KahunaKeyValue> secondScan = await session.GetByBucket(prefix, durability, cancellationToken: cancellationToken);

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

        List<KahunaKeyValue> bucketItems = await session.GetByBucket(prefix, durability, cancellationToken: cancellationToken);
        Assert.Single(bucketItems);

        bool rolled = await session.Rollback(cancellationToken);
        Assert.True(rolled);

        // After rollback the prefix lock must be released so writes under the prefix succeed.
        KahunaKeyValue newEntry = await client.SetKeyValue(
            keyB, "new-entry", 10000, durability: durability, cancellationToken: cancellationToken
        );
        Assert.True(newEntry.Success);

        // The original key must be unchanged (rollback did not write anything).
        KahunaKeyValue finalA = await client.GetKeyValue(keyA, durability, cancellationToken: cancellationToken);
        Assert.True(finalA.Success);
        Assert.Equal("original", finalA.ValueAsString());
    }

    /// <summary>
    /// Verifies that a pessimistic GetByRange acquires a range lock that blocks
    /// phantom inserts into the locked range until the transaction commits.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestRangeLockBlocksPhantomInsert(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/range-phantom";
        string keyA = $"{prefix}/a";
        string keyB = $"{prefix}/b";
        string phantomKey = $"{prefix}/c";

        Assert.True((await client.SetKeyValue(keyA, "1", 10000, durability: durability, cancellationToken: cancellationToken)).Success);
        Assert.True((await client.SetKeyValue(keyB, "2", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession rangeSession = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 },
            cancellationToken
        );

        // Acquire range lock via GetByRange — covers [prefix/a .. prefix/z]
        KeyValueGetByRangePageResult rangeResult = await rangeSession.GetByRange(
            prefix, $"{prefix}/a", true, $"{prefix}/z", true,
            durability: durability, cancellationToken: cancellationToken
        );
        Assert.Equal(2, rangeResult.Items.Count);

        // Concurrent tx tries to insert a brand-new key inside the locked range.
        Task<SimpleCommitAttempt> phantomWriteTask = ExecutePhantomInsertAttempt(
            client, phantomKey, "phantom", durability, cancellationToken
        );

        bool phantomCommittedWhileLocked = await TryAwaitSuccess(
            phantomWriteTask, TimeSpan.FromMilliseconds(500), cancellationToken
        );

        CommitAttempt rangeCommit = await CommitTransactionAttempt(rangeSession, cancellationToken);
        SimpleCommitAttempt phantomWrite = await phantomWriteTask;

        Assert.True(rangeCommit.Committed);
        Assert.False(phantomCommittedWhileLocked);
        Assert.False(phantomWrite.Committed);

        // After the range lock is released, a fresh write must succeed.
        KahunaKeyValue afterRelease = await client.SetKeyValue(
            phantomKey, "phantom-after", 10000, durability: durability, cancellationToken: cancellationToken
        );
        Assert.True(afterRelease.Success);
    }

    /// <summary>
    /// Verifies that calling GetByRange twice with the same range inside a single pessimistic
    /// transaction is idempotent — the range lock is not acquired twice and the transaction
    /// commits cleanly.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestRangeLockIdempotentForSameRange(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/range-idempotent";
        string keyA = $"{prefix}/a";

        Assert.True((await client.SetKeyValue(keyA, "hello", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 },
            cancellationToken
        );

        KeyValueGetByRangePageResult firstScan = await session.GetByRange(
            prefix, null, true, null, true, durability: durability, cancellationToken: cancellationToken
        );
        KeyValueGetByRangePageResult secondScan = await session.GetByRange(
            prefix, null, true, null, true, durability: durability, cancellationToken: cancellationToken
        );

        Assert.Single(firstScan.Items);
        Assert.Single(secondScan.Items);
        Assert.Equal(firstScan.Items[0].Key, secondScan.Items[0].Key);

        CommitAttempt commit = await CommitTransactionAttempt(session, cancellationToken);
        Assert.True(commit.Committed);
    }

    /// <summary>
    /// Verifies that rolling back a pessimistic transaction releases the range lock so that
    /// subsequent transactions can write freely inside the range.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestRangeLockReleasedAfterRollback(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/range-rollback";
        string keyA = $"{prefix}/a";
        string keyB = $"{prefix}/b";

        Assert.True((await client.SetKeyValue(keyA, "original", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 },
            cancellationToken
        );

        KeyValueGetByRangePageResult rangeItems = await session.GetByRange(
            prefix, null, true, null, true, durability: durability, cancellationToken: cancellationToken
        );
        Assert.Single(rangeItems.Items);

        bool rolled = await session.Rollback(cancellationToken);
        Assert.True(rolled);

        // After rollback the range lock must be released so writes inside the range succeed.
        KahunaKeyValue newEntry = await client.SetKeyValue(
            keyB, "new-entry", 10000, durability: durability, cancellationToken: cancellationToken
        );
        Assert.True(newEntry.Success);

        KahunaKeyValue finalA = await client.GetKeyValue(keyA, durability, cancellationToken: cancellationToken);
        Assert.True(finalA.Success);
        Assert.Equal("original", finalA.ValueAsString());
    }

    /// <summary>
    /// Verifies that a range lock with an explicit EndKey only blocks writes within
    /// the bounded range — a key outside the end boundary commits freely.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestRangeLockRespectsEndKey(
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        KahunaClient client = GetClientByType(clientType);

        await AssertClusterAvailable(client, cancellationToken);

        string prefix = $"tx-anomaly/{Guid.NewGuid():N}/range-endkey";
        string keyA = $"{prefix}/a";
        string keyInsideRange = $"{prefix}/b";    // inside [prefix/a .. prefix/c]
        string keyOutsideRange = $"{prefix}/z";   // outside [prefix/a .. prefix/c]

        Assert.True((await client.SetKeyValue(keyA, "1", 10000, durability: durability, cancellationToken: cancellationToken)).Success);

        await using KahunaTransactionSession rangeSession = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 },
            cancellationToken
        );

        // Lock only the range [prefix/a .. prefix/c].
        await rangeSession.GetByRange(
            prefix, $"{prefix}/a", true, $"{prefix}/c", true,
            durability: durability, cancellationToken: cancellationToken
        );

        // A key outside the range should commit immediately.
        Task<SimpleCommitAttempt> outsideTask = ExecutePhantomInsertAttempt(
            client, keyOutsideRange, "outside", durability, cancellationToken
        );
        SimpleCommitAttempt outsideCommit = await outsideTask.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.True(outsideCommit.Committed);

        // A key inside the range must be blocked.
        Task<SimpleCommitAttempt> insideTask = ExecutePhantomInsertAttempt(
            client, keyInsideRange, "inside", durability, cancellationToken
        );
        bool insideCommittedWhileLocked = await TryAwaitSuccess(
            insideTask, TimeSpan.FromMilliseconds(500), cancellationToken
        );
        Assert.False(insideCommittedWhileLocked);

        CommitAttempt rangeCommit = await CommitTransactionAttempt(rangeSession, cancellationToken);
        SimpleCommitAttempt insideWrite = await insideTask;

        Assert.True(rangeCommit.Committed);
        Assert.False(insideWrite.Committed);
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

    private static async Task<NonRepeatableReadAttempt> ExecuteNonRepeatableReadAttempt(
        KahunaClient client,
        string accountKey,
        string observedKey,
        KeyValueTransactionLocking locking,
        KeyValueDurability durability,
        NonRepeatableReadSchedule schedule,
        CancellationToken cancellationToken
    )
    {
        long? firstRead = null;
        long? secondRead = null;

        try
        {
            await using KahunaTransactionSession session = await client.StartTransactionSession(
                new() { Locking = locking, Timeout = 5000 },
                cancellationToken
            );

            KahunaKeyValue first = await session.GetKeyValue(accountKey, durability, cancellationToken: cancellationToken);
            Assert.True(first.Success);
            firstRead = first.ValueAsLong();

            schedule.MarkFirstReadComplete();
            await schedule.WaitForSecondReadRelease(cancellationToken);

            KahunaKeyValue second = await session.GetKeyValue(accountKey, durability, cancellationToken: cancellationToken);
            Assert.True(second.Success);
            secondRead = second.ValueAsLong();

            KahunaKeyValue observedWrite = await session.SetKeyValue(
                observedKey,
                $"{firstRead.Value}:{secondRead.Value}",
                10000,
                durability: durability,
                cancellationToken: cancellationToken
            );
            Assert.True(observedWrite.Success);

            bool committed = await session.Commit(cancellationToken);
            return new(firstRead.Value, secondRead.Value, committed, committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(firstRead ?? -1, secondRead ?? -1, false, exception.KeyValueErrorCode);
        }
    }

    private static async Task ExecuteExplicitRollbackScenario(
        KahunaClient client,
        string lockedKey,
        KeyValueDurability durability,
        bool asyncRelease,
        CancellationToken cancellationToken
    )
    {
        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new()
            {
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 5000,
                AsyncRelease = asyncRelease
            },
            cancellationToken
        );

        KahunaKeyValue setResult = await session.SetKeyValue(
            lockedKey,
            "temp",
            10000,
            durability: durability,
            cancellationToken: cancellationToken
        );
        Assert.True(setResult.Success);

        bool rolledBack = await session.Rollback(cancellationToken);
        Assert.True(rolledBack);
    }

    private static async Task ExecuteScriptErrorScenario(
        KahunaClient client,
        string lockedKey,
        KeyValueDurability durability,
        bool asyncRelease,
        CancellationToken cancellationToken
    )
    {
        string setCommand = durability == KeyValueDurability.Persistent ? "SET" : "ESET";
        string asyncReleaseValue = asyncRelease ? "true" : "false";

        string script = $$"""
        BEGIN (locking="pessimistic", asyncRelease="{{asyncReleaseValue}}")
         {{setCommand}} @key 'temp' EX 10000
         THROW 'boom'
        END
        """;

        try
        {
            await client.ExecuteKeyValueTransactionScript(
                script,
                parameters: [new() { Key = "@key", Value = lockedKey }],
                cancellationToken: cancellationToken
            );

            Assert.Fail("Expected the script error scenario to throw.");
        }
        catch (KahunaException exception)
        {
            Assert.Equal(KeyValueResponseType.Errored, exception.KeyValueErrorCode);
        }
    }

    private static async Task ExecuteTimeoutReleaseScenario(
        KahunaClient client,
        string lockedKey,
        KeyValueDurability durability,
        bool asyncRelease,
        CancellationToken cancellationToken
    )
    {
        KahunaTransactionSession session = await client.StartTransactionSession(
            new()
            {
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 100,
                AsyncRelease = asyncRelease
            },
            cancellationToken
        );

        try
        {
            KahunaKeyValue setResult = await session.SetKeyValue(
                lockedKey,
                "temp",
                10000,
                durability: durability,
                cancellationToken: cancellationToken
            );
            Assert.True(setResult.Success);

            await Task.Delay(300, cancellationToken);

            await AssertSubsequentPessimisticWriteSucceeds(client, lockedKey, durability, asyncRelease, cancellationToken);
        }
        finally
        {
            try
            {
                await session.Rollback(cancellationToken);
            }
            catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry or KeyValueResponseType.Errored)
            {
                // The transaction may already have been reaped by timeout.
            }

            await session.DisposeAsync();
        }
    }

    private static async Task ExecuteFailedPrepareConflictScenario(
        KahunaClient client,
        string lockedKey,
        KeyValueDurability durability,
        bool asyncRelease,
        CancellationToken cancellationToken
    )
    {
        TxSchedule schedule = new();

        Task<TransactionAttempt> firstAttempt = ExecuteOptimisticConflictAttempt(
            client,
            lockedKey,
            "1",
            durability,
            asyncRelease,
            schedule,
            cancellationToken
        );
        Task<TransactionAttempt> secondAttempt = ExecuteOptimisticConflictAttempt(
            client,
            lockedKey,
            "2",
            durability,
            asyncRelease,
            schedule,
            cancellationToken
        );

        await schedule.WaitForBothReads(cancellationToken);
        schedule.AllowWrites();

        TransactionAttempt[] attempts = await Task.WhenAll(firstAttempt, secondAttempt);

        Assert.Equal(1, attempts.Count(attempt => attempt.Committed));
        Assert.Equal(1, attempts.Count(attempt => attempt.ErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry));
    }

    private static async Task<TransactionAttempt> ExecuteOptimisticConflictAttempt(
        KahunaClient client,
        string key,
        string value,
        KeyValueDurability durability,
        bool asyncRelease,
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
                    Locking = KeyValueTransactionLocking.Optimistic,
                    Timeout = 5000,
                    AsyncRelease = asyncRelease
                },
                cancellationToken
            );

            KahunaKeyValue currentValue = await session.GetKeyValue(key, durability, cancellationToken: cancellationToken);
            Assert.True(currentValue.Success);
            readValue = currentValue.ValueAsLong();

            schedule.MarkReadComplete();
            await schedule.ReleaseWrites(cancellationToken);

            KahunaKeyValue setResult = await session.SetKeyValue(
                key,
                value,
                10000,
                durability: durability,
                cancellationToken: cancellationToken
            );
            Assert.True(setResult.Success);

            bool committed = await session.Commit(cancellationToken);
            return new(readValue, committed, committed ? null : KeyValueResponseType.Aborted);
        }
        catch (KahunaException exception) when (exception.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry)
        {
            return new(readValue, false, exception.KeyValueErrorCode);
        }
    }

    private static async Task AssertSubsequentPessimisticWriteSucceeds(
        KahunaClient client,
        string lockedKey,
        KeyValueDurability durability,
        bool asyncRelease,
        CancellationToken cancellationToken
    )
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(3));

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new()
            {
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 1000,
                AsyncRelease = asyncRelease
            },
            timeout.Token
        );

        KahunaKeyValue setResult = await session.SetKeyValue(
            lockedKey,
            "released",
            10000,
            durability: durability,
            cancellationToken: timeout.Token
        );
        Assert.True(setResult.Success);

        bool committed = await session.Commit(timeout.Token);
        Assert.True(committed);

        KahunaKeyValue finalValue = await client.GetKeyValue(lockedKey, durability, cancellationToken: timeout.Token);
        Assert.True(finalValue.Success);
        Assert.Equal("released", finalValue.ValueAsString());
    }

    private static string GetSingleReadScript(KeyValueDurability durability, string key)
    {
        return durability == KeyValueDurability.Persistent
            ? $"GET `{key}`"
            : $"EGET `{key}`";
    }

    private static async Task<SimpleCommitAttempt> ExecuteInsertAttempt(
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
                new() { Locking = locking, Timeout = 5000 },
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

            KahunaKeyValue currentValue = await session.GetKeyValue(key, durability, cancellationToken: cancellationToken);

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

            KahunaKeyValue currentA = await session.GetKeyValue(keyA, durability, cancellationToken: cancellationToken);
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

            KahunaKeyValue alice = await session.GetKeyValue(aliceKey, durability, cancellationToken: cancellationToken);
            KahunaKeyValue bob = await session.GetKeyValue(bobKey, durability, cancellationToken: cancellationToken);

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
        return new GrpcCommunication(new() { AllowInsecureCertificateValidation = true }, null);
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

    private sealed class NonRepeatableReadSchedule
    {
        private readonly TaskCompletionSource firstReadCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource secondReadReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void MarkFirstReadComplete()
        {
            firstReadCompleted.TrySetResult();
        }

        public Task WaitForFirstRead(CancellationToken cancellationToken)
        {
            return firstReadCompleted.Task.WaitAsync(cancellationToken);
        }

        public Task WaitForSecondReadRelease(CancellationToken cancellationToken)
        {
            return secondReadReleased.Task.WaitAsync(cancellationToken);
        }

        public void AllowSecondRead()
        {
            secondReadReleased.TrySetResult();
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

    private sealed record NonRepeatableReadAttempt(
        long FirstRead,
        long SecondRead,
        bool Committed,
        KeyValueResponseType? ErrorCode
    );

    public enum LockReleaseScenario
    {
        ExplicitRollback,
        ScriptError,
        Timeout,
        FailedPrepareConflict
    }
}
