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

    private sealed record TransactionAttempt(long? ReadValue, bool Committed, KeyValueResponseType? ErrorCode);
}
