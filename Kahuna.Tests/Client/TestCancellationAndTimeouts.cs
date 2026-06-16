using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
// GrpcBatcher is in Kahuna.Client.Communication (internal, visible via InternalsVisibleTo)

namespace Kahuna.Tests.Client;

[Collection("GrpcBatcherTests")]
public class TestCancellationAndTimeouts
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    private readonly ITestOutputHelper outputHelper;

    public TestCancellationAndTimeouts(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
    }

    [Theory, CombinatorialData]
    public async Task TestCancelSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Create a cancellation token source and cancel it immediately
        using CancellationTokenSource cts = new();
        cts.Cancel();
        
        // Attempt to set a key with a cancelled token
        await AssertCancelledAsync(async () =>
        {
            await client.SetKeyValue(
                keyName,
                "test-value",
                10000,
                cancellationToken: cts.Token
            );
        });
    }

    [Theory, CombinatorialData]
    public async Task TestCancelGetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Set a key first
        KahunaKeyValue setResult = await client.SetKeyValue(
            keyName,
            "test-value",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(setResult.Success);
        
        // Create a cancellation token source and cancel it immediately
        using CancellationTokenSource cts = new();
        cts.Cancel();
        
        // Attempt to get the key with a cancelled token
        await AssertCancelledAsync(async () =>
        {
            await client.GetKeyValue(
                keyName,
                cancellationToken: cts.Token
            );
        });
    }

    [Theory, CombinatorialData]
    public async Task TestCancelAcquireLock(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomKeyName();
        
        // Create a cancellation token source and cancel it immediately
        using CancellationTokenSource cts = new();
        cts.Cancel();
        
        async Task AcquireLockWithCancelledToken()
        {
            await client.GetOrCreateLock(
                lockName,
                expiryTime: 10000,
                cancellationToken: cts.Token
            );
        }

        // Attempt to acquire a lock with a cancelled token
        await AssertCancelledAsync(AcquireLockWithCancelledToken);
    }

    [Theory, CombinatorialData]
    public async Task TestCancelTransactionExecution(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        // Create a cancellation token source and cancel it immediately
        using CancellationTokenSource cts = new();
        cts.Cancel();
        
        // Attempt to execute a transaction script with a cancelled token
        await AssertCancelledAsync(async () =>
        {
            await client.ExecuteKeyValueTransactionScript(
                "SET `some-key` 'some-value'",
                cancellationToken: cts.Token
            );
        });
    }

    [Theory, CombinatorialData]
    public async Task TestLockAcquisitionWithTimeout(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomKeyName();
        
        // First acquire the lock
        KahunaLock firstLock = await client.GetOrCreateLock(
            lockName,
            expiryTime: 10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(firstLock.IsAcquired);
        
        // Try to acquire the same lock with a short wait time
        KahunaLock secondLock = await client.GetOrCreateLock(
            lockName,
            10000,
            waitTime: 100, // 100ms wait time
            retryTime: 50, // 50ms retry time
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        // The second lock acquisition should fail due to timeout
        Assert.False(secondLock.IsAcquired);
        
        // Clean up
        await firstLock.DisposeAsync();
    }

    [Theory, CombinatorialData]
    public async Task TestCancellationDuringLockWait(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomKeyName();
        
        // First acquire the lock
        KahunaLock firstLock = await client.GetOrCreateLock(
            lockName,
            expiryTime: 10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(firstLock.IsAcquired);
        
        // Create a cancellation token source that will be cancelled after a short delay
        using CancellationTokenSource cts = new();
        
        // Start a task that cancels the token after a short delay
        _ = Task.Run(async () =>
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
            cts.Cancel();
        }, TestContext.Current.CancellationToken);
        
        // Try to acquire the same lock with a long wait time, but it should be cancelled
        await AssertCancelledAsync(async () =>
        {
            await client.GetOrCreateLock(
                lockName,
                10000,
                waitTime: 10000, // 10s wait time
                retryTime: 100,  // 100ms retry time
                cancellationToken: cts.Token
            );
        });
        
        // Clean up
        await firstLock.DisposeAsync();
    }

    [Theory, CombinatorialData]
    public async Task TestRetryableTransactionWithCancellation(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        // Create a cancellation token source that will be cancelled after a short delay
        using CancellationTokenSource cts = new();
        
        // Start a task that cancels the token after a short delay
        _ = Task.Run(async () =>
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
            cts.Cancel();
        }, TestContext.Current.CancellationToken);
        
        // Try to execute a retryable transaction that should be cancelled
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await client.RetryableTransaction(
                new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Optimistic },
                async (session, token) =>
                {
                    // Simulate some work
                    await Task.Delay(1000, token);
                    await session.SetKeyValue(GetRandomKeyName(), "value", 10000, cancellationToken: token);
                },
                cts.Token
            );
        });
    }

    [Theory, CombinatorialData]
    public async Task TestTransactionSessionWithCancellation(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        // Start a transaction session
        KahunaTransactionSession session = await client.StartTransactionSession(
            new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Optimistic },
            TestContext.Current.CancellationToken
        );
        
        // Create a cancellation token source and cancel it immediately
        using CancellationTokenSource cts = new();
        cts.Cancel();
        
        // Attempt to set a key in the transaction with a cancelled token
        await AssertCancelledAsync(async () =>
        {
            await session.SetKeyValue(
                GetRandomKeyName(),
                "value",
                10000,
                cancellationToken: cts.Token
            );
        });
        
        // Clean up
        await session.Rollback(TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Verifies that cancelling a batched request before it is dispatched leaves requestRefs/requestStreamRefs empty.
    /// This is a unit-level test (no Docker) — it uses a bogus URL; the pre-cancelled token causes the
    /// IsCompleted early-bail in RunBatch so no connection is attempted.
    /// </summary>
    [Fact]
    public async Task TestCancelledToken_DoesNotLeakRequestRefs()
    {
        using CancellationTokenSource cts = new();
        cts.Cancel(); // pre-cancel

        GrpcBatcher batcher = new("https://localhost:1"); // unreachable — should not be contacted

        GrpcTrySetKeyValueRequest request = new()
        {
            Key = "test-key",
            Value = Google.Protobuf.ByteString.CopyFromUtf8("v"),
            ExpiresMs = 10000,
            Durability = GrpcKeyValueDurability.Persistent
        };

        int countBefore = GrpcBatcher.PendingRequestCount;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => batcher.Enqueue(request, cts.Token));

        // Give DeliverMessages a moment to run the IsCompleted early-bail.
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.Equal(countBefore, GrpcBatcher.PendingRequestCount);
    }

    private KahunaClient GetClientByType(KahunaCommunicationType communicationType, KahunaClientType clientType)
    {
        IKahunaCommunication communication = GetCommunicationByType(communicationType);
        
        return clientType switch
        {
            KahunaClientType.SingleEndpoint => new KahunaClient(url, communication: communication),
            KahunaClientType.PoolOfEndpoints => new KahunaClient(urls, communication: communication),
            _ => throw new ArgumentOutOfRangeException(nameof(clientType))
        };
    }

    private static IKahunaCommunication GetCommunicationByType(KahunaCommunicationType communicationType)
    {
        return communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(null, null),
            KahunaCommunicationType.Rest => new RestCommunication(null),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType))
        };
    }

    private static string GetRandomKeyName()
    {
        return $"test-key-{Guid.NewGuid():N}";
    }

    private static async Task AssertCancelledAsync(Func<Task> action)
    {
        Exception exception = await Assert.ThrowsAnyAsync<Exception>(action);
        
        Assert.True(
            exception is OperationCanceledException or KahunaException,
            $"Expected cancellation to surface as {nameof(OperationCanceledException)} or {nameof(KahunaException)}, got {exception.GetType().Name}"
        );
    }
} 
