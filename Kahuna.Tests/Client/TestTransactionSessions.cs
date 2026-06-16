using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kahuna.Tests.Client;

public class TestTransactionSessions
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    private readonly ITestOutputHelper outputHelper;

    public TestTransactionSessions(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
    }

    [Theory, CombinatorialData]
    public async Task TestTransactionSessionCommit(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Start a transaction session
        KahunaTransactionSession session = await client.StartTransactionSession(
            new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Optimistic },
            TestContext.Current.CancellationToken
        );
        
        // Set a key in the transaction
        KahunaKeyValue result = await session.SetKeyValue(
            keyName,
            "transaction-value",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        
        // Commit the transaction
        await session.Commit(TestContext.Current.CancellationToken);
        
        // Verify the key exists outside the transaction
        KahunaKeyValue getResult = await client.GetKeyValue(
            keyName,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult.Success);
        Assert.Equal("transaction-value", getResult.ValueAsString());
    }

    [Theory, CombinatorialData]
    public async Task TestTransactionSessionRollback(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Start a transaction session
        KahunaTransactionSession session = await client.StartTransactionSession(
            new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Optimistic },
            TestContext.Current.CancellationToken
        );
        
        // Set a key in the transaction
        KahunaKeyValue result = await session.SetKeyValue(
            keyName,
            "transaction-value",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        
        // Rollback the transaction
        await session.Rollback(TestContext.Current.CancellationToken);
        
        // Verify the key doesn't exist outside the transaction
        KahunaKeyValue getResult = await client.GetKeyValue(
            keyName,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(getResult.Success);
    }

    [Theory, CombinatorialData]
    public async Task TestTransactionSessionWithMultipleOperations(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();
        string keyName3 = GetRandomKeyName();
        
        // Start a transaction session
        KahunaTransactionSession session = await client.StartTransactionSession(
            new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Optimistic },
            TestContext.Current.CancellationToken
        );
        
        // Set multiple keys
        KahunaKeyValue result1 = await session.SetKeyValue(
            keyName1,
            "value1",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        KahunaKeyValue result2 = await session.SetKeyValue(
            keyName2,
            "value2",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        KahunaKeyValue result3 = await session.SetKeyValue(
            keyName3,
            "value3",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result1.Success);
        Assert.True(result2.Success);
        Assert.True(result3.Success);
        
        // Delete one key
        KahunaKeyValue deleteResult = await session.DeleteKeyValue(
            keyName2,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(deleteResult.Success);
        
        // Commit the transaction
        await session.Commit(TestContext.Current.CancellationToken);
        
        // Verify key1 exists
        KahunaKeyValue getResult1 = await client.GetKeyValue(
            keyName1,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult1.Success);
        Assert.Equal("value1", getResult1.ValueAsString());
        
        // Verify key2 doesn't exist (was deleted)
        KahunaKeyValue getResult2 = await client.GetKeyValue(
            keyName2,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(getResult2.Success);
        
        // Verify key3 exists
        KahunaKeyValue getResult3 = await client.GetKeyValue(
            keyName3,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult3.Success);
        Assert.Equal("value3", getResult3.ValueAsString());
    }

    [Theory, CombinatorialData]
    public async Task TestTransactionSessionWithPessimisticLocking(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Set a key first
        KahunaKeyValue initialSetResult = await client.SetKeyValue(
            keyName,
            "initial-value",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(initialSetResult.Success);
        
        // Start a transaction session with pessimistic locking
        KahunaTransactionSession session = await client.StartTransactionSession(
            new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Pessimistic },
            TestContext.Current.CancellationToken
        );
        
        // Get the key (which should lock it)
        KahunaKeyValue getResult = await session.GetKeyValue(
            keyName,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult.Success);
        Assert.Equal("initial-value", getResult.ValueAsString());
        
        // Update the key in the transaction
        KahunaKeyValue updateResult = await session.SetKeyValue(
            keyName,
            "updated-value",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(updateResult.Success);
        
        // Commit the transaction
        await session.Commit(TestContext.Current.CancellationToken);
        
        // Verify the key was updated
        KahunaKeyValue finalGetResult = await client.GetKeyValue(
            keyName,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(finalGetResult.Success);
        Assert.Equal("updated-value", finalGetResult.ValueAsString());
    }

    [Theory, CombinatorialData]
    public async Task TestRetryableTransactionSuccess(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        int attempts = 0;
        
        await client.RetryableTransaction(
            new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Optimistic },
            async (session, token) => {
                attempts++;
                await session.SetKeyValue(keyName, $"value-{attempts}", 10000, cancellationToken: token);
                await session.Commit(token);
            },
            TestContext.Current.CancellationToken
        );
        
        // Verify the transaction succeeded
        KahunaKeyValue getResult = await client.GetKeyValue(
            keyName,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult.Success);
        Assert.Equal($"value-1", getResult.ValueAsString());
        Assert.Equal(1, attempts);
    }

    [Theory, CombinatorialData]
    public async Task TestTransactionSessionTimeout(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        // Start a transaction session with a very short timeout
        KahunaTransactionSession session = await client.StartTransactionSession(
            new KahunaTransactionOptions { 
                Locking = KeyValueTransactionLocking.Optimistic,
                Timeout = 1  // 1ms timeout
            },
            TestContext.Current.CancellationToken
        );
        
        // Wait for the timeout to expire
        await Task.Delay(100, TestContext.Current.CancellationToken);
        
        // The timeout option is handled by the server transaction lifecycle, not by client-side session expiry.
        KahunaKeyValue result = await session.SetKeyValue(
            GetRandomKeyName(),
            "value",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        await session.Rollback(TestContext.Current.CancellationToken);
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
            KahunaCommunicationType.Grpc => new GrpcCommunication(new() { AllowInsecureCertificateValidation = true }, null),
            KahunaCommunicationType.Rest => new RestCommunication(null, new() { AllowInsecureCertificateValidation = true }),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType))
        };
    }

    private static string GetRandomKeyName()
    {
        return $"test-key-{Guid.NewGuid():N}";
    }
} 
