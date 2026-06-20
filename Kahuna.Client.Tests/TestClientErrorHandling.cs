using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kahuna.Client.Tests;

public class TestClientErrorHandling
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    private readonly ITestOutputHelper outputHelper;

    public TestClientErrorHandling(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
    }

    [Theory, CombinatorialData]
    public async Task TestSetKeyValueWithEmptyKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        // Test with empty key
        KahunaException ex = await Assert.ThrowsAsync<KahunaException>(async () =>
        {
            await client.SetKeyValue("", "test-value", cancellationToken: TestContext.Current.CancellationToken);
        });

        Assert.Contains("Failed to set key/value", ex.Message);
    }

    [Theory, CombinatorialData]
    public async Task TestSetKeyValueWithNullValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        if (communicationType == KahunaCommunicationType.Grpc)
        {
            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            {
                await client.SetKeyValue(
                    keyName,
                    null as byte[],
                    10000,
                    cancellationToken: TestContext.Current.CancellationToken
                );
            });
            
            return;
        }

        // Test with null value (byte[] overload)
        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            null as byte[], 
            10000, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        
        // Verify the value is null
        KahunaKeyValue getResult = await client.GetKeyValue(
            keyName,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult.Success);
        Assert.NotNull(getResult.Value);
        Assert.Empty(getResult.Value);
        Assert.Equal("", getResult.ValueAsString());
    }

    [Theory, CombinatorialData]
    public async Task TestGetNonExistentKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string nonExistentKey = $"non-existent-key-{Guid.NewGuid():N}";

        // Try to get a key that doesn't exist
        KahunaKeyValue result = await client.GetKeyValue(
            nonExistentKey,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(result.Success);
        Assert.Null(result.Value);
    }

    [Theory, CombinatorialData]
    public async Task TestDeleteNonExistentKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string nonExistentKey = $"non-existent-key-{Guid.NewGuid():N}";

        // Try to delete a key that doesn't exist
        KahunaKeyValue result = await client.DeleteKeyValue(
            nonExistentKey,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(result.Success);
    }

    [Theory, CombinatorialData]
    public async Task TestExtendNonExistentKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string nonExistentKey = $"non-existent-key-{Guid.NewGuid():N}";

        // Try to extend a key that doesn't exist
        KahunaKeyValue result = await client.ExtendKeyValue(
            nonExistentKey,
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(result.Success);
    }

    [Theory, CombinatorialData]
    public async Task TestCompareValueAndSetWithNonExistentKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string nonExistentKey = $"non-existent-key-{Guid.NewGuid():N}";

        // Try to compare-and-set on a key that doesn't exist
        KahunaKeyValue result = await client.TryCompareValueAndSetKeyValue(
            nonExistentKey,
            "new-value",
            "old-value",
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(result.Success);
    }

    [Theory, CombinatorialData]
    public async Task TestCompareRevisionAndSetWithNonExistentKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string nonExistentKey = $"non-existent-key-{Guid.NewGuid():N}";

        // Try to compare-revision-and-set on a key that doesn't exist
        KahunaKeyValue result = await client.TryCompareRevisionAndSetKeyValue(
            nonExistentKey,
            "new-value",
            0,
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(result.Success);
    }

    [Theory, CombinatorialData]
    public async Task TestInvalidTransactionScript(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        // Test with invalid script syntax
        KahunaException ex = await Assert.ThrowsAsync<KahunaException>(async () =>
        {
            await client.ExecuteKeyValueTransactionScript(
                "INVALID SYNTAX",
                cancellationToken: TestContext.Current.CancellationToken
            );
        });

        Assert.Contains("Syntax error", ex.Message);
    }

    [Theory, CombinatorialData]
    public async Task TestLockWithInvalidResource(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        // Test with empty resource name
        KahunaException ex = await Assert.ThrowsAsync<KahunaException>(async () =>
        {
            await client.GetOrCreateLock(
                "",
                expiryTime: 10000,
                cancellationToken: TestContext.Current.CancellationToken
            );
        });

        Assert.Contains("Failed to lock", ex.Message);
    }

    [Theory, CombinatorialData]
    public async Task TestGetLockInfoForNonExistentLock(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string nonExistentLock = $"non-existent-lock-{Guid.NewGuid():N}";

        // Try to get info for a lock that doesn't exist
        await Assert.ThrowsAsync<KahunaException>(async () =>
        {
            await client.GetLockInfo(
                nonExistentLock,
                cancellationToken: TestContext.Current.CancellationToken
            );
        });
    }

    [Theory, CombinatorialData]
    public async Task TestUnlockWithInvalidOwner(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomKeyName();
        
        // First acquire a lock
        KahunaLock kLock = await client.GetOrCreateLock(
            lockName,
            expiryTime: 10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(kLock.IsAcquired);
        
        // Try to unlock with an invalid owner
        try
        {
            bool unlockResult = await client.Unlock(
                lockName,
                "invalid-owner",
                cancellationToken: TestContext.Current.CancellationToken
            );
            
            Assert.False(unlockResult);
        }
        catch (KahunaException ex)
        {
            Assert.Contains("InvalidOwner", ex.Message);
        }
        
        // Clean up
        await kLock.DisposeAsync();
    }

    [Theory, CombinatorialData]
    public async Task TestSetKeyValueWithLargeValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Create a large value (1MB)
        byte[] largeValue = new byte[1024 * 1024];
        Random.Shared.NextBytes(largeValue);
        
        // Set the large value
        KahunaKeyValue result = await client.SetKeyValue(
            keyName,
            largeValue,
            10000,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        
        // Retrieve and verify
        KahunaKeyValue getResult = await client.GetKeyValue(
            keyName,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult.Success);
        Assert.Equal(largeValue.Length, getResult.Value?.Length);
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
