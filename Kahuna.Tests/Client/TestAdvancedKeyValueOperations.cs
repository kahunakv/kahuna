using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Kahuna.Tests.Client;

public class TestAdvancedKeyValueOperations
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    private readonly ITestOutputHelper outputHelper;

    public TestAdvancedKeyValueOperations(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
    }

    [Theory, CombinatorialData]
    public async Task TestSetManyKeyValues(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyPrefix = $"test-batch-{Guid.NewGuid():N}-";
        
        // Create a batch of key-value pairs
        List<KahunaSetKeyValueRequestItem> items = new();
        for (int i = 0; i < 10; i++)
        {
            items.Add(new KahunaSetKeyValueRequestItem
            {
                Key = $"{keyPrefix}{i}",
                Value = Encoding.UTF8.GetBytes($"value-{i}"),
                ExpiresMs = 10000,
                Flags = KeyValueFlags.Set,
                Durability = durability
            });
        }
        
        // Set many key-values at once
        List<KahunaKeyValue> results = await client.SetManyKeyValues(
            items,
            TestContext.Current.CancellationToken
        );
        
        Assert.Equal(items.Count, results.Count);
        Assert.All(results, result => Assert.True(result.Success));
        
        // Verify each key was set correctly
        for (int i = 0; i < 10; i++)
        {
            KahunaKeyValue getResult = await client.GetKeyValue(
                $"{keyPrefix}{i}",
    durability,
    cancellationToken: TestContext.Current.CancellationToken
            );
            
            Assert.True(getResult.Success);
            Assert.Equal($"value-{i}", getResult.ValueAsString());
        }
    }

    [Theory, CombinatorialData]
    public async Task TestScanAllByPrefixWithMultipleKeys(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyPrefix = $"test-scan-{Guid.NewGuid():N}-";
        
        // Create a set of keys with the same prefix
        for (int i = 0; i < 10; i++)
        {
            KahunaKeyValue result = await client.SetKeyValue(
                $"{keyPrefix}{i}",
                $"value-{i}",
                10000,
                durability: durability,
                cancellationToken: TestContext.Current.CancellationToken
            );
            
            Assert.True(result.Success);
        }
        
        // Create some keys with a different prefix
        string otherPrefix = $"test-other-{Guid.NewGuid():N}-";
        for (int i = 0; i < 5; i++)
        {
            KahunaKeyValue result = await client.SetKeyValue(
                $"{otherPrefix}{i}",
                $"other-value-{i}",
                10000,
                durability: durability,
                cancellationToken: TestContext.Current.CancellationToken
            );
            
            Assert.True(result.Success);
        }
        
        // Scan by prefix
        List<KahunaKeyValue> scanResults = await client.ScanAllByPrefix(
            keyPrefix,
    durability,
    cancellationToken: TestContext.Current.CancellationToken
        );
        
        // Verify we got only the keys with our prefix
        Assert.Equal(10, scanResults.Count);
        Assert.All(scanResults, result => Assert.StartsWith(keyPrefix, result.Key));
        
        // Verify the values are correct
        for (int i = 0; i < 10; i++)
        {
            KahunaKeyValue keyValue = scanResults.FirstOrDefault(kv => kv.Key == $"{keyPrefix}{i}")!;
            Assert.NotNull(keyValue);
            Assert.Equal($"value-{i}", keyValue.ValueAsString());
        }
    }

    [Theory, CombinatorialData]
    public async Task TestGetByBucketWithMultipleKeys(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string bucketPrefix = $"test-bucket-{Guid.NewGuid():N}";
        
        // Create keys in the bucket
        for (int i = 0; i < 10; i++)
        {
            KahunaKeyValue result = await client.SetKeyValue(
                $"{bucketPrefix}/key-{i}",
                $"value-{i}",
                10000,
                durability: durability,
                cancellationToken: TestContext.Current.CancellationToken
            );
            
            Assert.True(result.Success);
        }
        
        // Create keys in a different bucket
        string otherBucketPrefix = $"test-other-bucket-{Guid.NewGuid():N}";
        for (int i = 0; i < 5; i++)
        {
            KahunaKeyValue result = await client.SetKeyValue(
                $"{otherBucketPrefix}/key-{i}",
                $"other-value-{i}",
                10000,
                durability: durability,
                cancellationToken: TestContext.Current.CancellationToken
            );
            
            Assert.True(result.Success);
        }
        
        // Get by bucket
        List<KahunaKeyValue> bucketResults = await client.GetByBucket(
            bucketPrefix,
    durability,
    cancellationToken: TestContext.Current.CancellationToken
        );
        
        // Verify we got only the keys from our bucket
        Assert.Equal(10, bucketResults.Count);
        Assert.All(bucketResults, result => Assert.StartsWith($"{bucketPrefix}/", result.Key));
        
        // Verify the values are correct
        for (int i = 0; i < 10; i++)
        {
            KahunaKeyValue keyValue = bucketResults.FirstOrDefault(kv => kv.Key == $"{bucketPrefix}/key-{i}")!;
            Assert.NotNull(keyValue);
            Assert.Equal($"value-{i}", keyValue.ValueAsString());
        }
    }

    [Theory, CombinatorialData]
    public async Task TestKeyValueRevisionHistory(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Set a key multiple times to create revision history
        KahunaKeyValue result1 = await client.SetKeyValue(
            keyName,
            "value-1",
            10000,
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result1.Success);
        Assert.Equal(0, result1.Revision);
        
        KahunaKeyValue result2 = await client.SetKeyValue(
            keyName,
            "value-2",
            10000,
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result2.Success);
        Assert.Equal(1, result2.Revision);
        
        KahunaKeyValue result3 = await client.SetKeyValue(
            keyName,
            "value-3",
            10000,
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result3.Success);
        Assert.Equal(2, result3.Revision);
        
        // Get the current value
        KahunaKeyValue currentValue = await client.GetKeyValue(
            keyName,
    durability,
    cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(currentValue.Success);
        Assert.Equal("value-3", currentValue.ValueAsString());
        Assert.Equal(2, currentValue.Revision);
        
        // Get a specific revision
        KahunaKeyValue revision1 = await client.GetKeyValueRevision(
            keyName,
            1,
            durability,
            TestContext.Current.CancellationToken
        );
        
        Assert.True(revision1.Success);
        Assert.Equal("value-2", revision1.ValueAsString());
        Assert.Equal(1, revision1.Revision);
        
        // Older revisions may be compacted or unavailable depending on durability/storage.
    }

    [Theory, CombinatorialData]
    public async Task TestLoadTransactionScript(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Load a transaction script
        string scriptText = @"
BEGIN (locking=""optimistic"")
 SET @key @value
 COMMIT
END
";
        
        KahunaTransactionScript script = client.LoadTransactionScript(scriptText);
        
        // Execute the script with parameters
        KahunaKeyValueTransactionResult result = await script.Run(
            new List<KeyValueParameter>
            {
                new() { Key = "@key", Value = keyName },
                new() { Key = "@value", Value = "script-value" }
            },
            TestContext.Current.CancellationToken
        );
        
        Assert.Equal(KeyValueResponseType.Set, result.Type);
        
        // Verify the key was set
        KahunaKeyValue getResult = await client.GetKeyValue(
            keyName,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult.Success);
        Assert.Equal("script-value", getResult.ValueAsString());
    }

    [Theory, CombinatorialData]
    public async Task TestCompareValueAndSetKeyValueSuccess(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Set initial value
        KahunaKeyValue initialResult = await client.SetKeyValue(
            keyName,
            "initial-value",
            10000,
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(initialResult.Success);
        
        // Compare-and-set with correct current value
        KahunaKeyValue casResult = await client.TryCompareValueAndSetKeyValue(
            keyName,
            "new-value",
            "initial-value",
            10000,
            durability,
            TestContext.Current.CancellationToken
        );
        
        Assert.True(casResult.Success);
        
        // Verify the value was updated
        KahunaKeyValue getResult = await client.GetKeyValue(
            keyName,
    durability,
    cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult.Success);
        Assert.Equal("new-value", getResult.ValueAsString());
    }

    [Theory, CombinatorialData]
    public async Task TestCompareValueAndSetKeyValueFailure(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        // Set initial value
        KahunaKeyValue initialResult = await client.SetKeyValue(
            keyName,
            "initial-value",
            10000,
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(initialResult.Success);
        
        // Compare-and-set with incorrect current value
        KahunaKeyValue casResult = await client.TryCompareValueAndSetKeyValue(
            keyName,
            "new-value",
            "wrong-value",
            10000,
            durability,
            TestContext.Current.CancellationToken
        );
        
        Assert.False(casResult.Success);
        
        // Verify the value was not updated
        KahunaKeyValue getResult = await client.GetKeyValue(
            keyName,
    durability,
    cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(getResult.Success);
        Assert.Equal("initial-value", getResult.ValueAsString());
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
} 
