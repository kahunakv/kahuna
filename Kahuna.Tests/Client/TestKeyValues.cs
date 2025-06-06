
using System.Text;
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Tests.Client;

public class TestKeyValues
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    private readonly ITestOutputHelper outputHelper;

    public TestKeyValues(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
    }
    
    [Theory, CombinatorialData]
    public async Task TestEmptyKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        KahunaException ex = await Assert.ThrowsAsync<KahunaException>(() => SetEmptyKey(client, durability));
        Assert.Contains("Failed to set key/value", ex.Message);
    }

    private static async Task SetEmptyKey(KahunaClient client, KeyValueDurability durability)
    {
        await client.SetKeyValue("", "some-value", 10000, durability: durability);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetBytes(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value"u8.ToArray(), 
            10000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueExpiresTimeSpan(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            TimeSpan.FromSeconds(10), 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetBytesExpiresTimeSpan(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value"u8.ToArray(), 
            TimeSpan.FromSeconds(10), 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueTwice(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);

        result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfNotExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.SetIfNotExists, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfNotExistsTwice(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.SetIfNotExists, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.SetIfNotExists, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(result.Success);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.SetIfExists, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(result.Success);
        Assert.Equal(-1, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfExistsTwice(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.Set, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.SetIfExists, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfOrNotExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.Set, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.SetIfExists, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        
        result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.SetIfNotExists, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.False(result.Success);
        Assert.Equal(1, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(
            keyName, 
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
        
        Assert.Equal("some-value", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueTwice(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(
            keyName, 
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
        
        Assert.Equal("some-value", result.ValueAsString());
        
        result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        
        result = await client.GetKeyValue(
            keyName, 
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.NotNull(result.Value);
        Assert.Equal(1, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfNotExistsAndGetValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            10000, 
            flags: KeyValueFlags.SetIfNotExists, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(
            keyName, 
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
        
        Assert.Equal("some-value", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueExpires(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value", 
            1000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(
            keyName, 
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
        
        Assert.Equal("some-value", result.ValueAsString());
        
        await Task.Delay(1500, TestContext.Current.CancellationToken);
        
        result = await client.GetKeyValue(
            keyName, 
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.Null(result.Value);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueExpires2(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType); 
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        KahunaKeyValue result1 = await client.SetKeyValue(
            keyName1, 
            "some-value", 
            1000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(result1.Success);
        Assert.Equal(0, result1.Revision);
        
        result1 = await client.SetKeyValue(
            keyName2, 
            "some-value", 
            1000, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(result1.Success);
        Assert.Equal(0, result1.Revision);
        
        result1 = await client.GetKeyValue(keyName1, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result1.Value);
        Assert.Equal(0, result1.Revision);
        
        Assert.Equal("some-value", result1.ValueAsString());
        
        KahunaKeyValue result2 = await client.GetKeyValue(keyName2, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result2.Value);
        Assert.Equal(0, result2.Revision);
        
        Assert.Equal("some-value", result2.ValueAsString());
        
        await Task.Delay(1500, TestContext.Current.CancellationToken);
        
        result1 = await client.GetKeyValue(keyName1, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result1.Value);
        Assert.Equal(0, result1.Revision);
        
        result2 = await client.GetKeyValue(keyName1, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result2.Value);
        Assert.Equal(0, result2.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueExpiresMultiple(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        KahunaKeyValue result1 = await client.SetKeyValue(
            keyName1, 
            "some-value", 
            1000, 
            durability: durability, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(result1.Success);
        Assert.Equal(0, result1.Revision);
        
        KahunaKeyValue result2 = await client.SetKeyValue(
            keyName2, 
            "some-value", 
            1000, 
            durability: durability, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(result2.Success);
        Assert.Equal(0, result2.Revision);
        
        result1 = await client.GetKeyValue(keyName1, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result1.Value);
        Assert.Equal(0, result1.Revision);
        
        Assert.Equal("some-value", result1.ValueAsString());
        
        result2 = await client.GetKeyValue(keyName2, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result2.Value);
        Assert.Equal(0, result2.Revision);
        
        Assert.Equal("some-value", result2.ValueAsString());
        
        await Task.Delay(1500, TestContext.Current.CancellationToken);
        
        result1 = await client.GetKeyValue(keyName1, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result1.Value);
        Assert.Equal(0, result1.Revision);
        
        result1 = await client.GetKeyValue(keyName1, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result1.Value);
        Assert.Equal(0, result1.Revision);
        
        result1 = await client.SetKeyValue(keyName1, "some-value", 1000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result1.Success);
        Assert.Equal(1, result1.Revision);
        
        result2 = await client.SetKeyValue(keyName2, "some-value", 1000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result2.Success);
        Assert.Equal(1, result2.Revision);
        
        result1 = await client.GetKeyValue(keyName1, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result1.Value);
        Assert.Equal(1, result1.Revision);
        
        result2 = await client.GetKeyValue(keyName2, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result2.Value);
        Assert.Equal(1, result2.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueShortExpires(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 1, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        await Task.Delay(50, TestContext.Current.CancellationToken);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result.Value);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndExtend(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 1000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.ExtendKeyValue(keyName, 5000, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        await Task.Delay(2000, TestContext.Current.CancellationToken);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndExtendTimeSpan(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", TimeSpan.FromSeconds(1), durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.ExtendKeyValue(keyName, TimeSpan.FromSeconds(5), durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        await Task.Delay(2000, TestContext.Current.CancellationToken);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndDelete(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
        
        Assert.Equal("some-value", result.ValueAsString());
        
        result = await client.DeleteKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result.Value);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(keyName, "some-value-2", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(1, result.Revision);
        
        Assert.Equal("some-value-2", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndDeleteWithRefObject(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await result.Delete(cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
        
        Assert.Equal("some-value", result.ValueAsString());
        
        result = await client.ExistsKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndDeleteAndSetIfExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
        
        Assert.Equal("some-value", result.ValueAsString());
        
        result = await client.DeleteKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result.Value);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(keyName, "some-value-2", 10000, flags: KeyValueFlags.SetIfExists, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result.Value);
        Assert.Equal(0, result.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestMultiSet(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        List<KahunaKeyValue> responses = await client.SetManyKeyValues([
            new()
            {
                Key = keyName1,
                Value = "some-value 1"u8.ToArray(),                
                Durability = durability
            },
            new()
            {
                Key = keyName2,
                Value = "some-valu 2e"u8.ToArray(),                
                Durability = durability
            }                        
        ], cancellationToken: TestContext.Current.CancellationToken);
               
        Assert.Equal(2, responses.Count);
        Assert.True(responses[0].Success);
        Assert.True(responses[1].Success);
        Assert.Equal(0, responses[0].Revision);
        Assert.Equal(0, responses[1].Revision);
    }

    [Theory, CombinatorialData]
    public async Task TestCompareValueAndSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.TryCompareValueAndSetKeyValue(keyName, "some-new-value", "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(1, result.Revision);
        
        Assert.Equal("some-new-value", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestCompareUnknownValueAndSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.TryCompareValueAndSetKeyValue(keyName, "some-new-value", "other-some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(0, result.Revision);
        
        Assert.Equal("some-value", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestCompareRevisionAndSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(keyName, "some-new-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        
        result = await client.TryCompareRevisionAndSetKeyValue(keyName, "some-new-new-value", 1, 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(2, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(2, result.Revision);
        
        Assert.Equal("some-new-new-value", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestCompareUnknownRevisionAndSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(keyName, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(keyName, "some-new-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        
        result = await client.TryCompareRevisionAndSetKeyValue(keyName, "some-new-new-value",10, 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result.Success);
        Assert.Equal(1, result.Revision);
        
        result = await client.GetKeyValue(keyName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result.Value);
        Assert.Equal(1, result.Revision);
        
        Assert.Equal("some-new-value", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndGetRevision(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(
            keyName, 
            "some-value",  
            flags: KeyValueFlags.Set, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(
            keyName, 
            "some-value-2",             
            flags: KeyValueFlags.Set, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        
        result = await client.GetKeyValue(
            keyName, 
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        Assert.Equal("some-value-2", result.ValueAsString());

        //await Task.Delay(1000, TestContext.Current.CancellationToken);
        
        result = await client.GetKeyValueRevision(
            keyName, 
            0,
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        Assert.Equal("some-value", result.ValueAsString());
        
        result = await client.SetKeyValue(
            keyName, 
            "some-value-3",  
            flags: KeyValueFlags.Set, 
            durability: durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(2, result.Revision);
        
        result = await client.GetKeyValueRevision(
            keyName, 
            1,
            durability,
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(result.Success);
        Assert.Equal(1, result.Revision);
        Assert.Equal("some-value-2", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestScanAllByPrefix(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string prefix = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(prefix + "/" + GetRandomKeyName(), "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(prefix + "/" + GetRandomKeyName(), "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(prefix + "/" + GetRandomKeyName(), "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        List<KahunaKeyValue> items = await client.ScanAllByPrefix(prefix, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, items.Count);
    }
    
    [Theory, CombinatorialData]
    public async Task TestScanAllByExactPrefix(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string randomKey = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(randomKey, "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        List<KahunaKeyValue> items = await client.ScanAllByPrefix(randomKey, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Single(items);
    }
    
    [Theory, CombinatorialData]
    public async Task TestGetByBucket(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string prefix = GetRandomKeyName();

        KahunaKeyValue result = await client.SetKeyValue(prefix + "/" + GetRandomKeyName(), "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(prefix + "/" + GetRandomKeyName(), "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);
        
        result = await client.SetKeyValue(prefix + "/" + GetRandomKeyName(), "some-value", 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.Equal(0, result.Revision);

        await Task.Delay(1000, cancellationToken: TestContext.Current.CancellationToken);
        
        List<KahunaKeyValue> items = await client.GetByBucket(prefix, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        
        foreach (KahunaKeyValue item in items)
        {
            Assert.True(item.Success);
            Assert.StartsWith(prefix, item.Key);
        }

        Assert.Equal(3, items.Count);
    }
    
    [Theory, CombinatorialData]
    public async Task TestGetByBucketUnknown(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string prefix = GetRandomKeyName();
        
        List<KahunaKeyValue> items = await client.GetByBucket(prefix, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Empty(items);
    }
    
    private KahunaClient GetClientByType(KahunaCommunicationType communicationType, KahunaClientType clientType)
    {
        return clientType switch
        {
            KahunaClientType.SingleEndpoint => new(url, null, GetCommunicationByType(communicationType)),
            KahunaClientType.PoolOfEndpoints => new(urls, null, GetCommunicationByType(communicationType)),
            _ => throw new ArgumentOutOfRangeException(nameof(clientType), clientType, null)
        };
    }

    private static IKahunaCommunication GetCommunicationByType(KahunaCommunicationType communicationType)
    {
        return communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(null, null),
            KahunaCommunicationType.Rest => new RestCommunication(null),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType), communicationType, null)
        };
    }
    
    private static string GetRandomKeyName()
    {
        return Guid.NewGuid().ToString("N")[..16];
    }
}
