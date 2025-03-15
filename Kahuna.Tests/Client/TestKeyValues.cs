
using System.Text;
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

namespace Kahuna.Tests.Client;

public class TestKeyValues
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    //private int total;

    private static string GetRandomKeyName()
    {
        return Guid.NewGuid().ToString("N")[..16];
    }
    
    [Theory, CombinatorialData]
    public async Task TestEmptyKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        KahunaException ex = await Assert.ThrowsAsync<KahunaException>(() => SetEmptyKey(client, consistency));
        Assert.Contains("Failed to set key/value", ex.Message);
    }

    private static async Task SetEmptyKey(KahunaClient client, KeyValueConsistency consistency)
    {
        await client.SetKeyValue("", "some-value", 10000, consistency: consistency);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueExpiresTimeSpan(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", TimeSpan.FromSeconds(10), consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueTwice(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);

        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfNotExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfNotExistsTwice(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.False(success);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfExists, consistency: consistency);
        Assert.False(success);
        Assert.Equal(-1, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfExistsTwice(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.Set, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfOrNotExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.Set, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.False(success);
        Assert.Equal(1, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value));
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueTwice(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value));
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(1, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueIfNotExistsAndGetValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value));
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueExpires(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value));
        
        await Task.Delay(1500, TestContext.Current.CancellationToken);
        
        (value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.Null(value);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueExpires2(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType); 
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName1, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName2, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (byte[]? value1, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value1));
        
        (byte[]? value2, revision) = await client.GetKeyValue(keyName2, consistency);
        Assert.NotNull(value2);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value2));
        
        await Task.Delay(1500, TestContext.Current.CancellationToken);
        
        (value1, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
        
        (value2, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.Null(value2);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueExpiresMultiple(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName1, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName2, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (byte[]? value1, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value1));
        
        (byte[]? value2, revision) = await client.GetKeyValue(keyName2, consistency);
        Assert.NotNull(value2);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value2));
        
        await Task.Delay(1500, TestContext.Current.CancellationToken);
        
        (value1, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
        
        (value2, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.Null(value2);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName1, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (success, revision) = await client.SetKeyValue(keyName2, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (value1, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        Assert.Equal(1, revision);
        
        (value2, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value2);
        Assert.Equal(1, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetAndGetValueShortExpires(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 1, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        await Task.Delay(50, TestContext.Current.CancellationToken);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.Null(value);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndExtend(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.ExtendKeyValue(keyName, 5000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        await Task.Delay(2000, TestContext.Current.CancellationToken);
        
        (byte[]? value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndExtendTimeSpan(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.ExtendKeyValue(keyName, TimeSpan.FromSeconds(5), consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        await Task.Delay(2000, TestContext.Current.CancellationToken);
        
        (byte[]? value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndDelete(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (byte[]? value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value1));
        
        (success, revision) = await client.DeleteKeyValue(keyName, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value-2", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(1, revision);
        
        Assert.Equal("some-value-2", Encoding.UTF8.GetString(value1));
    }
    
    [Theory, CombinatorialData]
    public async Task TestSingleSetValueAndDeleteAndSetIfExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (byte[]? value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value1));
        
        (success, revision) = await client.DeleteKeyValue(keyName, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value-2", 10000, flags: KeyValueFlags.SetIfExists, consistency: consistency);
        Assert.False(success);
        Assert.Equal(0, revision);
        
        (value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
    }

    [Theory, CombinatorialData]
    public async Task TestCompareValueAndSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.TryCompareValueAndSetKeyValue(keyName, "some-new-value", "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(1, revision);
        
        Assert.Equal("some-new-value", Encoding.UTF8.GetString(value));
    }
    
    [Theory, CombinatorialData]
    public async Task TestCompareUnknownValueAndSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.TryCompareValueAndSetKeyValue(keyName, "some-new-value", "other-some-value", 10000, consistency: consistency);
        Assert.False(success);
        Assert.Equal(0, revision);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", Encoding.UTF8.GetString(value));
    }
    
    [Theory, CombinatorialData]
    public async Task TestCompareRevisionAndSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-new-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (success, revision) = await client.TryCompareRevisionAndSetKeyValue(keyName, "some-new-new-value", 1, 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(2, revision);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(2, revision);
        
        Assert.Equal("some-new-new-value", Encoding.UTF8.GetString(value));
    }
    
    [Theory, CombinatorialData]
    public async Task TestCompareUnknownRevisionAndSetKeyValue(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(KeyValueConsistency.Ephemeral, KeyValueConsistency.Linearizable)] KeyValueConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-new-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (success, revision) = await client.TryCompareRevisionAndSetKeyValue(keyName, "some-new-new-value",10, 10000, consistency: consistency);
        Assert.False(success);
        Assert.Equal(1, revision);
        
        (byte[]? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(1, revision);
        
        Assert.Equal("some-new-value", Encoding.UTF8.GetString(value));
    }
    
    private KahunaClient GetClientByType(KahunaCommunicationType communicationType, KahunaClientType clientType)
    {
        return clientType switch
        {
            KahunaClientType.Single => new(url, null, GetCommunicationByType(communicationType)),
            KahunaClientType.Pool => new(urls, null, GetCommunicationByType(communicationType)),
            _ => throw new ArgumentOutOfRangeException(nameof(clientType), clientType, null)
        };
    }

    private static IKahunaCommunication GetCommunicationByType(KahunaCommunicationType communicationType)
    {
        return communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(null),
            KahunaCommunicationType.Rest => new RestCommunication(null),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType), communicationType, null)
        };
    }
}
