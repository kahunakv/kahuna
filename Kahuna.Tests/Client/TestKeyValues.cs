
using Kahuna.Client;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Tests.Client;

public class TestKeyValues
{
    private readonly KahunaClient kahunaSingle = new("https://localhost:8082", null);
    
    private readonly KahunaClient kahunaPool = new(["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"], null);

    //private int total;

    private static string GetRandomKeyName()
    {
        return Guid.NewGuid().ToString("N")[..16];
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestEmptyKey(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        KahunaException ex = await Assert.ThrowsAsync<KahunaException>(() => SetEmptyKey(client, consistency));
        Assert.Equal("Failed to set key/value: KeyvalueResponseTypeInvalidInput", ex.Message);
    }

    private static async Task SetEmptyKey(KahunaClient client, KeyValueConsistency consistency)
    {
        await client.SetKeyValue("", "some-value", 10000, consistency: consistency);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValue(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueExpiresTimeSpan(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", TimeSpan.FromSeconds(10), consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueTwice(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);

        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueIfNotExists(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueIfNotExistsTwice(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.False(success);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueIfExists(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfExists, consistency: consistency);
        Assert.False(success);
        Assert.Equal(-1, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueIfExistsTwice(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.Set, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueIfOrNotExists(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
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
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValue(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueTwice(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value);
        
        (success, revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(1, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueIfNotExistsAndGetValue(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, flags: KeyValueFlags.SetIfNotExists, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueExpires(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value);
        
        await Task.Delay(1500);
        
        (value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.Null(value);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueExpires2(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType); 
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName1, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName2, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value1, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value1);
        
        (string? value2, revision) = await client.GetKeyValue(keyName2, consistency);
        Assert.NotNull(value2);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value2);
        
        await Task.Delay(1500);
        
        (value1, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
        
        (value2, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.Null(value2);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueExpiresMultiple(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName1, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.SetKeyValue(keyName2, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value1, revision) = await client.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value1);
        
        (string? value2, revision) = await client.GetKeyValue(keyName2, consistency);
        Assert.NotNull(value2);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value2);
        
        await Task.Delay(1500);
        
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
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueShortExpires(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 1, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        await Task.Delay(50);
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.Null(value);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueAndExtend(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 1000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.ExtendKeyValue(keyName, 5000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        await Task.Delay(2000);
        
        (string? value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueAndExtendTimeSpan(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.ExtendKeyValue(keyName, TimeSpan.FromSeconds(5), consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        await Task.Delay(2000);
        
        (string? value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueAndDelete(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value1);
        
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
        
        Assert.Equal("some-value-2", value1);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueAndDeleteAndSetIfExists(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value1, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value1);
        
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

    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestCompareValueAndSetKeyValue(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.TryCompareValueAndSetKeyValue(keyName, "some-new-value", "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(1, revision);
        
        Assert.Equal("some-new-value", value);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestCompareUnknownValueAndSetKeyValue(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await client.SetKeyValue(keyName, "some-value", 10000, consistency: consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await client.TryCompareValueAndSetKeyValue(keyName, "some-new-value", "other-some-value", 10000, consistency: consistency);
        Assert.False(success);
        Assert.Equal(0, revision);
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestCompareRevisionAndSetKeyValue(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
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
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(2, revision);
        
        Assert.Equal("some-new-new-value", value);
    }
    
    [Theory]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Single, KeyValueConsistency.Ephemeral)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Linearizable)]
    [InlineData(KahunaClientType.Pool, KeyValueConsistency.Ephemeral)]
    public async Task TestCompareUnknownRevisionAndSetKeyValue(KahunaClientType clientType, KeyValueConsistency consistency)
    {
        KahunaClient client = GetClientByType(clientType);
        
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
        
        (string? value, revision) = await client.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(1, revision);
        
        Assert.Equal("some-new-value", value);
    }
    
    private KahunaClient GetClientByType(KahunaClientType clientType)
    {
        return clientType switch
        {
            KahunaClientType.Single => kahunaSingle,
            KahunaClientType.Pool => kahunaPool,
            _ => throw new ArgumentOutOfRangeException(nameof(clientType), clientType, null)
        };
    }
}

public enum KahunaClientType
{
    Single,
    Pool
}

