
using Kahuna.Client;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Tests.Client;

public class TestKeyValues
{
    private readonly KahunaClient kahuna = new(["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"], null);

    //private int total;

    private static string GetRandomKeyName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValue(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueTwice(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);

        (success, revision) = await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValue(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value);
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueTwice(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value);
        
        (success, revision) = await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (value, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(1, revision);
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueExpires(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName, "some-value", 1000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value);
        
        await Task.Delay(1500);
        
        (value, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.Null(value);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueExpires2(KeyValueConsistency consistency)
    {
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName1, "some-value", 1000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await kahuna.SetKeyValue(keyName2, "some-value", 1000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value1, revision) = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value1);
        
        (string? value2, revision) = await kahuna.GetKeyValue(keyName2, consistency);
        Assert.NotNull(value2);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value2);
        
        await Task.Delay(1500);
        
        (value1, revision) = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
        
        (value2, revision) = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.Null(value2);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueExpiresMultiple(KeyValueConsistency consistency)
    {
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName1, "some-value", 1000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await kahuna.SetKeyValue(keyName2, "some-value", 1000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value1, revision) = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value1);
        
        (string? value2, revision) = await kahuna.GetKeyValue(keyName2, consistency);
        Assert.NotNull(value2);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value2);
        
        await Task.Delay(1500);
        
        (value1, revision) = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
        
        (value2, revision) = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.Null(value2);
        Assert.Equal(0, revision);
        
        (success, revision) = await kahuna.SetKeyValue(keyName1, "some-value", 1000, consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (success, revision) = await kahuna.SetKeyValue(keyName2, "some-value", 1000, consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (value1, revision) = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        Assert.Equal(1, revision);
        
        (value2, revision) = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value2);
        Assert.Equal(1, revision);
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueAndExtend(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName, "some-value", 1000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (success, revision) = await kahuna.ExtendKeyValue(keyName, 5000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        await Task.Delay(2000);
        
        (string? value1, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
    }
    
    [Theory]
    [InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValueAndDelete(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        (bool success, long revision) = await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency);
        Assert.True(success);
        Assert.Equal(0, revision);
        
        (string? value1, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(0, revision);
        
        Assert.Equal("some-value", value1);
        
        success = await kahuna.DeleteKeyValue(keyName, consistency);
        Assert.True(success);
        
        (value1, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.Null(value1);
        Assert.Equal(0, revision);
        
        (success, revision) = await kahuna.SetKeyValue(keyName, "some-value-2", 10000, consistency);
        Assert.True(success);
        Assert.Equal(1, revision);
        
        (value1, revision) = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value1);
        Assert.Equal(1, revision);
        
        Assert.Equal("some-value-2", value1);
    }
}