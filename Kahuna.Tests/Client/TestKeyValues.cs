
using Kahuna.Client;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Tests.Client;

public class TestKeyValues
{
    private readonly KahunaClient kahuna = new("https://localhost:8082", null);

    //private int total;

    private static string GetRandomKeyName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory]
    //[InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetValue(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        Assert.True(await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency));
    }
    
    [Theory]
    //[InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValue(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        Assert.True(await kahuna.SetKeyValue(keyName, "some-value", 10000, consistency));
        
        string? value = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        
        Assert.Equal("some-value", value);
    }
    
    [Theory]
    //[InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueExpires(KeyValueConsistency consistency)
    {
        string keyName = GetRandomKeyName();

        Assert.True(await kahuna.SetKeyValue(keyName, "some-value", 1000, consistency));
        
        string? value = await kahuna.GetKeyValue(keyName, consistency);
        Assert.NotNull(value);
        
        Assert.Equal("some-value", value);
        
        await Task.Delay(1500);
        
        value = await kahuna.GetKeyValue(keyName, consistency);
        Assert.Null(value);
    }
    
    [Theory]
    //[InlineData(KeyValueConsistency.Linearizable)]
    [InlineData(KeyValueConsistency.Ephemeral)]
    public async Task TestSingleSetAndGetValueExpires2(KeyValueConsistency consistency)
    {
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();

        Assert.True(await kahuna.SetKeyValue(keyName1, "some-value", 1000, consistency));
        Assert.True(await kahuna.SetKeyValue(keyName2, "some-value", 1000, consistency));
        
        string? value1 = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.NotNull(value1);
        
        Assert.Equal("some-value", value1);
        
        string? value2 = await kahuna.GetKeyValue(keyName2, consistency);
        Assert.NotNull(value2);
        
        Assert.Equal("some-value", value2);
        
        await Task.Delay(1500);
        
        value1 = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.Null(value1);
        
        value2 = await kahuna.GetKeyValue(keyName1, consistency);
        Assert.Null(value2);
    }
}