using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

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
    public async Task TestValidateAcquireSingleLock(KeyValueConsistency consistency)
    {
        string lockName = GetRandomKeyName();

        await kahuna.SetKeyValue(lockName, "some-value", 10000, KeyValueConsistency.Ephemeral);

        //Assert.True(kLock.IsAcquired);
    }
}