
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Tests.Client;

public class TestKeyValueTransactions
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    [Theory, CombinatorialData]
    public async Task TestBasicSet(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransaction("SET `" + GetRandomKeyName() + "` 'some value'");
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetPlaceholder(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransaction("SET `" + GetRandomKeyName() + "` @value", parameters: [
            new() { Key = "@value", Value = "some value" }
        ]);
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicGet(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        string keyName = GetRandomKeyName();
        
        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransaction("SET `" + keyName + "` 'some value'");
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.Revision);

        response = await client.ExecuteKeyValueTransaction("GET `" + keyName + "`");
        
        Assert.Equal(KeyValueResponseType.Get, response.Type);
        Assert.Equal(0, response.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicExtend(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        string keyName = GetRandomKeyName();
        
        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransaction("SET `" + keyName + "` 'some value'");
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.Revision);

        response = await client.ExecuteKeyValueTransaction("EXTEND `" + keyName + "` 1000");
        
        Assert.Equal(KeyValueResponseType.Extended, response.Type);
        Assert.Equal(0, response.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        string keyName = GetRandomKeyName();
        
        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransaction("SET `" + keyName + "` 'some value'");
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.Revision);

        response = await client.ExecuteKeyValueTransaction("EXISTS `" + keyName + "`");
        
        Assert.Equal(KeyValueResponseType.Exists, response.Type);
        Assert.Equal(0, response.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicDelete(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        string keyName = GetRandomKeyName();
        
        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransaction("SET `" + keyName + "` 'some value'");
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.Revision);

        response = await client.ExecuteKeyValueTransaction("DELETE `" + keyName + "`");
        
        Assert.Equal(KeyValueResponseType.Deleted, response.Type);
        Assert.Equal(0, response.Revision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSnapshotIsolationConflict(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.Single)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyNameA = GetRandomKeyName();
        string keyNameB = GetRandomKeyName();
        
        KahunaKeyValue result = await client.SetKeyValue(keyNameA, "10");
        Assert.True(result.Success);
        
        result = await client.SetKeyValue(keyNameB, "10");
        Assert.True(result.Success);

        const string script1 = """
        BEGIN (locking="optimistic")
         LET av = GET @keyA
         LET bv = GET @keyB
         IF to_int(av) + to_int(bv) = 20 THEN
          SET @keyA 0
         END
         COMMIT
        END 
        """;
        
        const string script2 = """
        BEGIN (locking="optimistic")
         LET av = GET @keyA
         LET bv = GET @keyB
         IF to_int(av) + to_int(bv) = 20 THEN
          SET @keyB 0
         END
         COMMIT
        END 
        """;

        try
        {
            await Task.WhenAll(
                client.ExecuteKeyValueTransaction(
                    script1,
                    null, 
                    [new() { Key = "@keyA", Value = keyNameA }, new() { Key = "@keyB", Value = keyNameB }]
                ),
                client.ExecuteKeyValueTransaction(
                    script2,
                    null, 
                    [new() { Key = "@keyA", Value = keyNameA }, new() { Key = "@keyB", Value = keyNameB }]
                )
            );
            
            Assert.False(true);
        }
        catch (KahunaException e)
        {
            Assert.Equal(KeyValueResponseType.Aborted, e.KeyValueErrorCode);
        }
        
        KahunaKeyValue resultA = await client.GetKeyValue(keyNameA);
        KahunaKeyValue resultB = await client.GetKeyValue(keyNameB);
        
        Assert.True(
            ("10" == resultA.ValueAsString() && "0" == resultB.ValueAsString()) || 
            ("0" == resultA.ValueAsString() && "10" == resultB.ValueAsString())
        );
    }
    
    [Theory, CombinatorialData]
    public async Task TestSerializableConflict(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.Single)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        int oneFailed = 0;
        
        string keyNameA = GetRandomKeyName();

        const string script1 = """
        BEGIN (locking="pessimistic")
         LET av = GET @keyA
         SLEEP 500
         SET @keyA 30
         COMMIT
        END 
        """;
        
        const string script2 = """
        BEGIN (locking="pessimistic")
         SET @keyA 20
         COMMIT
        END 
        """;

        try
        {
            await Task.WhenAll(
                client.ExecuteKeyValueTransaction(
                    script1,
                    null, 
                    [new() { Key = "@keyA", Value = keyNameA }]
                ),
                client.ExecuteKeyValueTransaction(
                    script2,
                    null, 
                    [new() { Key = "@keyA", Value = keyNameA }]
                )
            );
            
            Assert.False(true);
        }
        catch (KahunaException e)
        {
            oneFailed++;
            
            Assert.Equal(KeyValueResponseType.Aborted, e.KeyValueErrorCode);
        }
        
        Assert.Equal(1, oneFailed);
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
    
    private static string GetRandomKeyName()
    {
        return Guid.NewGuid().ToString("N")[..16];
    }
}