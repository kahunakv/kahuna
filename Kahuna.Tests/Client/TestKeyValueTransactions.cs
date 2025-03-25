
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