using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using System.Text;

namespace Kahuna.Client.Tests;

public class TestDeleteManyKeyValues
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    [Theory, CombinatorialData]
    public async Task TestDeleteManyPersistentKeys(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        string keyPrefix = GetRandomKeyName();

        List<string> keys = [];

        for (int i = 0; i < 3; i++)
        {
            string key = $"{keyPrefix}-persistent-{i}";
            keys.Add(key);

            KahunaKeyValue setResult = await client.SetKeyValue(
                key,
                $"value-{i}",
                10000,
                durability: KeyValueDurability.Persistent,
                cancellationToken: TestContext.Current.CancellationToken
            );

            Assert.True(setResult.Success);
        }

        List<KahunaKeyValue> deleteResults = await client.DeleteManyKeyValues(
            keys,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(keys.Count, deleteResults.Count);
        Assert.All(deleteResults, result => Assert.True(result.Success));
        Assert.All(deleteResults, result => Assert.Equal(KeyValueDurability.Persistent, result.Durability));

        foreach (string key in keys)
        {
            KahunaKeyValue getResult = await client.GetKeyValue(
                key,
                KeyValueDurability.Persistent,
                cancellationToken: TestContext.Current.CancellationToken
            );

            Assert.False(getResult.Success);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestDeleteManyEphemeralKeys(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        string keyPrefix = GetRandomKeyName();

        List<string> keys = [];

        for (int i = 0; i < 3; i++)
        {
            string key = $"{keyPrefix}-ephemeral-{i}";
            keys.Add(key);

            KahunaKeyValue setResult = await client.SetKeyValue(
                key,
                $"value-{i}",
                10000,
                durability: KeyValueDurability.Ephemeral,
                cancellationToken: TestContext.Current.CancellationToken
            );

            Assert.True(setResult.Success);
        }

        List<KahunaKeyValue> deleteResults = await client.DeleteManyKeyValues(
            keys,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(keys.Count, deleteResults.Count);
        Assert.All(deleteResults, result => Assert.True(result.Success));
        Assert.All(deleteResults, result => Assert.Equal(KeyValueDurability.Ephemeral, result.Durability));

        foreach (string key in keys)
        {
            KahunaKeyValue getResult = await client.GetKeyValue(
                key,
                KeyValueDurability.Ephemeral,
                cancellationToken: TestContext.Current.CancellationToken
            );

            Assert.False(getResult.Success);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestDeleteManyMixedDurability(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        string persistentKey = GetRandomKeyName();
        string ephemeralKey = GetRandomKeyName();

        Assert.True((await client.SetKeyValue(
            persistentKey,
            "persistent-value",
            10000,
            durability: KeyValueDurability.Persistent,
            cancellationToken: TestContext.Current.CancellationToken)).Success);

        Assert.True((await client.SetKeyValue(
            ephemeralKey,
            "ephemeral-value",
            10000,
            durability: KeyValueDurability.Ephemeral,
            cancellationToken: TestContext.Current.CancellationToken)).Success);

        List<KahunaKeyValue> deleteResults = await client.DeleteManyKeyValues(
        [
            new() { Key = persistentKey, Durability = KeyValueDurability.Persistent },
            new() { Key = ephemeralKey, Durability = KeyValueDurability.Ephemeral }
        ], TestContext.Current.CancellationToken);

        Assert.Equal(2, deleteResults.Count);
        Assert.All(deleteResults, result => Assert.True(result.Success));

        Assert.False((await client.GetKeyValue(persistentKey, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken)).Success);
        Assert.False((await client.GetKeyValue(ephemeralKey, KeyValueDurability.Ephemeral, cancellationToken: TestContext.Current.CancellationToken)).Success);
    }

    [Theory, CombinatorialData]
    public async Task TestDeleteManyWithExistingAndMissingKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        string existingKey = GetRandomKeyName();
        string missingKey = GetRandomKeyName();

        Assert.True((await client.SetKeyValue(
            existingKey,
            "value",
            10000,
            durability: KeyValueDurability.Persistent,
            cancellationToken: TestContext.Current.CancellationToken)).Success);

        List<KahunaKeyValue> deleteResults = await client.DeleteManyKeyValues(
            [existingKey, missingKey],
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(2, deleteResults.Count);
        Assert.Contains(deleteResults, result => result.Key == existingKey && result.Success);
        Assert.Contains(deleteResults, result => result.Key == missingKey && !result.Success);

        Assert.False((await client.GetKeyValue(existingKey, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken)).Success);
    }

    [Theory, CombinatorialData]
    public async Task TestDeleteManyInvalidKeyReturnsPerKeyInvalidInput(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        IKahunaCommunication communication = GetCommunicationByType(communicationType);
        string endpoint = clientType == KahunaClientType.SingleEndpoint ? url : urls[0];
        string existingKey = GetRandomKeyName();

        KahunaClient client = GetClientByType(communicationType, clientType);
        Assert.True((await client.SetKeyValue(
            existingKey,
            "value",
            10000,
            durability: KeyValueDurability.Persistent,
            cancellationToken: TestContext.Current.CancellationToken)).Success);

        (List<KahunaDeleteKeyValueResponseItem> items, int _) = await communication.TryDeleteManyKeyValues(
            endpoint,
            [
                new() { Key = "", Durability = KeyValueDurability.Persistent },
                new() { Key = existingKey, Durability = KeyValueDurability.Persistent }
            ],
            TestContext.Current.CancellationToken
        );

        Assert.Equal(2, items.Count);
        Assert.Contains(items, item => item.Key == "" && item.Type == KeyValueResponseType.InvalidInput);
        Assert.Contains(items, item => item.Key == existingKey && item.Type == KeyValueResponseType.Deleted);
    }

    [Theory, CombinatorialData]
    public async Task TestSessionDeleteManyCommit(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        string keyA = GetRandomKeyName();
        string keyB = GetRandomKeyName();

        Assert.True((await client.SetKeyValue(keyA, "a", 10000, cancellationToken: TestContext.Current.CancellationToken)).Success);
        Assert.True((await client.SetKeyValue(keyB, "b", 10000, cancellationToken: TestContext.Current.CancellationToken)).Success);

        KahunaTransactionSession session = await client.StartTransactionSession(
            new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Optimistic },
            TestContext.Current.CancellationToken
        );

        List<KahunaKeyValue> deleteResults = await session.DeleteManyKeyValues(
            [keyA, keyB],
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(2, deleteResults.Count);
        Assert.All(deleteResults, result => Assert.True(result.Success));

        Assert.True((await client.GetKeyValue(keyA, cancellationToken: TestContext.Current.CancellationToken)).Success);
        Assert.True((await client.GetKeyValue(keyB, cancellationToken: TestContext.Current.CancellationToken)).Success);

        await session.Commit(TestContext.Current.CancellationToken);

        Assert.False((await client.GetKeyValue(keyA, cancellationToken: TestContext.Current.CancellationToken)).Success);
        Assert.False((await client.GetKeyValue(keyB, cancellationToken: TestContext.Current.CancellationToken)).Success);
    }

    [Theory, CombinatorialData]
    public async Task TestSessionDeleteManyRollback(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        string keyA = GetRandomKeyName();
        string keyB = GetRandomKeyName();

        Assert.True((await client.SetKeyValue(keyA, "a", 10000, cancellationToken: TestContext.Current.CancellationToken)).Success);
        Assert.True((await client.SetKeyValue(keyB, "b", 10000, cancellationToken: TestContext.Current.CancellationToken)).Success);

        KahunaTransactionSession session = await client.StartTransactionSession(
            new KahunaTransactionOptions { Locking = KeyValueTransactionLocking.Optimistic },
            TestContext.Current.CancellationToken
        );

        List<KahunaKeyValue> deleteResults = await session.DeleteManyKeyValues(
            [keyA, keyB],
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(2, deleteResults.Count);
        Assert.All(deleteResults, result => Assert.True(result.Success));

        await session.Rollback(TestContext.Current.CancellationToken);

        Assert.True((await client.GetKeyValue(keyA, cancellationToken: TestContext.Current.CancellationToken)).Success);
        Assert.True((await client.GetKeyValue(keyB, cancellationToken: TestContext.Current.CancellationToken)).Success);
    }

    private KahunaClient GetClientByType(KahunaCommunicationType communicationType, KahunaClientType clientType)
    {
        IKahunaCommunication communication = GetCommunicationByType(communicationType);

        return clientType switch
        {
            KahunaClientType.SingleEndpoint => new KahunaClient(url, communication: communication),
            KahunaClientType.PoolOfEndpoints => new KahunaClient(urls, communication: communication),
            _ => throw new NotSupportedException($"Unsupported client type {clientType}")
        };
    }

    private static IKahunaCommunication GetCommunicationByType(KahunaCommunicationType communicationType)
    {
        return communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(new() { AllowInsecureCertificateValidation = true }, null),
            KahunaCommunicationType.Rest => new RestCommunication(null, new() { AllowInsecureCertificateValidation = true }),
            _ => throw new NotSupportedException($"Unsupported communication type {communicationType}")
        };
    }

    private static string GetRandomKeyName()
    {
        return $"test-delete-many-{Guid.NewGuid():N}";
    }
}
