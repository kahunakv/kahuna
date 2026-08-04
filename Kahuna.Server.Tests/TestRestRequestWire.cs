using System.Text.Json;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transactional operation sent over the REST transport must carry its coordinator registration
/// identity (coordinator key + operation id); a DTO that drops it silently routes the operation down
/// the unregistered path, and the commit later decides with a working set that never saw the write.
/// These tests round-trip each registered-operation request through the source-generated JSON context
/// so a field missing from the DTO or the serializer context fails loudly.
/// </summary>
public sealed class TestRestRequestWire
{
    private const string Coordinator = "coord-key";
    private const ulong High = 7, Low = 9;

    private static T RoundTrip<T>(T request, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo)
    {
        string json = JsonSerializer.Serialize(request, typeInfo);
        return JsonSerializer.Deserialize(json, typeInfo)!;
    }

    [Fact]
    public void SetRequest_CarriesRegistrationIdentity()
    {
        KahunaSetKeyValueRequest restored = RoundTrip(new KahunaSetKeyValueRequest
        {
            TransactionId = new HLCTimestamp(1, 100, 2),
            Key = "k",
            Value = [1, 2],
            Durability = KeyValueDurability.Persistent,
            CoordinatorKey = Coordinator,
            OperationIdHigh = High,
            OperationIdLow = Low
        }, KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        Assert.Equal(Coordinator, restored.CoordinatorKey);
        Assert.Equal(High, restored.OperationIdHigh);
        Assert.Equal(Low, restored.OperationIdLow);
    }

    [Fact]
    public void DeleteRequest_CarriesRegistrationIdentity()
    {
        KahunaDeleteKeyValueRequest restored = RoundTrip(new KahunaDeleteKeyValueRequest
        {
            TransactionId = new HLCTimestamp(1, 100, 2),
            Key = "k",
            Durability = KeyValueDurability.Persistent,
            CoordinatorKey = Coordinator,
            OperationIdHigh = High,
            OperationIdLow = Low
        }, KahunaJsonContext.Default.KahunaDeleteKeyValueRequest);

        Assert.Equal(Coordinator, restored.CoordinatorKey);
        Assert.Equal(High, restored.OperationIdHigh);
        Assert.Equal(Low, restored.OperationIdLow);
    }

    [Fact]
    public void ExtendRequest_CarriesRegistrationIdentity()
    {
        KahunaExtendKeyValueRequest restored = RoundTrip(new KahunaExtendKeyValueRequest
        {
            TransactionId = new HLCTimestamp(1, 100, 2),
            Key = "k",
            ExpiresMs = 1000,
            Durability = KeyValueDurability.Persistent,
            CoordinatorKey = Coordinator,
            OperationIdHigh = High,
            OperationIdLow = Low
        }, KahunaJsonContext.Default.KahunaExtendKeyValueRequest);

        Assert.Equal(Coordinator, restored.CoordinatorKey);
        Assert.Equal(High, restored.OperationIdHigh);
        Assert.Equal(Low, restored.OperationIdLow);
    }

    [Fact]
    public void GetRequest_CarriesRegistrationIdentity()
    {
        KahunaGetKeyValueRequest restored = RoundTrip(new KahunaGetKeyValueRequest
        {
            TransactionId = new HLCTimestamp(1, 100, 2),
            Key = "k",
            Durability = KeyValueDurability.Persistent,
            CoordinatorKey = Coordinator,
            OperationIdHigh = High,
            OperationIdLow = Low
        }, KahunaJsonContext.Default.KahunaGetKeyValueRequest);

        Assert.Equal(Coordinator, restored.CoordinatorKey);
        Assert.Equal(High, restored.OperationIdHigh);
        Assert.Equal(Low, restored.OperationIdLow);
    }

    [Fact]
    public void ExistsRequest_CarriesRegistrationIdentity()
    {
        KahunaExistsKeyValueRequest restored = RoundTrip(new KahunaExistsKeyValueRequest
        {
            TransactionId = new HLCTimestamp(1, 100, 2),
            Key = "k",
            Durability = KeyValueDurability.Persistent,
            CoordinatorKey = Coordinator,
            OperationIdHigh = High,
            OperationIdLow = Low
        }, KahunaJsonContext.Default.KahunaExistsKeyValueRequest);

        Assert.Equal(Coordinator, restored.CoordinatorKey);
        Assert.Equal(High, restored.OperationIdHigh);
        Assert.Equal(Low, restored.OperationIdLow);
    }

    [Fact]
    public void DeleteManyRequest_CarriesBatchRegistrationIdentity()
    {
        KahunaDeleteManyKeyValueRequest restored = RoundTrip(new KahunaDeleteManyKeyValueRequest
        {
            Items = [new() { Key = "k", Durability = KeyValueDurability.Persistent }],
            CoordinatorKey = Coordinator,
            OperationIdHigh = High,
            OperationIdLow = Low
        }, KahunaJsonContext.Default.KahunaDeleteManyKeyValueRequest);

        Assert.Equal(Coordinator, restored.CoordinatorKey);
        Assert.Equal(High, restored.OperationIdHigh);
        Assert.Equal(Low, restored.OperationIdLow);
        Assert.NotNull(restored.Items);
        Assert.Single(restored.Items);
    }
}
