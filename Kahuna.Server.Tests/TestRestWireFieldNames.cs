using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A REST field whose JSON name does not match what callers send is not a naming nit: an
/// unrecognized field deserializes to the type default instead of failing, so the operation runs
/// with a silently wrong argument. Three names had drifted from the value they carry — durability
/// travelled as "value" on the read/delete/extend requests, the lock owner as "lockId", and the
/// entry expiration as "timestamp" — and each drift is invisible until a caller that does not share
/// these DTOs talks to the server. These tests pin the name of every field that has drifted, in the
/// serializer that actually carries it: the source-generated context for requests the client sends,
/// and web-default options for the responses ASP.NET writes.
/// </summary>
public sealed class TestRestWireFieldNames
{
    private static readonly JsonSerializerOptions ResponseOptions = new(JsonSerializerDefaults.Web);
    private static readonly JsonSerializerOptions NoNamingPolicyOptions = new();

    private static void AssertNamedProperty(string json, string expected, string absent, string owner)
    {
        using JsonDocument document = JsonDocument.Parse(json);

        Assert.True(
            document.RootElement.TryGetProperty(expected, out _),
            $"{owner} must write \"{expected}\": {json}"
        );

        Assert.False(
            document.RootElement.TryGetProperty(absent, out _),
            $"{owner} must no longer write \"{absent}\": {json}"
        );
    }

    private static void AssertDurabilityWireContract<T>(
        Func<KeyValueDurability, T> create,
        Func<T, KeyValueDurability> durabilityOf,
        JsonTypeInfo<T> typeInfo
    )
    {
        // Persistent is the non-default value, so every assertion below fails if the field is dropped.
        string json = JsonSerializer.Serialize(create(KeyValueDurability.Persistent), typeInfo);

        // On a request that carries no payload, a field named "value" can only be the old misnomer.
        AssertNamedProperty(json, "durability", "value", typeof(T).Name);

        using (JsonDocument document = JsonDocument.Parse(json))
            Assert.Equal((int)KeyValueDurability.Persistent, document.RootElement.GetProperty("durability").GetInt32());

        Assert.Equal(KeyValueDurability.Persistent, durabilityOf(JsonSerializer.Deserialize(json, typeInfo)!));

        Assert.Equal(
            KeyValueDurability.Persistent,
            durabilityOf(JsonSerializer.Deserialize("""{"key":"k","durability":1}""", typeInfo)!)
        );

        // An explicit Ephemeral must survive the round trip, not merely coincide with the default.
        Assert.Equal(
            KeyValueDurability.Ephemeral,
            durabilityOf(JsonSerializer.Deserialize(
                JsonSerializer.Serialize(create(KeyValueDurability.Ephemeral), typeInfo), typeInfo)!)
        );

        // A request that never mentions durability keeps the documented default.
        Assert.Equal(
            KeyValueDurability.Ephemeral,
            durabilityOf(JsonSerializer.Deserialize("""{"key":"k"}""", typeInfo)!)
        );
    }

    [Fact]
    public void GetRequest_NamesDurabilityOnTheWire()
    {
        AssertDurabilityWireContract(
            durability => new KahunaGetKeyValueRequest { Key = "k", Revision = -1, Durability = durability },
            request => request.Durability,
            KahunaJsonContext.Default.KahunaGetKeyValueRequest
        );
    }

    [Fact]
    public void ExistsRequest_NamesDurabilityOnTheWire()
    {
        AssertDurabilityWireContract(
            durability => new KahunaExistsKeyValueRequest { Key = "k", Revision = -1, Durability = durability },
            request => request.Durability,
            KahunaJsonContext.Default.KahunaExistsKeyValueRequest
        );
    }

    [Fact]
    public void DeleteRequest_NamesDurabilityOnTheWire()
    {
        AssertDurabilityWireContract(
            durability => new KahunaDeleteKeyValueRequest { Key = "k", Durability = durability },
            request => request.Durability,
            KahunaJsonContext.Default.KahunaDeleteKeyValueRequest
        );
    }

    [Fact]
    public void ExtendRequest_NamesDurabilityOnTheWire()
    {
        AssertDurabilityWireContract(
            durability => new KahunaExtendKeyValueRequest { Key = "k", ExpiresMs = 1000, Durability = durability },
            request => request.Durability,
            KahunaJsonContext.Default.KahunaExtendKeyValueRequest
        );
    }

    /// <summary>
    /// The lock owner decides who holds the lease, and a request whose owner binds to null asks to
    /// lock, extend, or release on behalf of nobody. The get-info response has always reported it as
    /// "owner", so the request reads it under that name too.
    /// </summary>
    [Fact]
    public void LockRequest_NamesOwnerOnTheWire()
    {
        byte[] owner = [7, 8, 9];

        string json = JsonSerializer.Serialize(new KahunaLockRequest
        {
            Resource = "locks/resource",
            Owner = owner,
            ExpiresMs = 10000,
            Durability = LockDurability.Persistent
        }, KahunaJsonContext.Default.KahunaLockRequest);

        AssertNamedProperty(json, "owner", "lockId", nameof(KahunaLockRequest));

        KahunaLockRequest restored = JsonSerializer.Deserialize(json, KahunaJsonContext.Default.KahunaLockRequest)!;

        Assert.Equal(owner, restored.Owner);
        Assert.Equal("locks/resource", restored.Resource);

        // The name a caller building the request by hand would reach for must be the one that binds.
        KahunaLockRequest handWritten = JsonSerializer.Deserialize(
            $$"""{"resource":"locks/resource","owner":"{{Convert.ToBase64String(owner)}}","expiresMs":10000,"durability":1}""",
            KahunaJsonContext.Default.KahunaLockRequest)!;

        Assert.Equal(owner, handWritten.Owner);
        Assert.Equal(LockDurability.Persistent, handWritten.Durability);
    }

    /// <summary>
    /// The read responses carry two distinct HLCs — when the entry expires and when it was last
    /// written — and naming the first one "timestamp" left a caller no way to tell which was which.
    /// </summary>
    [Fact]
    public void GetResponse_NamesExpiresOnTheWire()
    {
        HLCTimestamp expires = new(1, 500, 3);
        HLCTimestamp lastModified = new(1, 400, 2);

        string json = JsonSerializer.Serialize(new KahunaGetKeyValueResponse
        {
            Type = KeyValueResponseType.Get,
            Value = [1, 2],
            Revision = 4,
            Expires = expires,
            LastModified = lastModified
        }, ResponseOptions);

        AssertNamedProperty(json, "expires", "timestamp", nameof(KahunaGetKeyValueResponse));

        KahunaGetKeyValueResponse restored = JsonSerializer.Deserialize<KahunaGetKeyValueResponse>(json, ResponseOptions)!;

        Assert.Equal(expires, restored.Expires);
        Assert.Equal(lastModified, restored.LastModified);
    }

    /// <summary>
    /// These three response types carry no naming policy of their own beyond their attributes, and
    /// once carried none at all — their wire names came from whichever policy the serializer that
    /// touched them happened to have, agreeing only by convention across the ASP.NET writer and the
    /// client's reader. Serializing them through policy-free options proves the names are now a
    /// property of the type: a caller wiring up different options can no longer silently rename the
    /// lock owner or a backup id and hand the far side an object of nulls and zeroes.
    /// </summary>
    [Theory]
    [InlineData(typeof(KahunaGetLockResponse), "servedFrom,type,owner,expires,fencingToken")]
    [InlineData(typeof(KahunaRestoreResponse), "targetDir,partitionsRestored,entriesApplied,lastAppliedPhysicalMs,"
        + "chain,outcome,minRecoverablePhysicalMs,maxRecoverablePhysicalMs")]
    [InlineData(typeof(KahunaBackupInfo), "backupId,formatVersion,type,createdAtUtc,parentBackupId,partitionCount,"
        + "clusterId,coordinatorNode,clusterSnapshotNode,clusterSnapshotPhysical,clusterSnapshotCounter,"
        + "requestedKind,actualKind,substitutionReason,isInvalid,isIncomplete,invalidReason,"
        + "minRecoverablePhysicalMs,maxRecoverablePhysicalMs")]
    public void ResponsesPinTheirWireNamesWithoutANamingPolicy(Type type, string expected)
    {
        // No camelCase policy: whatever survives here is spelled out on the type itself.
        string json = JsonSerializer.Serialize(Activator.CreateInstance(type), type, NoNamingPolicyOptions);

        using JsonDocument document = JsonDocument.Parse(json);

        Assert.Equal(expected, string.Join(",", document.RootElement.EnumerateObject().Select(p => p.Name)));
    }

    [Fact]
    public void ExistsResponse_NamesExpiresOnTheWire()
    {
        HLCTimestamp expires = new(1, 500, 3);
        HLCTimestamp lastModified = new(1, 400, 2);

        string json = JsonSerializer.Serialize(new KahunaExistsKeyValueResponse
        {
            Type = KeyValueResponseType.Exists,
            Revision = 4,
            Expires = expires,
            LastModified = lastModified
        }, ResponseOptions);

        AssertNamedProperty(json, "expires", "timestamp", nameof(KahunaExistsKeyValueResponse));

        KahunaExistsKeyValueResponse restored = JsonSerializer.Deserialize<KahunaExistsKeyValueResponse>(json, ResponseOptions)!;

        Assert.Equal(expires, restored.Expires);
        Assert.Equal(lastModified, restored.LastModified);
    }
}
