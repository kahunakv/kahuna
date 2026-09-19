using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Functions;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Sequencer.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The JSON that Kahuna writes to disk, prints, and sends over REST resolves its types from
/// source-generated metadata, so that a trimmed build keeps working. These tests pin two properties:
/// the generated output is byte-identical to what reflection-based serialization with the same options
/// produced (existing files, segment digests and wire payloads stay compatible), and every root type
/// the REST surface uses has generated metadata (a missing type fails only in a trimmed build, so it
/// must fail here first).
/// </summary>
public sealed class TestTrimSafeJson
{
    private static readonly JsonSerializerOptions ReflectionDefaults = new()
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver()
    };

    private static readonly JsonSerializerOptions ReflectionIndented = new()
    {
        WriteIndented = true,
        TypeInfoResolver = new DefaultJsonTypeInfoResolver()
    };

    [Fact]
    public void WalSegmentEntry_GeneratedOutputMatchesReflection()
    {
        WalSegmentEntry entry = new()
        {
            Id = 42,
            Term = 3,
            TimeNode = 7,
            TimePhysical = 1_789_000_000_000,
            TimeCounter = 5,
            LogType = "kv",
            LogData = [1, 2, 3, 250]
        };

        string generated = JsonSerializer.Serialize(entry, PitrJsonContext.Default.WalSegmentEntry);

        Assert.Equal(JsonSerializer.Serialize(entry, ReflectionDefaults), generated);

        WalSegmentEntry restored = JsonSerializer.Deserialize(generated, PitrJsonContext.Default.WalSegmentEntry)!;
        Assert.Equal(entry.Id, restored.Id);
        Assert.Equal(entry.Time, restored.Time);
        Assert.Equal(entry.LogType, restored.LogType);
        Assert.Equal(entry.LogData, restored.LogData);
    }

    [Fact]
    public void WalSegmentEntry_NullLogDataStaysNull()
    {
        WalSegmentEntry entry = new() { Id = 1, Term = 1, LogType = "kv", LogData = null };

        string generated = JsonSerializer.Serialize(entry, PitrJsonContext.Default.WalSegmentEntry);

        Assert.Equal(JsonSerializer.Serialize(entry, ReflectionDefaults), generated);
        Assert.Null(JsonSerializer.Deserialize(generated, PitrJsonContext.Default.WalSegmentEntry)!.LogData);
    }

    [Fact]
    public void CheckpointManifest_GeneratedOutputMatchesReflection()
    {
        CheckpointManifest manifest = CheckpointManifest.From(99, new HLCTimestamp(4, 1_789_000_000_123, 2));

        string generated = JsonSerializer.Serialize(manifest, PitrJsonContext.Default.CheckpointManifest);

        Assert.Equal(JsonSerializer.Serialize(manifest, ReflectionDefaults), generated);
        Assert.Equal(manifest, JsonSerializer.Deserialize(generated, PitrJsonContext.Default.CheckpointManifest));
    }

    [Fact]
    public void BackupManifest_GeneratedOutputMatchesReflection()
    {
        BackupManifest manifest = BackupManifest.CreateIncremental(Guid.NewGuid(),
        [
            new PartitionBackupRange { PartitionId = 1, FromIndex = 1, ToIndex = 10, ToTerm = 2 }
        ]);
        manifest.SetBaseCut(new HLCTimestamp(1, 2, 3));
        manifest.Checksums["checkpoint/000001.sst"] = "abc";
        manifest.Sizes["checkpoint/000001.sst"] = 1024;
        manifest.ClusterPartitions = [1, 2];

        string generated = JsonSerializer.Serialize(manifest, PitrIndentedJsonContext.Default.BackupManifest);

        Assert.Equal(JsonSerializer.Serialize(manifest, ReflectionIndented), generated);

        BackupManifest restored = JsonSerializer.Deserialize(generated, PitrIndentedJsonContext.Default.BackupManifest)!;
        Assert.Equal(manifest.BackupId, restored.BackupId);
        Assert.Equal(manifest.ParentBackupId, restored.ParentBackupId);
        Assert.Equal(manifest.BaseCut, restored.BaseCut);
        Assert.Equal(10, restored.PartitionRanges[0].ToIndex);
        Assert.Equal(1024, restored.Sizes["checkpoint/000001.sst"]);
    }

    [Fact]
    public void MemoryCheckpointEntries_GeneratedOutputMatchesReflection()
    {
        List<MemoryPersistenceBackend.MemoryCheckpointEntry> kv =
        [
            new() { Key = "k", Value = [9, 8], Revision = 3, LastModifiedPhysical = 11, State = 1 },
            new() { Key = "no-value", Value = null, Revision = 4 },
            new() { Key = "empty-value", Value = [], Revision = 5 }
        ];
        List<MemoryPersistenceBackend.MemoryCheckpointLockEntry> locks =
        [
            new() { Resource = "r", Owner = [1], FencingToken = 5, ExpiresPhysical = 12 },
            new() { Resource = "no-owner", Owner = null }
        ];

        Assert.Equal(JsonSerializer.Serialize(kv, ReflectionDefaults),
            JsonSerializer.Serialize(kv, PitrJsonContext.Default.ListMemoryCheckpointEntry));
        Assert.Equal(JsonSerializer.Serialize(locks, ReflectionDefaults),
            JsonSerializer.Serialize(locks, PitrJsonContext.Default.ListMemoryCheckpointLockEntry));

        List<MemoryPersistenceBackend.MemoryCheckpointEntry> restored = JsonSerializer.Deserialize(
            JsonSerializer.Serialize(kv, PitrJsonContext.Default.ListMemoryCheckpointEntry),
            PitrJsonContext.Default.ListMemoryCheckpointEntry)!;
        Assert.Null(restored[1].Value);
        Assert.NotNull(restored[2].Value);
        Assert.Empty(restored[2].Value!);
    }

    [Fact]
    public void ToJsonArray_GeneratedOutputMatchesReflection()
    {
        List<KeyValueExpressionResult> array =
        [
            new(5L),
            new(2.5),
            new("s\"q"),
            new(true),
            new([new KeyValueExpressionResult(1L)])
        ];

        Assert.Equal(JsonSerializer.Serialize(array, ReflectionDefaults),
            JsonSerializer.Serialize(array, ToJsonContext.Default.ListKeyValueExpressionResult));
        Assert.Equal(JsonSerializer.Serialize("s\"q", ReflectionDefaults),
            JsonSerializer.Serialize("s\"q", ToJsonContext.Default.String));
        Assert.Equal(JsonSerializer.Serialize(2.5, ReflectionDefaults),
            JsonSerializer.Serialize(2.5, ToJsonContext.Default.Double));
    }

    [Fact]
    public void LegacyJsonSequenceRecord_ReadsWithWebDefaults()
    {
        // The original record format: camel-case names, and a number sent as a string, which only the
        // web defaults accept.
        byte[] legacy = Encoding.UTF8.GetBytes(
            """
            {"name":"seq","currentValue":"17","initialValue":1,"increment":2,"maxValue":null,
             "createdAt":{"n":1,"l":100,"c":0},"updatedAt":{"n":1,"l":200,"c":3},
             "idempotency":{"key-1":{"name":"seq","start":5,"end":6,"count":2,"revision":4}}}
            """);

        SequenceState state = SequenceStateCodec.Deserialize(legacy)!;

        Assert.Equal("seq", state.Name);
        Assert.Equal(17, state.CurrentValue);
        Assert.Equal(2, state.Increment);
        Assert.Equal(new HLCTimestamp(1, 200, 3), state.UpdatedAt);
        Assert.Equal(5, state.Idempotency["key-1"].Allocation.Start);
    }

    [Fact]
    public void RestFlurlOptions_MatchFlurlDefaultWireFormat()
    {
        // Flurl's default serializer uses no naming policy and case-insensitive reads. A property with
        // no explicit JSON name must keep its .NET name on the wire.
        JsonSerializerOptions flurlDefaults = new()
        {
            PropertyNameCaseInsensitive = true,
            TypeInfoResolver = new DefaultJsonTypeInfoResolver()
        };

        KahunaBackupIncrementalRequest request = new() { ParentBackupId = Guid.NewGuid() };

        string generated = KahunaRestJson.FlurlSerializer.Serialize(request);

        Assert.Equal(JsonSerializer.Serialize(request, flurlDefaults), generated);
        Assert.Contains("\"ParentBackupId\"", generated);
        Assert.Equal("{}", KahunaRestJson.FlurlSerializer.Serialize(KahunaEmptyRequest.Instance));

        KahunaTxKeyValueResponse response = KahunaRestJson.FlurlSerializer.Deserialize<KahunaTxKeyValueResponse>(
            """{"type":3,"reason":"r"}""")!;
        Assert.Equal(KeyValueResponseType.Get, response.Type);
    }

    public static TheoryData<Type> RestRootTypes() =>
    [
        typeof(KahunaSetKeyValueRequest), typeof(KahunaSetKeyValueResponse),
        typeof(KahunaExtendKeyValueRequest), typeof(KahunaExtendKeyValueResponse),
        typeof(KahunaDeleteKeyValueRequest), typeof(KahunaDeleteKeyValueResponse),
        typeof(KahunaGetKeyValueRequest), typeof(KahunaGetKeyValueResponse),
        typeof(KahunaExistsKeyValueRequest), typeof(KahunaExistsKeyValueResponse),
        typeof(KahunaSetManyKeyValueRequest), typeof(KahunaSetManyKeyValueResponse),
        typeof(KahunaDeleteManyKeyValueRequest), typeof(KahunaDeleteManyKeyValueResponse),
        typeof(KahunaManyKeyValuesRequest), typeof(KahunaManyKeyValuesResponse),
        typeof(KahunaGetByRangeRequest), typeof(KahunaGetByRangeResponse),
        typeof(KahunaGetByBucketRequest), typeof(KahunaGetByBucketResponse),
        typeof(KahunaScanAllByPrefixRequest),
        typeof(KahunaTxKeyValueRequest), typeof(KahunaTxKeyValueResponse), typeof(KeyValueTransactionResponse),
        typeof(KahunaAcquireKeyValueLockRequest), typeof(KahunaReleaseKeyValueLockRequest), typeof(KahunaKeyValueLockResponse),
        typeof(KahunaAcquireRangeLockRequest), typeof(KahunaReleaseRangeLockRequest),
        typeof(KahunaStartTransactionRequest), typeof(KahunaStartTransactionResponse),
        typeof(KahunaCommitTransactionRequest), typeof(KahunaCommitTransactionResponse),
        typeof(KahunaLockRequest), typeof(KahunaLockResponse), typeof(KahunaGetLockRequest), typeof(KahunaGetLockResponse),
        typeof(KahunaSequenceCreateRequest), typeof(KahunaSequenceUpdateRequest), typeof(KahunaSequenceNameRequest),
        typeof(KahunaSequenceNextRequest), typeof(KahunaSequenceReserveRequest), typeof(KahunaSequenceResponse),
        typeof(KahunaBackupInfo), typeof(List<KahunaBackupInfo>), typeof(IReadOnlyList<KahunaBackupInfo>),
        typeof(KahunaBackupIncrementalRequest), typeof(KahunaBackupRestoreRequest), typeof(KahunaRestoreResponse),
        typeof(KahunaBackupGcResult), typeof(KahunaEmptyRequest),
        typeof(KahunaAcquireSnapshotHoldRequest), typeof(KahunaAcquireSnapshotHoldResponse),
        typeof(KahunaRenewSnapshotHoldRequest), typeof(KahunaRenewSnapshotHoldResponse),
        typeof(KahunaReleaseSnapshotHoldRequest), typeof(KahunaReleaseSnapshotHoldResponse),
        typeof(KahunaGetSnapshotFloorResponse),
        typeof(KahunaKeyRangeRequest), typeof(KahunaRegisterKeyRangeResponse), typeof(KahunaRemoveKeyRangeResponse),
        typeof(KahunaSplitRangeRequest), typeof(KahunaSplitRangeResponse), typeof(KahunaMergeRangesResponse),
        typeof(KahunaRangeMapResponse), typeof(KahunaRoutingMetadataResponse),
        typeof(KahunaClusterMembershipResponse), typeof(KahunaClusterHealthResponse), typeof(KahunaClusterLeaveResponse),
        typeof(KahunaClusterPlacementResponse), typeof(KahunaSetReplicationFactorRequest), typeof(KahunaSetReplicationFactorResponse),
        typeof(KahunaDashboardSummaryResponse), typeof(KahunaDashboardMetricsResponse)
    ];

    [Theory]
    [MemberData(nameof(RestRootTypes))]
    public void RestRootType_HasGeneratedMetadata(Type type)
    {
        Assert.NotNull(KahunaJsonContext.Default.GetTypeInfo(type));
    }
}
