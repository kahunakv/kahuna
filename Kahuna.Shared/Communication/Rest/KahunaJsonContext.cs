
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

[JsonSerializable(typeof(KahunaGetLockRequest))]
[JsonSerializable(typeof(KahunaGetLockResponse))]
[JsonSerializable(typeof(KahunaLockRequest))]
[JsonSerializable(typeof(KahunaExistsKeyValueRequest))]
[JsonSerializable(typeof(KahunaGetLockResponse))]
[JsonSerializable(typeof(KahunaSetKeyValueRequest))]
[JsonSerializable(typeof(KahunaGetKeyValueRequest))]
[JsonSerializable(typeof(KahunaDeleteKeyValueRequest))]
[JsonSerializable(typeof(KahunaSetManyKeyValueRequest))]
[JsonSerializable(typeof(KahunaSetManyKeyValueResponse))]
[JsonSerializable(typeof(KahunaDeleteManyKeyValueRequest))]
[JsonSerializable(typeof(KahunaDeleteManyKeyValueResponse))]
[JsonSerializable(typeof(KahunaExtendKeyValueRequest))]
[JsonSerializable(typeof(KahunaDeleteKeyValueRequestItem))]
[JsonSerializable(typeof(KahunaDeleteKeyValueResponseItem))]
[JsonSerializable(typeof(KeyValueTransactionRequest))]
[JsonSerializable(typeof(KahunaSequenceCreateRequest))]
[JsonSerializable(typeof(KahunaSequenceNameRequest))]
[JsonSerializable(typeof(KahunaSequenceNextRequest))]
[JsonSerializable(typeof(KahunaSequenceReserveRequest))]
[JsonSerializable(typeof(KahunaSequenceResponse))]
[JsonSerializable(typeof(ReadOnlySequenceEntry))]
[JsonSerializable(typeof(SequenceAllocation))]
[JsonSerializable(typeof(KahunaClusterMembershipResponse))]
[JsonSerializable(typeof(KahunaClusterMemberResponse))]
[JsonSerializable(typeof(List<KahunaClusterMemberResponse>))]
[JsonSerializable(typeof(KahunaBackupInfo))]
[JsonSerializable(typeof(List<KahunaBackupInfo>))]
[JsonSerializable(typeof(KahunaBackupIncrementalRequest))]
[JsonSerializable(typeof(KahunaBackupRestoreRequest))]
[JsonSerializable(typeof(KahunaRestoreResponse))]
[JsonSerializable(typeof(KahunaAcquireSnapshotHoldRequest))]
[JsonSerializable(typeof(KahunaAcquireSnapshotHoldResponse))]
[JsonSerializable(typeof(KahunaRenewSnapshotHoldRequest))]
[JsonSerializable(typeof(KahunaRenewSnapshotHoldResponse))]
[JsonSerializable(typeof(KahunaReleaseSnapshotHoldRequest))]
[JsonSerializable(typeof(KahunaReleaseSnapshotHoldResponse))]
[JsonSerializable(typeof(KahunaGetSnapshotFloorResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public sealed partial class KahunaJsonContext : JsonSerializerContext
{

}
