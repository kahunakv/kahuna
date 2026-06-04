
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
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public sealed partial class KahunaJsonContext : JsonSerializerContext
{

}
