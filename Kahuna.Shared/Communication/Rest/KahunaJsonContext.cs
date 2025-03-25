
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

[JsonSerializable(typeof(KahunaGetLockRequest))]
[JsonSerializable(typeof(KahunaGetLockResponse))]
[JsonSerializable(typeof(KahunaLockRequest))]
[JsonSerializable(typeof(KahunaGetLockResponse))]
[JsonSerializable(typeof(KahunaSetKeyValueRequest))]
[JsonSerializable(typeof(KahunaGetKeyValueRequest))]
[JsonSerializable(typeof(KahunaDeleteKeyValueRequest))]
[JsonSerializable(typeof(KahunaExtendKeyValueRequest))]
[JsonSerializable(typeof(KeyValueTransactionRequest))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public sealed partial class KahunaJsonContext : JsonSerializerContext
{

}