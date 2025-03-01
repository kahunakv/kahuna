
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

[JsonSerializable(typeof(KahunaGetLockRequest))]
[JsonSerializable(typeof(KahunaGetLockResponse))]
[JsonSerializable(typeof(KahunaLockRequest))]
[JsonSerializable(typeof(KahunaGetLockResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public sealed partial class KahunaJsonContext : JsonSerializerContext
{

}