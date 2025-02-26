
using System.Text.Json.Serialization;

namespace Kahuna.Client.Communication.Data;

[JsonSerializable(typeof(KahunaGetRequest))]
[JsonSerializable(typeof(KahunaLockRequest))]
[JsonSerializable(typeof(KahunaGetResponse))]
[JsonSerializable(typeof(KahunaLockResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
internal sealed partial class KahunaJsonContext : JsonSerializerContext
{

}