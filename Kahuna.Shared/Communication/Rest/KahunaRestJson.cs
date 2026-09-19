using System.Text.Json;
using Flurl.Http;
using Flurl.Http.Configuration;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Serialization settings for the REST calls Kahuna makes through Flurl.
/// <para>
/// Flurl's own default serializer resolves types by reflection, which a trimmed build cannot rely
/// on. These options resolve every REST type through <see cref="KahunaJsonContext"/> instead. They
/// otherwise match Flurl's defaults exactly (no naming policy, case-insensitive property matching),
/// so the bytes on the wire do not change: a property without an explicit JSON name is still sent
/// with its .NET name, which the server accepts because it matches names case-insensitively.
/// </para>
/// </summary>
public static class KahunaRestJson
{
    public static readonly JsonSerializerOptions FlurlOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        TypeInfoResolver = KahunaJsonContext.Default
    };

    public static readonly ISerializer FlurlSerializer = new DefaultJsonSerializer(FlurlOptions);

    /// <summary>
    /// Per-request settings for every Kahuna REST call: HTTP/2 and the generated-metadata serializer.
    /// </summary>
    public static readonly Action<FlurlHttpSettings> Http2Settings = settings =>
    {
        settings.HttpVersion = "2.0";
        settings.JsonSerializer = FlurlSerializer;
    };
}
