
namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// The body of a REST call that takes no arguments. It serializes as <c>{}</c>, the same body an
/// anonymous empty object produced, but it has generated serialization metadata, so the call keeps
/// working in a trimmed build.
/// </summary>
public sealed class KahunaEmptyRequest
{
    public static readonly KahunaEmptyRequest Instance = new();
}
