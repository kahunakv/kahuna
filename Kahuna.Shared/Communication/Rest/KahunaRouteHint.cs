using System.Text.Json.Serialization;
using Kahuna.Shared.Routing;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// The REST form of an advisory routing hint. Present only when the answering node resolved an
/// owner; a client that does not understand it, or receives none, keeps its existing endpoint
/// selection. A hint never changes the outcome of the operation it rides on.
/// </summary>
public sealed class KahunaRouteHint
{
    /// <summary>The partition the resource resolved to. 0 when unknown.</summary>
    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    /// <summary>
    /// The client-reachable base URL the resolved node advertises, never a Raft transport address.
    /// Empty when that node advertises no client endpoint.
    /// </summary>
    [JsonPropertyName("endpoint")]
    public string Endpoint { get; set; } = "";

    [JsonPropertyName("provenance")]
    public KahunaRouteProvenance Provenance { get; set; }

    /// <summary>
    /// Committed generation of the range descriptor that admitted the resource, for key-range
    /// routed spaces. 0 for hash-routed spaces and whenever no descriptor was consulted.
    /// </summary>
    [JsonPropertyName("generation")]
    public long Generation { get; set; }
}
