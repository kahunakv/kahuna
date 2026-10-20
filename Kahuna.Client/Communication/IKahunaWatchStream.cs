using Kahuna.Shared.KeyValue;

namespace Kahuna.Client.Communication;

/// <summary>
/// One watch open on one node, as a transport delivers it. <see cref="ReadAllAsync"/> yields the frames after
/// the opening, in the order the node sent them, and stops after a terminal frame. When the connection ends
/// without a terminal frame it yields one <see cref="KeyValueWatchFrameType.Lagged"/> frame with
/// <see cref="KeyValueWatchLagReason.Disconnected"/>, because events can be missing from that point. After
/// <see cref="IAsyncDisposable.DisposeAsync"/> it ends without a frame.
/// </summary>
public interface IKahunaWatchStream : IAsyncDisposable
{
    /// <summary>Reads the watch's frames. Only one reader may read a stream.</summary>
    IAsyncEnumerable<KeyValueWatchFrame> ReadAllAsync(CancellationToken cancellationToken);
}

/// <summary>
/// A node's answer to a watch request. <see cref="Type"/> is <see cref="KeyValueResponseType.Get"/> with an open
/// <see cref="Stream"/> registered on <see cref="Partitions"/>, or a refusal with no stream. A
/// <see cref="KeyValueResponseType.DoesNotExist"/> refusal carries <see cref="Routes"/>: the replicas of each
/// partition the request covers.
/// </summary>
public sealed record KahunaWatchOpening(
    KeyValueResponseType Type,
    IKahunaWatchStream? Stream,
    IReadOnlyList<int> Partitions,
    IReadOnlyList<KeyValueWatchRoute> Routes)
{
    public static KahunaWatchOpening Refused(KeyValueResponseType type, IReadOnlyList<KeyValueWatchRoute>? routes = null) =>
        new(type, null, [], routes ?? []);

    /// <summary>The opening that the first frame of a watch announces.</summary>
    internal static KahunaWatchOpening FromAnswer(KeyValueWatchFrame answer, IKahunaWatchStream stream) =>
        answer.Type == KeyValueWatchFrameType.Opened
            ? new(KeyValueResponseType.Get, stream, answer.Partitions, [])
            : Refused(answer.Type == KeyValueWatchFrameType.Refused ? answer.Refusal : KeyValueResponseType.Errored, answer.Routes);
}
