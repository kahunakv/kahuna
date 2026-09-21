namespace Kahuna.Shared.Communication.Grpc;

/// <summary>
/// The contract both ends of the client key-value batch stream share for multi-request frames.
///
/// <para>A frame is one stream message that carries several independent requests, or several independent
/// responses. It exists because a stream message has a fixed cost on each side (a gRPC message frame, an
/// HTTP/2 frame, the pipe locks, the thread hops), and under load that fixed cost is most of what a small
/// request costs.</para>
///
/// <para>A peer built before frames existed does not answer a batch type it does not know, so a frame sent
/// to it would leave every request inside unanswered until its deadline. Support is therefore announced,
/// never probed: a node that reads frames says so in <see cref="SupportHeader"/> on the response headers of
/// the stream, and a client sends a frame only after it saw that header on that same stream. The other
/// direction is gated the same way: a node sends response frames only on a stream that already carried a
/// request frame, which is the proof that the client can read them. Both decisions are per stream, so a
/// client that talks to nodes of mixed versions uses frames with exactly the nodes that announced them.</para>
/// </summary>
public static class ClientBatchFrames
{
    /// <summary>Response header a node sets on the batch stream when it reads request frames.</summary>
    public const string SupportHeader = "kahuna-batch-frames";

    /// <summary>The frame contract version this build speaks, as the value of <see cref="SupportHeader"/>.</summary>
    public const string SupportVersion = "1";

    /// <summary>
    /// Most items one frame may carry. Bounds the work a single stream message can start on the node, and
    /// the latency the first item of a frame pays while the rest are packed.
    /// </summary>
    public const int MaxItems = 256;

    /// <summary>
    /// Most serialized bytes the items of one frame may add up to. Far below the 4 MB default gRPC message
    /// limit, so a frame built inside this budget can never be the message that a peer's transport rejects.
    /// An item that is larger than this on its own travels as an ordinary single message.
    /// </summary>
    public const int MaxBytes = 1024 * 1024;
}
