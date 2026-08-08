
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Sequences;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Typed "no definitive answer was produced; retry to resolve it" responses for the sequence gRPC
/// surface. An allocation is deliberately left unset: none was produced, and the response carries
/// its outcome in the type.
/// </summary>
internal static class SequenceMustRetry
{
    private const GrpcSequenceResponseType Type = (GrpcSequenceResponseType)SequenceResponseType.MustRetry;

    public static GrpcSequenceResponse Sequence() => new() { Type = Type };

    public static GrpcSequenceAllocationResponse Allocation() => new() { Type = Type };
}
