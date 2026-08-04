using Kahuna.Shared.Sequences;

namespace Kahuna.Server.Sequencer.Data;

/// <summary>
/// Reply from the <see cref="SequenceActor"/>.
/// </summary>
internal sealed class SequenceResponse
{
    public SequenceResponseType Type { get; }

    /// <summary>Allocated range; default for every non-<c>Reserve</c> operation and every failure.</summary>
    public SequenceAllocation Allocation { get; }

    /// <summary>Record revision produced by a create; -1 otherwise.</summary>
    public long Revision { get; }

    public SequenceResponse(SequenceResponseType type, SequenceAllocation allocation = default, long revision = -1)
    {
        Type = type;
        Allocation = allocation;
        Revision = revision;
    }
}

/// <summary>
/// Shared instances for the parameterless replies, so the hot path allocates nothing beyond the
/// successful allocation itself.
/// </summary>
internal static class SequenceStaticResponses
{
    public static readonly SequenceResponse Success = new(SequenceResponseType.Success);

    public static readonly SequenceResponse NotFound = new(SequenceResponseType.NotFound);

    public static readonly SequenceResponse AlreadyExists = new(SequenceResponseType.AlreadyExists);

    public static readonly SequenceResponse InvalidInput = new(SequenceResponseType.InvalidInput);

    public static readonly SequenceResponse MaxValueExceeded = new(SequenceResponseType.MaxValueExceeded);

    public static readonly SequenceResponse MustRetry = new(SequenceResponseType.MustRetry);

    public static readonly SequenceResponse Aborted = new(SequenceResponseType.Aborted);

    public static readonly SequenceResponse Error = new(SequenceResponseType.Error);
}
