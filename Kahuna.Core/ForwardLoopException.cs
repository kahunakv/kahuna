namespace Kahuna;

/// <summary>
/// Thrown when one serving flow nests more inter-node forwards than the loop-safety bound allows.
/// It reports a routing defect, never an ordinary cluster condition: the forward-hop budget already
/// makes nodes with disagreeing leadership views answer <c>MustRetry</c> rather than bounce an
/// operation between them, so a flow that nests this many forwards has found a loop the budget does
/// not cover.
///
/// <para>
/// The bound exists because the in-memory transport forwards by calling the target node in
/// process: an unguarded loop there is mutual recursion and ends the process with a stack
/// overflow, which takes down every other request and, in a test host, the whole run. Failing the
/// one looping operation with a named exception keeps the fault diagnosable and contained.
/// </para>
/// </summary>
public sealed class ForwardLoopException(int nestedForwards) : KahunaServerException(
    $"Inter-node forwarding nested {nestedForwards} levels in a single request; the forward-hop budget should have refused this chain long before")
{
    /// <summary>How many forwards the serving flow had already nested when the limit was reached.</summary>
    public int NestedForwards { get; } = nestedForwards;
}
