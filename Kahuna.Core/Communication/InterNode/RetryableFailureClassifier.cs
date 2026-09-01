
using Grpc.Core;
using Kommander;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Decides whether an exception means "no definitive answer was produced, so retrying is how the
/// outcome gets resolved". Every public surface — REST and gRPC alike — answers such a failure with
/// its own typed MustRetry response instead of letting it escape as an unclassifiable server error,
/// so all of them must agree on what counts as retryable. This is the single implementation they
/// share; a second copy would drift and reintroduce the escape on whichever surface fell behind.
/// </summary>
public static class RetryableFailureClassifier
{
    /// <summary>
    /// Retryable = any Raft resolution failure (missing partition, undecided leader, node not
    /// initialized), an inter-node transport failure — the remote unreachable or no longer
    /// answering, including a dead pooled HTTP/2 connection that reports StatusCode.Internal with a
    /// transport-layer cause — or a forward chain that hit its loop-safety ceiling, which means the
    /// nodes it visited hold disagreeing views and a retry against a settled one is the resolution.
    /// Cancellation is deliberately not mapped — the client gave up, there is nobody to answer.
    ///
    /// <para>Wrappers are unwrapped before classifying: a retryable failure that arrives inside an
    /// <see cref="AggregateException"/> or as another exception's <c>InnerException</c> is still
    /// retryable — it is the same "no definitive answer" condition wearing a different shell.</para>
    /// </summary>
    public static bool IsRetryable(Exception ex)
    {
        switch (ex)
        {
            case RaftException:
                return true;

            case ForwardLoopException:
                return true;

            case RpcException rpc:
                return InterNodeTransportFailure.IsRetryable(rpc);

            case AggregateException aggregate:
                foreach (Exception inner in aggregate.InnerExceptions)
                {
                    if (IsRetryable(inner))
                        return true;
                }

                return false;

            default:
                return ex.InnerException is { } wrapped && IsRetryable(wrapped);
        }
    }
}
