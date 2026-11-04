
namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// The persistence backend found its storage damaged: the engine reported a <c>Corruption</c> status (a
/// block or file whose checksum did not match what was written, or a file shorter than its manifest
/// records), or one of the backend's own bookkeeping rows could not be decoded. Nothing read from a damaged
/// store is ever served. The backend fail-stops on the first such finding and answers every later
/// operation with this exception until the process is restarted over a repaired or replaced store, so a
/// caller must classify it as terminal — the non-retryable <c>StorageCorrupt</c> response — never as a
/// transient fault to retry.
/// </summary>
public sealed class StorageCorruptionException : Exception
{
    /// <summary>The store directory the damage was found in.</summary>
    public string StorePath { get; }

    public StorageCorruptionException(string storePath, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        StorePath = storePath;
    }

    /// <summary>
    /// The <see cref="StorageCorruptionException"/> inside <paramref name="exception"/>, if it is one or
    /// wraps one (an <see cref="AggregateException"/> from a faulted task, or an inner exception), else null.
    /// </summary>
    public static StorageCorruptionException? Find(Exception? exception)
    {
        switch (exception)
        {
            case null:
                return null;

            case StorageCorruptionException corruption:
                return corruption;

            case AggregateException aggregate:
                foreach (Exception inner in aggregate.InnerExceptions)
                {
                    if (Find(inner) is { } found)
                        return found;
                }

                return null;

            default:
                return Find(exception.InnerException);
        }
    }
}
