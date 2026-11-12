
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Represents the set of flags used to determine the behavior of key-value "set" operations.
/// </summary>
[Flags]
public enum KeyValueFlags
{
    None = 0,
    Set = 1 << 0,                    // 1
    SetNoRevision = 1 << 1,          // 2
    SetIfExists = 1 << 2,            // 4
    SetIfNotExists = 1 << 3,         // 8
    SetIfEqualToValue = 1 << 4,      // 16
    SetIfEqualToRevision = 1 << 5,   // 32

    /// <summary>
    /// Internal signal on a ReleaseProposal: the proposal was rejected by the key-range generation
    /// fence, so the release resolves the client promise as <c>MustRetry</c> (re-resolve
    /// LocateRange and retry), not <c>Errored</c>.
    /// </summary>
    FenceRetry = 1 << 6,             // 64

    /// <summary>
    /// Internal signal on a ReleaseProposal: replication failed with a transient status (leadership
    /// change, proposal timeout, queue full, restore in progress, …), so the release resolves the
    /// client promise as <c>MustRetry</c> — the caller retries and the write succeeds once the
    /// partition settles — rather than surfacing the retryable failure as terminal <c>Errored</c>.
    /// </summary>
    ReplicationRetry = 1 << 7,       // 128

    /// <summary>
    /// A transactional set that can no longer commit answers <c>Aborted</c> at once instead of <c>MustRetry</c>.
    /// The set is doomed when a live write intent of another transaction refuses it and this transaction already
    /// read the key (it holds a read pin on the key and staged no write there): the pin is below the other
    /// transaction's write, so the set can succeed only if that transaction rolls back. Without the flag the set
    /// answers <c>MustRetry</c> with the holder, and the caller waits for the holder's outcome. Ignored outside a
    /// transaction.
    /// </summary>
    AbortWhenDoomed = 1 << 8,        // 256

    /// <summary>
    /// A transactional set of a key that an older transaction has read, and is still running, answers
    /// <c>Aborted</c> at once instead of staging the write. Without the flag the write is staged, and the older
    /// reader is refused at its commit, after it did all its work, because a key it read carries a write intent.
    /// With it the younger of the two gives way at its first write, which is the order a shared lock on the key
    /// would give. The reader counts only while its coordinator session is open on the node that leads the key.
    /// Ignored outside a transaction. Experimental.
    /// </summary>
    YieldToOlderReaders = 1 << 9     // 512
}