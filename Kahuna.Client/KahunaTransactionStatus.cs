
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client;

/// <summary>
/// Represents the status of a transaction in Kahuna, encapsulating its lifecycle states.
/// </summary>
public enum KahunaTransactionStatus
{
    /// <summary>
    /// Represents a transaction that is currently in progress and has not yet been
    /// either committed or rolled back. The Pending state indicates that the transaction
    /// is active and its final outcome is yet to be determined.
    /// </summary>
    Pending,

    /// <summary>
    /// A commit or rollback has been requested and is in flight. The session is closed to new operations
    /// so no work can be issued after finalization began; a finalize that fails returns the session to
    /// <see cref="Pending"/> so the caller can retry.
    /// </summary>
    Finalizing,

    /// <summary>
    /// Represents a transaction that has been successfully finalized and all changes
    /// made during the transaction have been permanently saved to the underlying system.
    /// The Committed state signifies the completion of the transaction's lifecycle
    /// with all operations confirmed.
    /// </summary>
    Committed,

    /// <summary>
    /// Indicates that the transaction has been reverted to its previous state due to a rollback operation.
    /// This status reflects that the changes made during the transaction have been discarded,
    /// ensuring that the system remains consistent and no partial updates are retained.
    /// </summary>
    Rolledback,

    /// <summary>
    /// A commit was definitively aborted — a read-set conflict or a permanent two-phase-commit failure — so
    /// the transaction will never commit. This is terminal, not retryable: the server has already finalized
    /// (released and removed) the transaction, so the caller must start a new one rather than retry, and
    /// disposal does not roll back again. Distinct from <see cref="Pending"/>, which a transient
    /// <c>MustRetry</c> returns to so the same finalize can be retried.
    /// </summary>
    Aborted
}