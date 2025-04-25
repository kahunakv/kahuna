
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
    Rolledback
}