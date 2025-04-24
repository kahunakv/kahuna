
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

namespace Kahuna.Client;

/// <summary>
/// Represents an exception specific to the Kahuna client operations.
/// </summary>
public sealed class KahunaException : Exception
{
    public LockResponseType LockErrorCode { get; }
    
    public KeyValueResponseType KeyValueErrorCode { get; }
    
    /// <summary>
    /// Represents an exception specific to Kahuna Lock operations.
    /// </summary>
    public KahunaException(string message, LockResponseType errorCode) : base(message)
    {
        LockErrorCode = errorCode;
    }

    /// <summary>
    /// Represents an exception specific to Kahuna key/value operations.
    /// </summary>
    public KahunaException(string message, KeyValueResponseType errorCode) : base(message)
    {
        KeyValueErrorCode = errorCode;
    }
}