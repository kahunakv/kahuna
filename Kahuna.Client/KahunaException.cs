
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;

namespace Kahuna.Client;

/// <summary>
/// Identifies which of the three response-code families a <see cref="KahunaException"/> carries.
/// Each family has its own enum, and only one of them is set for a given exception. Without this
/// marker a reader cannot tell an unset code from a real code, because every enum has a member
/// with the value 0.
/// </summary>
public enum KahunaErrorDomain
{
    Lock,
    KeyValue,
    Sequence
}

/// <summary>
/// Represents an exception specific to the Kahuna client operations.
/// </summary>
public sealed class KahunaException : Exception
{
    public LockResponseType LockErrorCode { get; }
    
    public KeyValueResponseType KeyValueErrorCode { get; }

    public SequenceResponseType SequenceErrorCode { get; }

    /// <summary>
    /// Tells which response-code family this exception carries. Read this before you read a code.
    /// </summary>
    public KahunaErrorDomain ErrorDomain { get; }

    /// <summary>
    /// The name of the response code that belongs to <see cref="ErrorDomain"/>. Use this for logs
    /// and reports, because it never shows the default 0 member of a family that was not set.
    /// </summary>
    public string ErrorCodeName => ErrorDomain switch
    {
        KahunaErrorDomain.Lock => LockErrorCode.ToString(),
        KahunaErrorDomain.KeyValue => KeyValueErrorCode.ToString(),
        _ => SequenceErrorCode.ToString()
    };
    
    /// <summary>
    /// Represents an exception specific to Kahuna Lock operations.
    /// </summary>
    public KahunaException(string message, LockResponseType errorCode) : base(message)
    {
        LockErrorCode = errorCode;
        ErrorDomain = KahunaErrorDomain.Lock;
    }

    /// <summary>
    /// Represents an exception specific to Kahuna key/value operations.
    /// </summary>
    public KahunaException(string message, KeyValueResponseType errorCode) : base(message)
    {
        KeyValueErrorCode = errorCode;
        ErrorDomain = KahunaErrorDomain.KeyValue;
    }

    public KahunaException(string message, SequenceResponseType errorCode) : base(message)
    {
        SequenceErrorCode = errorCode;
        ErrorDomain = KahunaErrorDomain.Sequence;
    }
}
