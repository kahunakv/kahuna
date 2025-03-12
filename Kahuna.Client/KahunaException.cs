
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.Locks;

namespace Kahuna.Client;

public sealed class KahunaException : Exception
{
    public LockResponseType ErrorCode { get; }
    
    public KahunaException(string message, LockResponseType errorCode) : base(message)
    {
        ErrorCode = errorCode;
    }
}