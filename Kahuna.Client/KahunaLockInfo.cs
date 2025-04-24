
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace Kahuna.Client;

/// <summary>
/// Represents the information of a lock acquired from the Kahuna service.
/// </summary>
public sealed class KahunaLockInfo
{
    /// <summary>
    /// Represents the identifier of the lock's owner.
    /// This property holds information that uniquely identifies the entity
    /// that currently holds the lock, enabling attribution and conflict resolution.
    /// </summary>
    public byte[]? Owner { get; }

    /// <summary>
    /// Represents the timestamp indicating when the lock will expire.
    /// This property provides information about the time limit set for a lock's validity,
    /// ensuring that locks are automatically invalidated after a specific duration.
    /// </summary>
    public HLCTimestamp Expires { get; }

    /// <summary>
    /// A unique identifier for determining the fencing token associated with a lock instance.
    /// The fencing token is used to ensure consistency and safety in distributed systems by
    /// preventing outdated lock holders from making changes once a newer token is issued.
    /// </summary>
    public long FencingToken { get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="owner"></param>
    /// <param name="expires"></param>
    /// <param name="fencingToken"></param>
    public KahunaLockInfo(byte[]? owner, HLCTimestamp expires, long fencingToken)
    {
        Owner = owner;
        Expires = expires;
        FencingToken = fencingToken;
    }
}