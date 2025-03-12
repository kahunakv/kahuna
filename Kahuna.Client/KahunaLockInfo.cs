
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
    public string Owner { get; }
    
    public HLCTimestamp Expires { get; }
    
    public long FencingToken { get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="owner"></param>
    /// <param name="expires"></param>
    /// <param name="fencingToken"></param>
    public KahunaLockInfo(string owner, HLCTimestamp expires, long fencingToken)
    {
        Owner = owner;
        Expires = expires;
        FencingToken = fencingToken;
    }
}