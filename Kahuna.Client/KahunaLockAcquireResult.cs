
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client;

/// <summary>
/// Represents the result of an attempt to acquire a lock in the Kahuna system.
/// </summary>
public enum KahunaLockAcquireResult
{
    Success = 0,
    Conflicted = 1,
    Error = 2,
}