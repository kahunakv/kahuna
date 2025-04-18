
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

namespace Kahuna.Client.Communication;

public interface IKahunaCommunication
{
     Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken);

     Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken);

     Task<(bool, long)> TryExtendLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken);

     Task<KahunaLockInfo?> Get(string url, string resource, LockDurability durability, CancellationToken cancellationToken);

     Task<(bool, long)> TrySetKeyValue(string url, string key, byte[]? value, int expiryTime, KeyValueFlags flags, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, long)> TryCompareValueAndSetKeyValue(string url, string key, byte[]? value, byte[]? compareValue, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, long)> TryCompareRevisionAndSetKeyValue(string url, string key, byte[]? value, long compareRevision, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, byte[]?, long)> TryGetKeyValue(string url, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken);
     
     Task<(bool, long)> TryExistsKeyValue(string url, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, long)> TryDeleteKeyValue(string url, string key, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, long)> TryExtendKeyValue(string url, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransaction(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken);

     Task<(bool, List<string>)> GetByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken);
     
     Task<(bool, List<string>)> ScanAllByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken);
}