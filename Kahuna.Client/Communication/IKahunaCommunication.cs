
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Client.Communication;

public interface IKahunaCommunication
{
     Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken);

     Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken);

     Task<(bool, long)> TryExtendLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken);

     Task<KahunaLockInfo?> GetLock(string url, string resource, LockDurability durability, CancellationToken cancellationToken);

     Task<(bool, long, int)> TrySetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, int expiryTime, KeyValueFlags flags, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, long, int)> TryCompareValueAndSetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, long, int)> TryCompareRevisionAndSetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, long compareRevision, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, byte[]?, long, int)> TryGetKeyValue(string url, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken);
     
     Task<(bool, long, int)> TryExistsKeyValue(string url, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, long, int)> TryDeleteKeyValue(string url, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<(bool, long, int)> TryExtendKeyValue(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

     Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransactionScript(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken);

     Task<(bool, List<string>)> GetByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken);
     
     Task<(bool, List<string>)> ScanAllByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken);
     
     Task<(string, HLCTimestamp transactionId)> StartTransactionSession(string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken);
     
     Task<bool> CommitTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, CancellationToken cancellationToken);
     
     Task<bool> RollbackTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, CancellationToken cancellationToken);
}