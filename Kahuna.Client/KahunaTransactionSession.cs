
using System.Text;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Client;

/// <summary>
/// Represents a transaction session within the Kahuna key-value storage system.
/// It provides methods to perform operations such as setting, getting, committing,
/// and rolling back key-value pairs within the context of a transactional session.
/// </summary>
/// <remarks>
/// The KahunaTransactionSession class supports both single-step and multi-step key-value transactions
/// which may include locking mechanisms and durability preferences depending on the setup.
/// </remarks>
public class KahunaTransactionSession : IAsyncDisposable
{
    private KahunaClient Client { get; }

    private string Url { get; }

    private string UniqueId { get; }
    
    private KahunaTransactionStatus Status { get; set; } = KahunaTransactionStatus.Pending;
    
    public HLCTimestamp TransactionId { get; }
    
    private readonly SemaphoreSlim semaphore = new(1, 1);
    
    private HashSet<(string, KeyValueDurability)>? modifiedKeys;

    private bool disposed;
    
    public KahunaTransactionSession(KahunaClient client, string url, string uniqueId, HLCTimestamp transactionId)
    {
        Client = client;
        Url = url;
        UniqueId = uniqueId;
        TransactionId = transactionId;
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(
        string key, 
        byte[]? value, 
        int expiryTime = 0, 
        KeyValueFlags flags = KeyValueFlags.Set, 
        KeyValueDurability durability = KeyValueDurability.Persistent, 
        CancellationToken cancellationToken = default
    )
    {       
        if (Status != KahunaTransactionStatus.Pending)
            throw new KahunaException("Cannot perform actions on a completed transaction.", KeyValueResponseType.Errored);
        
        (bool success, long revision, int timeElapsedMs) = await Client.Communication.TrySetKeyValue(
            Url,
            TransactionId,
            key,
            value,
            expiryTime,
            flags,
            durability,
            cancellationToken
        ).ConfigureAwait(false);

        if (success)
        {
            modifiedKeys ??= [];
            modifiedKeys.Add((key, durability));
        }

        return new(Client, key, success, value, revision, durability, timeElapsedMs);        
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(
        string key, 
        string value, 
        int expiryTime = 0, 
        KeyValueFlags flags = KeyValueFlags.Set, 
        KeyValueDurability durability = KeyValueDurability.Persistent, 
        CancellationToken cancellationToken = default
    )
    {
        if (Status != KahunaTransactionStatus.Pending)
            throw new KahunaException("Cannot perform actions on a completed transaction.", KeyValueResponseType.Errored);
        
        byte[] valueBytes = Encoding.UTF8.GetBytes(value);
        
        (bool success, long revision, int timeElapsedMs) = await Client.Communication.TrySetKeyValue(
            Url, 
            TransactionId,
            key, 
            valueBytes, 
            expiryTime, 
            flags, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        if (success)
        {
            modifiedKeys ??= [];
            modifiedKeys.Add((key, durability));
        }
        
        return new(Client, key, success, valueBytes, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Retrieves a key-value pair from the server. If the key does not exist null is returned
    /// </summary>
    /// <param name="key">The key to identify the requested value.</param>
    /// <param name="durability">The specified durability level of the key-value pair, default is persistent.</param>
    /// <param name="cancellationToken">A token to propagate notifications that the operation should be canceled.</param>
    /// <returns>A task that represents the asynchronous operation, which returns a <see cref="KahunaKeyValue"/> containing the key-value information.</returns>
    public async Task<KahunaKeyValue> GetKeyValue(string key, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        if (Status != KahunaTransactionStatus.Pending)
            throw new KahunaException("Cannot perform actions on a completed transaction.", KeyValueResponseType.Errored);
        
        (bool success, byte[]? value, long revision, int timeElapsedMs) = await Client.Communication.TryGetKeyValue(
            Url, 
            TransactionId,
            key, 
            -1, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(Client, key, success, value, revision, durability, timeElapsedMs);
    }

    /// <summary>
    /// Commits the current transaction session if the transaction status is pending.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token that can be used to observe cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="KahunaException">
    /// Thrown if the transaction is not in the pending state or if an error occurs during the commit operation.
    /// </exception>
    public async Task<bool> Commit(CancellationToken cancellationToken = default)
    {
        if (Status != KahunaTransactionStatus.Pending)
            throw new KahunaException("Cannot commit a transaction that is not pending.", KeyValueResponseType.Errored);
        
        try
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            
            if (Status != KahunaTransactionStatus.Pending)
                throw new KahunaException("Cannot commit a transaction that is not pending.", KeyValueResponseType.Errored);
            
            List<KeyValueTransactionModifiedKey> modifiedKeysList = new();

            if (modifiedKeys is not null)
            {
                foreach ((string key, KeyValueDurability durability) in modifiedKeys)
                    modifiedKeysList.Add(new() { Key = key, Durability = durability});
            }
            
            bool result = await Client.Communication.CommitTransactionSession(
                Url,
                UniqueId,
                TransactionId,
                modifiedKeysList,
                cancellationToken
            ).ConfigureAwait(false);
            
            if (result)
                Status = KahunaTransactionStatus.Committed;
            
            return result;
        }
        finally
        {
            semaphore.Release();
        }
    }

    /// <summary>
    /// Rolls back the current transaction session, reverting all modifications made during the session.
    /// </summary>
    /// <param name="cancellationToken">A token that can be used to cancel the rollback operation.</param>
    /// <returns>A task that represents the asynchronous rollback operation.</returns>
    public async Task<bool> Rollback(CancellationToken cancellationToken = default)
    {
        if (Status != KahunaTransactionStatus.Pending)
            throw new KahunaException("Cannot rollback a transaction that is not pending.", KeyValueResponseType.Errored);

        try
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            if (Status != KahunaTransactionStatus.Pending)
                throw new KahunaException("Cannot rollback a transaction that is not pending.", KeyValueResponseType.Errored);
            
            List<KeyValueTransactionModifiedKey> modifiedKeysList = new();

            if (modifiedKeys is not null)
            {
                foreach ((string key, KeyValueDurability durability) in modifiedKeys)
                    modifiedKeysList.Add(new() { Key = key, Durability = durability});
            }

            bool result = await Client.Communication.RollbackTransactionSession(
                Url,
                UniqueId,
                TransactionId,
                modifiedKeysList,
                cancellationToken
            ).ConfigureAwait(false);

            if (result)
                Status = KahunaTransactionStatus.Rolledback;

            return result;
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (disposed)
            return;

        disposed = true;
        
        GC.SuppressFinalize(this);
        
        if (Status == KahunaTransactionStatus.Pending)
            await Rollback().ConfigureAwait(false);
        
        semaphore.Dispose();
    }
}