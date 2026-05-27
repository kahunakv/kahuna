
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// 
/// </summary>
public enum KeyValueTransactionState
{
    /// <summary>
    /// 
    /// </summary>
    Pending,

    /// <summary>
    /// 
    /// </summary>
    Preparing,
    
    /// <summary>
    /// 
    /// </summary>
    Prepared,
    
    /// <summary>
    /// 
    /// </summary>
    Committing,
    
    /// <summary>
    /// 
    /// </summary>
    Committed,
    
    /// <summary>
    /// 
    /// </summary>
    RollingBack,
    
    /// <summary>
    /// 
    /// </summary>
    RolledBack
}