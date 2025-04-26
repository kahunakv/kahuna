
// ReSharper disable UnassignedGetOnlyAutoProperty

using Kommander.Time;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Represents the context for a key-value transaction.
/// This class contains details about the transaction, such as the transaction ID,
/// parameters, status, locking information, and variables.
/// Provides methods to access and manipulate transaction variables and parameters.
/// </summary>
internal sealed class KeyValueTransactionContext
{
    /// <summary>
    /// Gets the unique identifier for the current transaction context.
    /// </summary>
    /// <remarks>
    /// The <see cref="TransactionId"/> property represents a hybrid logical clock (HLC) timestamp
    /// that uniquely identifies the transaction. It is used for tracking and managing the
    /// transaction's lifecycle within the system and ensures consistency across distributed operations.
    /// </remarks>
    public HLCTimestamp TransactionId { get; init; }

    /// <summary>
    /// Gets the timeout duration for the current transaction context.
    /// </summary>
    /// <remarks>
    /// The <see cref="Timeout"/> property specifies the maximum duration, in milliseconds,
    /// that the transaction is allowed to execute before timing out. If the transaction exceeds
    /// this duration without completion, it may be terminated or rolled back to ensure system stability.
    /// This property is configured based on input options or default system settings.
    /// </remarks>
    public int Timeout { get; init; }

    /// <summary>
    /// Gets the locking strategy for the current transaction context.
    /// </summary>
    /// <remarks>
    /// The <see cref="Locking"/> property determines whether the transaction
    /// uses a pessimistic or optimistic locking model for concurrency control.
    /// This setting is critical for ensuring data consistency and avoiding conflicts
    /// during transaction execution in distributed systems.
    /// </remarks>
    public KeyValueTransactionLocking Locking { get; init; }

    /// <summary>
    /// Gets or sets the last result of the current key-value execution
    /// </summary>
    /// <remarks>
    /// The <see cref="Result"/> property holds the outcome of the key-value transaction
    /// represented by a <see cref="KeyValueTransactionResult"/> object. It provides the details
    /// regarding the transaction’s execution and final status. This property can be evaluated
    /// to determine the completion or error state of the transaction during execution.
    /// </remarks>
    public KeyValueTransactionResult? Result { get; set; }

    /// <summary>
    /// Gets or sets the last result of a key-value write operation
    /// </summary>
    /// <remarks>
    /// The <see cref="ModifiedResult"/> property contains detailed information about the outcome
    /// of a key-value transaction, including the response type and a collection of modified values.
    /// It serves as a representation of the transaction's impact, allowing further operations to assess
    /// its state and outcomes for consistency or rollback purposes.
    /// </remarks>
    public KeyValueTransactionResult? ModifiedResult { get; set; }

    /// <summary>
    /// Gets or sets the current action to be performed within the transaction context.
    /// </summary>
    /// <remarks>
    /// The <see cref="Action"/> property represents the desired course of action for
    /// managing the state of the transaction. It is an enumeration of type
    /// <see cref="KeyValueTransactionAction"/>, allowing the context to dictate whether
    /// the transaction should commit or abort based on its execution flow.
    /// This property is pivotal in controlling transaction lifecycle operations.
    /// </remarks>
    public KeyValueTransactionAction Action { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether transaction resources should be released asynchronously upon completion.
    /// </summary>
    /// <remarks>
    /// The <see cref="AsyncRelease"/> property determines whether the cleanup of transaction-related resources
    /// is performed asynchronously. When set to true, resources are released in a non-blocking manner, allowing
    /// better responsiveness in scenarios with long-running transactions or high concurrency. If set to false,
    /// resource release occurs synchronously, which may simplify management but could introduce latency in
    /// high-throughput environments.
    /// </remarks>
    public bool AsyncRelease { get; set; }

    /// <summary>
    /// Gets or sets the current execution status of the key-value transaction context.
    /// </summary>
    /// <remarks>
    /// The <see cref="Status"/> property represents the state of the transaction execution,
    /// indicating whether the transaction should continue or stop. It is primarily used
    /// to control the flow of commands within the context of key-value operations.
    /// Possible values are defined in the <see cref="KeyValueExecutionStatus"/> enumeration.
    /// </remarks>
    public KeyValueExecutionStatus Status { get; set; } = KeyValueExecutionStatus.Continue;

    /// <summary>
    /// Tracks the set of locks acquired during the execution of a transaction.
    /// </summary>
    /// <remarks>
    /// The <see cref="LocksAcquired"/> property maintains a collection of unique keys and their
    /// respective durability levels that have been locked while processing a transaction.
    /// This property helps ensure the proper management of locking mechanisms across
    /// multiple commands in a transaction and prevents conflicting modifications to
    /// shared resources.
    /// </remarks>
    public HashSet<(string, KeyValueDurability)>? LocksAcquired { get; set; }

    /// <summary>
    /// Gets or sets the collection of keys modified during the transaction along with their durability specification.
    /// </summary>
    /// <remarks>
    /// The <see cref="ModifiedKeys"/> property holds a set of key-durability pairs that represent
    /// the keys altered as part of the transaction. Each entry includes the key name and its
    /// associated <see cref="KeyValueDurability"/> value, indicating whether the key modification
    /// is ephemeral or persistent.
    /// This property is updated dynamically when keys are created, updated, or deleted within
    /// the transaction context. It provides a comprehensive view of all changes made during
    /// the transaction, enabling tracking and post-processing.
    /// </remarks>
    public HashSet<(string, KeyValueDurability)>? ModifiedKeys { get; set; }

    /// <summary>
    /// Gets the collection of key-value parameters (placeholders) associated with the transaction context.
    /// </summary>
    /// <remarks>
    /// The <see cref="Parameters"/> property is used to store a list of key-value parameters (placeholders) that
    /// are passed to the execution of the transaction. If no parameters are
    /// associated, the value will be null. These parameters can be used to dynamically pass
    /// arbitrary values to the transaction script reusing the parsing context (ast)
    /// </remarks>
    public List<KeyValueParameter>? Parameters { get; init; }

    /// <summary>
    /// Gets or sets the collection of local-scope variables that are used within the transaction context.
    /// </summary>
    /// <remarks>
    /// The <see cref="Variables"/> property maintains a dictionary mapping variable names to their
    /// associated <see cref="KeyValueExpressionResult"/> values. This allows dynamic variable management
    /// during transaction execution. If a variable is undefined or missing, an
    /// appropriate exception will be thrown.
    /// Variables are released once the transaction is executed
    /// </remarks>
    private Dictionary<string, KeyValueExpressionResult>? Variables { get; set; }

    /// <summary>
    /// Gets or sets the current state of the transaction's execution lifecycle.
    /// </summary>
    /// <remarks>
    /// The <see cref="State"/> property represents the operational status of a key-value transaction at any given moment.
    /// This includes states such as <c>Pending</c>, <c>Preparing</c>, <c>Prepared</c>, <c>Committing</c>, <c>Committed</c>,
    /// <c>RollingBack</c>, and <c>RolledBack</c>. The state reflects the progress and actions being performed on the transaction
    /// and helps coordinate transaction consistency and recovery processes.
    /// </remarks>
    private KeyValueTransactionState state = KeyValueTransactionState.Pending;
    
    /// <summary>
    /// Returns the current state of the transaction.
    /// </summary>
    public KeyValueTransactionState State => state; 

    /// <summary>
    /// Retrieves a local-scope variable value associated with the specified variable name.
    /// </summary>
    /// <param name="ast">The abstract syntax tree node associated with the variable.</param>
    /// <param name="varName">The name of the variable to retrieve.</param>
    /// <returns>The value of the variable as a <see cref="KeyValueExpressionResult"/>.</returns>
    /// <exception cref="KahunaScriptException">Thrown when the variable is not defined or cannot be found.</exception>
    public KeyValueExpressionResult GetVariable(NodeAst ast, string varName)
    {
        if (Variables is null || !Variables.TryGetValue(varName, out KeyValueExpressionResult? value))
            throw new KahunaScriptException("Undefined variable: " + varName, ast.yyline);

        return value;
    }

    /// <summary>
    /// Sets a local-scope variable with the specified name and value.
    /// </summary>
    /// <param name="ast">The abstract syntax tree node associated with the variable.</param>
    /// <param name="varName">The name of the variable to set or overwrite.</param>
    /// <param name="value">The value to assign to the variable.</param>
    public void SetVariable(NodeAst ast, string varName, KeyValueExpressionResult value)
    {
        Variables ??= new();

        Variables[varName] = value;
    }

    /// <summary>
    /// Retrieves the value of a parameter (placeholder)
    /// </summary>
    /// <param name="ast">The abstract syntax tree node used to identify the parameter.</param>
    /// <returns>The value of the parameter as a string.</returns>
    /// <exception cref="KahunaScriptException">Thrown when the parameter is undefined or cannot be found.</exception>
    public string GetParameter(NodeAst ast)
    {
        if (Parameters is null)
            throw new KahunaScriptException("Undefined parameter: " + ast.yytext!, ast.yyline);
        
        foreach (KeyValueParameter variable in Parameters)
        {
            if (variable.Key == ast.yytext!)
                return variable.Value!;
        }
        
        throw new KahunaScriptException("Undefined parameter: " + ast.yytext!, ast.yyline);
    }

    /// <summary>
    /// Attempts to update the transaction state to a new state, if it matches the expected current state.
    /// </summary>
    /// <param name="newState">The desired new state for the transaction.</param>
    /// <param name="expectedState">The current state that is required before performing the update.</param>
    /// <returns>True if the state was successfully updated; otherwise, false.</returns>
    public bool SetState(KeyValueTransactionState newState, KeyValueTransactionState expectedState)
    {
        return expectedState == Interlocked.CompareExchange(ref state, newState, expectedState);
    }    
}