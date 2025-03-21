
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValueTransactionContext
{
    public HLCTimestamp TransactionId { get; set; }
    
    public KeyValueTransactionResult? Result { get; set; }

    public KeyValueTransactionAction Action { get; set; }
    
    public KeyValueExecutionStatus Status { get; set; } = KeyValueExecutionStatus.Continue;
    
    public List<(string, KeyValueConsistency)>? LocksAcquired { get; set; }

    private Dictionary<string , KeyValueExpressionResult>? Variables { get; set; }

    public KeyValueExpressionResult GetVariable(string varName)
    {
        Console.WriteLine("Get variable {0}", varName);
        
        if (Variables is null)
            throw new InvalidOperationException("Undefined variable: " + varName);
        
        if (!Variables.TryGetValue(varName, out KeyValueExpressionResult? value))
            throw new InvalidOperationException("Undefined variable: " + varName);

        return value;
    }
    
    public void SetVariable(string varName, KeyValueExpressionResult value)
    {
        Console.WriteLine("Set variable {0} {1}", varName, value.ToString());
        
        Variables ??= new();
        
        Variables[varName] = value;
    }
}