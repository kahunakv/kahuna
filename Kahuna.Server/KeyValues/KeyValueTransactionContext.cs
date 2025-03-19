
using System.Text;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValueTransactionContext
{
    public HLCTimestamp TransactionId { get; set; }
    
    public KeyValueTransactionResult? Result { get; set; }

    public KeyValueTransactionAction Action { get; set; }
    
    public KeyValueExecutionStatus Status { get; set; } = KeyValueExecutionStatus.Continue;
    
    public List<string>? LocksAcquired { get; set; }

    private Dictionary<string , KeyValueTransactionResult>? Variables { get; set; }

    public KeyValueTransactionResult GetVariable(string varName)
    {
        Console.WriteLine("Get variable {0}", varName);
        
        if (Variables is null)
            throw new InvalidOperationException("Undefined variable: " + varName);
        
        return Variables[varName];
    }
    
    public void SetVariable(string varName, KeyValueTransactionResult value)
    {
        Console.WriteLine("Set variable {0} {1}", varName, Encoding.UTF8.GetString(value.Value ?? []));
        
        Variables ??= new();
        
        Variables[varName] = value;
    }
}