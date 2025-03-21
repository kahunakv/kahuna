
using Kahuna.Server.ScriptParser;
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

    public KeyValueExpressionResult GetVariable(NodeAst ast, string varName)
    {
        //Console.WriteLine("Get variable {0}", varName);
        
        if (Variables is null)
            throw new KahunaScriptException("Undefined variable: " + varName, ast.yyline);
        
        if (!Variables.TryGetValue(varName, out KeyValueExpressionResult? value))
            throw new KahunaScriptException("Undefined variable: " + varName, ast.yyline);

        return value;
    }
    
    public void SetVariable(NodeAst ast, string varName, KeyValueExpressionResult value)
    {
        //Console.WriteLine("Set variable {0} {1}", varName, value.ToString());
        
        Variables ??= new();
        
        Variables[varName] = value;
    }
}