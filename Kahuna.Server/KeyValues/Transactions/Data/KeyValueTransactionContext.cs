
// ReSharper disable UnassignedGetOnlyAutoProperty

using Kommander.Time;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Represents the context of a transaction.
/// It includes all the necessary information to execute a transaction.
/// </summary>
internal sealed class KeyValueTransactionContext
{
    public HLCTimestamp TransactionId { get; init; }
    
    public KeyValueTransactionLocking Locking { get; init; }
    
    public KeyValueTransactionResult? Result { get; set; }

    public KeyValueTransactionAction Action { get; set; }
    
    public bool AsyncRelease { get; set; }
    
    public KeyValueExecutionStatus Status { get; set; } = KeyValueExecutionStatus.Continue;
    
    public List<(string, KeyValueDurability)>? LocksAcquired { get; set; }
    
    public List<(string, KeyValueDurability)>? ModifiedKeys { get; set; }
    
    public List<KeyValueParameter>? Parameters { get; init; }

    private Dictionary<string , KeyValueExpressionResult>? Variables { get; set; }

    public KeyValueExpressionResult GetVariable(NodeAst ast, string varName)
    {
        if (Variables is null)
            throw new KahunaScriptException("Undefined variable: " + varName, ast.yyline);
        
        if (!Variables.TryGetValue(varName, out KeyValueExpressionResult? value))
            throw new KahunaScriptException("Undefined variable: " + varName, ast.yyline);

        return value;
    }
    
    public void SetVariable(NodeAst ast, string varName, KeyValueExpressionResult value)
    {
        Variables ??= new();
        
        Variables[varName] = value;
    }
    
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
}