
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Extends <see cref="TransactionContext"/> with script-execution state: parameters, local
/// variables, execution flow control, and AST-backed accessors. Only the script executor and
/// its command/expression helpers depend on this type.
/// </summary>
internal sealed class ScriptTransactionContext : TransactionContext
{
    /// <summary>
    /// Controls whether the script executor continues executing statements or stops.
    /// </summary>
    public KeyValueExecutionStatus Status { get; set; } = KeyValueExecutionStatus.Continue;

    /// <summary>
    /// Script parameters (placeholders) passed into the script at execution time.
    /// </summary>
    public List<KeyValueParameter>? Parameters { get; init; }

    /// <summary>
    /// Local-scope variables allocated during script execution; released when the script finishes.
    /// </summary>
    private Dictionary<string, KeyValueExpressionResult>? Variables { get; set; }

    /// <summary>
    /// Returns the value of a named local variable. Throws a script error when undefined.
    /// </summary>
    public KeyValueExpressionResult GetVariable(NodeAst ast, string varName)
    {
        if (Variables is null || !Variables.TryGetValue(varName, out KeyValueExpressionResult? value))
            throw new KahunaScriptException("Undefined variable: " + varName, ast.yyline);

        return value;
    }

    /// <summary>
    /// Sets or overwrites a local variable.
    /// </summary>
    public void SetVariable(NodeAst ast, string varName, KeyValueExpressionResult value)
    {
        Variables ??= new();
        Variables[varName] = value;
    }

    /// <summary>
    /// Returns the string value of a parameter placeholder. Throws a script error when missing.
    /// </summary>
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
