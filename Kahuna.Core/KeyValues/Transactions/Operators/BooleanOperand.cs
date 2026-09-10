using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Evaluates an operand that must be a boolean.
///
/// The language uses one truthiness model, and it is strict: only a boolean is a condition. A number, a
/// string, or a null in a condition is a script error, not a silent false. The alternative — treating a
/// non-zero number as true — was rejected because it leaves the string and null cases undefined, and
/// because a guard that silently does nothing is the worst outcome in a transaction language.
/// </summary>
internal static class BooleanOperand
{
    public static bool Require(ScriptTransactionContext context, NodeAst operandAst, NodeAst ast, string operatorName)
    {
        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, operandAst);

        if (result.Type != KeyValueExpressionType.BoolType)
            throw new KahunaScriptException($"Invalid operand for {operatorName}: expected a boolean, found {result.Type}", ast.yyline);

        return result.BoolValue;
    }
}
