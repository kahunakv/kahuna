using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Evaluates '&lt;'. '&gt;=' is its negation. Numbers compare by their exact values (see
/// <see cref="ScriptNumeric.Compare"/>), and a numeric string compares as the number it spells.
/// </summary>
internal static class LessThanOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast, string operatorType)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);

        if (!ScriptNumeric.TryReadOperands(left, right, out ScriptNumber a, out ScriptNumber b))
            throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);

        return KeyValueExpressionResult.FromBool(ScriptNumeric.Compare(a, b) < 0);
    }
}
