using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Evaluates a subtraction by the rules of <see cref="ScriptNumeric"/>: two longs subtract as longs and an
/// overflow is a script error; any other numeric pair subtracts as doubles and a result that is not finite
/// is a script error.
/// </summary>
internal static class SubOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);

        if (!ScriptNumeric.TryReadOperands(left, right, out ScriptNumber a, out ScriptNumber b))
            throw new KahunaScriptException("Invalid operands: " + left.Type + " - " + right.Type, ast.yyline);

        if (a.IsLong && b.IsLong)
            return new(ScriptNumeric.Subtract(a.Long, b.Long, ast));

        return new(ScriptNumeric.RequireFinite(a.AsDouble - b.AsDouble, a, "-", b, ast));
    }
}
