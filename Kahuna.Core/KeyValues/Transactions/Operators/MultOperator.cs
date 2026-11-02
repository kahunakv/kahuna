using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Evaluates a multiplication by the rules of <see cref="ScriptNumeric"/>: two longs multiply as longs and
/// an overflow is a script error; any other numeric pair multiplies as doubles and a result that is not
/// finite is a script error.
/// </summary>
internal static class MultOperator
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
            throw new KahunaScriptException("Invalid operands: " + left.Type + " * " + right.Type, ast.yyline);

        if (a.IsLong && b.IsLong)
            return new(ScriptNumeric.Multiply(a.Long, b.Long, ast));

        return new(ScriptNumeric.RequireFinite(a.AsDouble * b.AsDouble, a, "*", b, ast));
    }
}
