using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Provides functionality to evaluate math addition operations within the context of a
/// key-value transaction represented by a specific AST (Abstract Syntax Tree) node.
///
/// <para>Two longs add as longs, and a sum outside the long range is a script error. Any other numeric pair
/// adds as doubles, and a sum that is not finite is a script error. The rules live in
/// <see cref="ScriptNumeric"/>, which every arithmetic operator shares.</para>
/// </summary>
internal static class AddOperator
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
            throw new KahunaScriptException("Invalid operands: " + left.Type + " + " + right.Type, ast.yyline);

        if (a.IsLong && b.IsLong)
            return new(ScriptNumeric.Add(a.Long, b.Long, ast));

        return new(ScriptNumeric.RequireFinite(a.AsDouble + b.AsDouble, a, "+", b, ast));
    }
}
