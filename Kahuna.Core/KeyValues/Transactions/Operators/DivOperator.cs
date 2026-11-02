using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Evaluates a division by the rules of <see cref="ScriptNumeric"/>. Two longs divide as longs, truncated
/// toward zero, and long.MinValue / -1 is a script error. Any other numeric pair divides as doubles, and a
/// result that is not finite is a script error.
/// </summary>
internal static class DivOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);

        // One answer for both numeric types. An integer divide used to raise a framework exception with no
        // script line, while a double divide returned infinity and carried that value on into a stored key.
        // The divisor is checked first, so a zero divisor is reported as such whatever the dividend is. A
        // string that is not a number is not zero: the operand check below reports it as an invalid operand,
        // which names both types and is the more useful message.
        bool divisorIsNumber = ScriptNumeric.TryRead(right, out ScriptNumber b);

        if (divisorIsNumber && b.IsZero)
            throw new KahunaScriptException("Division by zero", ast.yyline);

        if (!divisorIsNumber || !ScriptNumeric.TryReadOperands(left, right, out ScriptNumber a, out b))
            throw new KahunaScriptException("Invalid operands: " + left.Type + " / " + right.Type, ast.yyline);

        if (a.IsLong && b.IsLong)
            return new(ScriptNumeric.Divide(a.Long, b.Long, ast));

        return new(ScriptNumeric.RequireFinite(a.AsDouble / b.AsDouble, a, "/", b, ast));
    }
}
