using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Evaluates a unary minus. The scanner does not fold a sign into a numeric literal, so every negative
/// value in a script arrives here, and "5-3" without spaces stays a subtraction rather than two adjacent
/// literals.
/// </summary>
internal static class NegateOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid expression to negate", ast.yyline);

        KeyValueExpressionResult operand = KeyValueTransactionExpression.Eval(context, ast.leftAst);

        // The arithmetic operators all coerce a numeric string, so unary minus does the same, and by the same
        // rule: a string that spells NaN or an infinity is not a number.
        if (operand.Type is not (KeyValueExpressionType.LongType or KeyValueExpressionType.DoubleType or KeyValueExpressionType.StringType)
            || !ScriptNumeric.TryRead(operand, out ScriptNumber number))
            throw new KahunaScriptException("Invalid operand to negate: " + operand.Type, ast.yyline);

        // long.MinValue has no positive counterpart. The literal cannot reach here (the scanner would have to
        // accept a magnitude one above long.MaxValue), but a computed value or a numeric string can, and
        // silently wrapping to itself would be worse than a script error.
        if (number.IsLong)
            return new(ScriptNumeric.Negate(number.Long, ast));

        return new(-number.Double);
    }
}
