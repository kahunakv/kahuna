using System.Globalization;
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

        switch (operand.Type)
        {
            case KeyValueExpressionType.LongType:
                // long.MinValue has no positive counterpart, so negating it overflows. The literal cannot
                // reach here (the scanner would have to accept a magnitude one above long.MaxValue), but a
                // computed value can, and silently wrapping to itself would be worse than a script error.
                if (operand.LongValue == long.MinValue)
                    throw new KahunaScriptException("Cannot negate " + long.MinValue, ast.yyline);

                return new(-operand.LongValue);

            case KeyValueExpressionType.DoubleType:
                return new(-operand.DoubleValue);

            case KeyValueExpressionType.StringType:
            {
                // The arithmetic operators all coerce a numeric string, so unary minus does the same.
                if (long.TryParse(operand.StrValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out long asLong))
                    return new(-asLong);

                if (double.TryParse(operand.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double asDouble))
                    return new(-asDouble);

                throw new KahunaScriptException("Invalid operand to negate: " + operand.Type, ast.yyline);
            }

            default:
                throw new KahunaScriptException("Invalid operand to negate: " + operand.Type, ast.yyline);
        }
    }
}
