
using System.Globalization;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

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
        if (IsZero(right))
            throw new KahunaScriptException("Division by zero", ast.yyline);

        switch (left.Type)
        {
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new(left.LongValue / right.LongValue);
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new(left.DoubleValue / right.LongValue);
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new(left.LongValue / right.DoubleValue);
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new(left.DoubleValue / right.DoubleValue);
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.DoubleType:
            {
                if (!long.TryParse(left.StrValue, out long leftLong))
                {
                    if (!double.TryParse(left.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double leftDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " / " + right.Type, ast.yyline);
                
                    return new(leftDouble / right.DoubleValue);
                }

                return new(leftLong / right.DoubleValue);
            }

            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.LongType:
            {
                if (!long.TryParse(left.StrValue, out long leftLong))
                {
                    if (!double.TryParse(left.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double leftDouble))              
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " / " + right.Type, ast.yyline);
                
                    return new(leftDouble / right.LongValue);
                }

                return new(leftLong / right.LongValue);
            }

            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.StringType:
            {
                if (!long.TryParse(right.StrValue, out long rightLong))
                {
                    if (!double.TryParse(right.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double rightDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " / " + right.Type, ast.yyline);
                    
                    return new(left.LongValue / rightDouble);
                }

                return new(left.LongValue / rightLong);
            }
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.StringType:
            {
                if (!long.TryParse(right.StrValue, out long rightLong))
                {
                    if (!double.TryParse(right.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double rightDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " / " + right.Type, ast.yyline);
                    
                    return new(left.DoubleValue / rightDouble);
                }

                return new(left.DoubleValue / rightLong);
            }
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " / " + right.Type, ast.yyline);
        }
    }

    /// <summary>
    /// Reports whether the divisor is zero, covering the numeric string the operator also accepts. A string
    /// that is not a number is not zero: the operand switch below reports it as an invalid operand instead,
    /// which names both types and is the more useful message.
    /// </summary>
    private static bool IsZero(KeyValueExpressionResult right)
    {
        switch (right.Type)
        {
            case KeyValueExpressionType.LongType:
                return right.LongValue == 0;

            case KeyValueExpressionType.DoubleType:
                return right.DoubleValue == 0;

            case KeyValueExpressionType.StringType:
                if (long.TryParse(right.StrValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out long asLong))
                    return asLong == 0;

                if (double.TryParse(right.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double asDouble))
                    return asDouble == 0;

                return false;

            default:
                return false;
        }
    }
}
