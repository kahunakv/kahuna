
using System.Globalization;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal sealed class LessThanOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast, string operatorType)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
            
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
            
        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);
    
        switch (left.Type)
        {
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return KeyValueExpressionResult.FromBool(left.LongValue < right.LongValue);
        
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return KeyValueExpressionResult.FromBool(left.DoubleValue < right.LongValue);
        
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return KeyValueExpressionResult.FromBool(left.LongValue < right.DoubleValue);
        
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return KeyValueExpressionResult.FromBool(left.DoubleValue < right.DoubleValue);
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.DoubleType:
            {
                if (!long.TryParse(left.StrValue, out long leftLong))
                {
                    if (!double.TryParse(left.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double leftDouble))                    
                        throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
                
                    return KeyValueExpressionResult.FromBool(leftDouble < right.DoubleValue);
                }

                return KeyValueExpressionResult.FromBool(leftLong < right.DoubleValue);
            }

            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.LongType:
            {
                if (!long.TryParse(left.StrValue, out long leftLong))
                {
                    if (!double.TryParse(left.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double leftDouble))                    
                        throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
                
                    return KeyValueExpressionResult.FromBool(leftDouble < right.LongValue);
                }

                return KeyValueExpressionResult.FromBool(leftLong < right.LongValue);
            }

            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.StringType:
            {
                if (!long.TryParse(right.StrValue, out long rightLong))
                {
                    if (!double.TryParse(right.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double rightDouble))                    
                        throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
                    
                    return KeyValueExpressionResult.FromBool(left.LongValue < rightDouble);
                }

                return KeyValueExpressionResult.FromBool(left.LongValue < rightLong);
            }
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.StringType:
            {
                if (!long.TryParse(right.StrValue, out long rightLong))
                {
                    if (!double.TryParse(right.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double rightDouble))                    
                        throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
                    
                    return KeyValueExpressionResult.FromBool(left.DoubleValue < rightDouble);
                }

                return KeyValueExpressionResult.FromBool(left.DoubleValue < rightLong);
            }
            
            default:
                throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
        }
    }
}
