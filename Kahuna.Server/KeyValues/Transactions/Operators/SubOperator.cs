
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal sealed class SubOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
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
                return new() { Type = KeyValueExpressionType.LongType, LongValue = left.LongValue - right.LongValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue - right.LongValue };
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.LongValue - right.DoubleValue };
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = left.DoubleValue - right.DoubleValue };
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.DoubleType:
                if (!double.TryParse(left.StrValue, out double leftDouble))
                    throw new KahunaScriptException("Invalid operands: " + left.Type + " + " + right.Type, ast.yyline);
                
                return new() { Type = KeyValueExpressionType.DoubleType, DoubleValue = leftDouble - right.DoubleValue };
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.LongType:
                if (!long.TryParse(left.StrValue, out long leftLong))
                    throw new KahunaScriptException("Invalid operands: " + left.Type + " + " + right.Type, ast.yyline);
                
                return new() { Type = KeyValueExpressionType.LongType, LongValue = leftLong - right.LongValue };
                
            default:
                throw new KahunaScriptException("Invalid operands: " + left.Type + " - " + right.Type, ast.yyline);
        }
    }
}