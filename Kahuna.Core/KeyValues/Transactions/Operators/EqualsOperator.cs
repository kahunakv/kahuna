
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class EqualsOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast, string operatorType)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);
                
        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);
        
        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);

        switch (left.Type)
        {
            case KeyValueExpressionType.NullType when right.Type == KeyValueExpressionType.NullType:
                return new(true);
            
            case KeyValueExpressionType.NullType when right.Type != KeyValueExpressionType.NullType:
                return new(false);
            
            case KeyValueExpressionType.BoolType when right.Type == KeyValueExpressionType.BoolType:
                return new(left.BoolValue == right.BoolValue);
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.StringType:
                return new(string.Compare(left.StrValue, right.StrValue, StringComparison.Ordinal) == 0);
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return new(left.LongValue == right.LongValue);
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return new(Math.Abs(left.DoubleValue - right.DoubleValue) <= 0.001);
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return new(Math.Abs(left.LongValue - right.DoubleValue) <= 0.001);
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return new(Math.Abs(left.DoubleValue - right.LongValue) <= 0.001);
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.StringType:
                Span<byte> rightBytes = stackalloc byte[Encoding.UTF8.GetByteCount(right.StrValue ?? "")];
                Encoding.UTF8.GetBytes(left.StrValue.AsSpan(), rightBytes);
                return new(((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(rightBytes));
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.BytesType:
                Span<byte> leftBytes = stackalloc byte[Encoding.UTF8.GetByteCount(left.StrValue ?? "")];
                Encoding.UTF8.GetBytes(left.StrValue.AsSpan(), leftBytes);
                return new(((ReadOnlySpan<byte>)right.BytesValue).SequenceEqual(leftBytes));
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.BytesType:
                return new(((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(right.BytesValue));
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.DoubleType:
            {
                if (!long.TryParse(left.StrValue, out long leftLong))
                {
                    if (!double.TryParse(left.StrValue, out double leftDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " == " + right.Type, ast.yyline);
                
                    return new(Math.Abs(leftDouble - right.DoubleValue) <= 0.001);
                }

                return new(Math.Abs(leftLong - right.DoubleValue) <= 0.001);
            }

            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.LongType:
            {
                if (!long.TryParse(left.StrValue, out long leftLong))
                {
                    if (!double.TryParse(left.StrValue, out double leftDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " == " + right.Type, ast.yyline);
                
                    return new(Math.Abs(leftDouble - right.LongValue) <= 0.001);
                }

                return new(leftLong == right.LongValue);
            }

            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.StringType:
            {
                if (!long.TryParse(right.StrValue, out long rightLong))
                {
                    if (!double.TryParse(right.StrValue, out double rightDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " == " + right.Type, ast.yyline);
                    
                    return new(Math.Abs(left.LongValue - rightDouble) <= 0.001);
                }

                return new(left.LongValue == rightLong);
            }
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.StringType:
            {
                if (!long.TryParse(right.StrValue, out long rightLong))
                {
                    if (!double.TryParse(right.StrValue, out double rightDouble))                    
                        throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
                    
                    return new(Math.Abs(left.DoubleValue - rightDouble) <= 0.001);
                }

                return new(Math.Abs(left.DoubleValue - rightLong) <= 0.001);
            }

            default:
                
                if (right.Type == KeyValueExpressionType.NullType && left.Type != KeyValueExpressionType.NullType)
                    return new(false);
                
                throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
        }
    }
}