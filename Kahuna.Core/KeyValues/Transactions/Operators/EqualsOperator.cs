
using System.Buffers;
using System.Text;
using System.Globalization;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Evaluates equality and inequality.
///
/// Numeric comparison is exact. An earlier version compared doubles within a fixed tolerance of 0.001,
/// which made "1 == 1.0009" true and left a script no way to ask for an exact answer — a trap for counters,
/// revisions and any other value that must match precisely. A script that wants a tolerance calls the
/// 'nearly_equals' function and states its own.
/// </summary>
internal static class EqualsOperator
{
    private const int StackAllocThreshold = 256;

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
            case KeyValueExpressionType.NullType when right.Type == KeyValueExpressionType.NullType:
                return KeyValueExpressionResult.FromBool(true);
            
            case KeyValueExpressionType.NullType when right.Type != KeyValueExpressionType.NullType:
                return KeyValueExpressionResult.FromBool(false);
            
            case KeyValueExpressionType.BoolType when right.Type == KeyValueExpressionType.BoolType:
                return KeyValueExpressionResult.FromBool(left.BoolValue == right.BoolValue);
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.StringType:
                return KeyValueExpressionResult.FromBool(string.Compare(left.StrValue, right.StrValue, StringComparison.Ordinal) == 0);
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.LongType:
                return KeyValueExpressionResult.FromBool(left.LongValue == right.LongValue);
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.DoubleType:
                return KeyValueExpressionResult.FromBool(left.DoubleValue == right.DoubleValue);
            
            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.DoubleType:
                return KeyValueExpressionResult.FromBool(left.LongValue == right.DoubleValue);
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.LongType:
                return KeyValueExpressionResult.FromBool(left.DoubleValue == right.LongValue);
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.StringType:
            {
                string str = right.StrValue ?? "";
                int byteCount = Encoding.UTF8.GetByteCount(str);
                byte[]? rented = byteCount > StackAllocThreshold ? ArrayPool<byte>.Shared.Rent(byteCount) : null;
                Span<byte> buf = rented is not null ? rented.AsSpan(0, byteCount) : stackalloc byte[byteCount];
                try
                {
                    Encoding.UTF8.GetBytes(str.AsSpan(), buf);
                    return KeyValueExpressionResult.FromBool(((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(buf));
                }
                finally
                {
                    if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
                }
            }

            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.BytesType:
            {
                string str = left.StrValue ?? "";
                int byteCount = Encoding.UTF8.GetByteCount(str);
                byte[]? rented = byteCount > StackAllocThreshold ? ArrayPool<byte>.Shared.Rent(byteCount) : null;
                Span<byte> buf = rented is not null ? rented.AsSpan(0, byteCount) : stackalloc byte[byteCount];
                try
                {
                    Encoding.UTF8.GetBytes(str.AsSpan(), buf);
                    return KeyValueExpressionResult.FromBool(((ReadOnlySpan<byte>)right.BytesValue).SequenceEqual(buf));
                }
                finally
                {
                    if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
                }
            }
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.BytesType:
                return KeyValueExpressionResult.FromBool(((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(right.BytesValue));
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.DoubleType:
            {
                if (!long.TryParse(left.StrValue, out long leftLong))
                {
                    if (!double.TryParse(left.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double leftDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " == " + right.Type, ast.yyline);
                
                    return KeyValueExpressionResult.FromBool(leftDouble == right.DoubleValue);
                }

                return KeyValueExpressionResult.FromBool(leftLong == right.DoubleValue);
            }

            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.LongType:
            {
                if (!long.TryParse(left.StrValue, out long leftLong))
                {
                    if (!double.TryParse(left.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double leftDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " == " + right.Type, ast.yyline);
                
                    return KeyValueExpressionResult.FromBool(leftDouble == right.LongValue);
                }

                return KeyValueExpressionResult.FromBool(leftLong == right.LongValue);
            }

            case KeyValueExpressionType.LongType when right.Type == KeyValueExpressionType.StringType:
            {
                if (!long.TryParse(right.StrValue, out long rightLong))
                {
                    if (!double.TryParse(right.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double rightDouble))                    
                        throw new KahunaScriptException("Invalid operands: " + left.Type + " == " + right.Type, ast.yyline);
                    
                    return KeyValueExpressionResult.FromBool(left.LongValue == rightDouble);
                }

                return KeyValueExpressionResult.FromBool(left.LongValue == rightLong);
            }
            
            case KeyValueExpressionType.DoubleType when right.Type == KeyValueExpressionType.StringType:
            {
                if (!long.TryParse(right.StrValue, out long rightLong))
                {
                    if (!double.TryParse(right.StrValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double rightDouble))                    
                        throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
                    
                    return KeyValueExpressionResult.FromBool(left.DoubleValue == rightDouble);
                }

                return KeyValueExpressionResult.FromBool(left.DoubleValue == rightLong);
            }

            default:
                
                if (right.Type == KeyValueExpressionType.NullType && left.Type != KeyValueExpressionType.NullType)
                    return KeyValueExpressionResult.FromBool(false);
                
                throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
        }
    }
}
