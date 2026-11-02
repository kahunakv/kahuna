
using System.Buffers;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// Evaluates equality and inequality.
///
/// Numeric comparison is exact. An earlier version compared doubles within a fixed tolerance of 0.001,
/// which made "1 == 1.0009" true and left a script no way to ask for an exact answer — a trap for counters,
/// revisions and any other value that must match precisely. A script that wants a tolerance calls the
/// 'nearly_equals' function and states its own. A long and a double also compare exactly: the long is not
/// converted to a double first, because that conversion makes 2^53 + 1 equal to 2^53.
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

        return KeyValueExpressionResult.FromBool(AreEqual(left, right, ast, operatorType));
    }

    /// <summary>
    /// Compares two values that are already evaluated, by the rules of '=='. SWITCH uses this to compare its
    /// subject, evaluated once, against each CASE value, so a CASE matches exactly when '==' would be true and
    /// fails with the same error when '==' would fail.
    /// </summary>
    public static bool AreEqual(KeyValueExpressionResult left, KeyValueExpressionResult right, NodeAst ast, string operatorType)
    {
        // A number against a number, or against a string that spells one, compares by exact value. Two
        // strings stay a string comparison, so '1' == '1.0' is false.
        if (IsNumericComparison(left.Type, right.Type))
        {
            if (!ScriptNumeric.TryReadOperands(left, right, out ScriptNumber a, out ScriptNumber b))
                throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);

            return ScriptNumeric.Compare(a, b) == 0;
        }

        switch (left.Type)
        {
            case KeyValueExpressionType.NullType when right.Type == KeyValueExpressionType.NullType:
                return true;
            
            case KeyValueExpressionType.NullType when right.Type != KeyValueExpressionType.NullType:
                return false;
            
            case KeyValueExpressionType.BoolType when right.Type == KeyValueExpressionType.BoolType:
                return left.BoolValue == right.BoolValue;
            
            case KeyValueExpressionType.StringType when right.Type == KeyValueExpressionType.StringType:
                return string.Compare(left.StrValue, right.StrValue, StringComparison.Ordinal) == 0;
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.StringType:
            {
                string str = right.StrValue ?? "";
                int byteCount = Encoding.UTF8.GetByteCount(str);
                byte[]? rented = byteCount > StackAllocThreshold ? ArrayPool<byte>.Shared.Rent(byteCount) : null;
                Span<byte> buf = rented is not null ? rented.AsSpan(0, byteCount) : stackalloc byte[byteCount];
                try
                {
                    Encoding.UTF8.GetBytes(str.AsSpan(), buf);
                    return ((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(buf);
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
                    return ((ReadOnlySpan<byte>)right.BytesValue).SequenceEqual(buf);
                }
                finally
                {
                    if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
                }
            }
            
            case KeyValueExpressionType.BytesType when right.Type == KeyValueExpressionType.BytesType:
                return ((ReadOnlySpan<byte>)left.BytesValue).SequenceEqual(right.BytesValue);
            
            default:
                
                if (right.Type == KeyValueExpressionType.NullType && left.Type != KeyValueExpressionType.NullType)
                    return false;
                
                throw new KahunaScriptException($"Invalid operands: {left.Type} {operatorType} {right.Type}", ast.yyline);
        }
    }

    private static bool IsNumericComparison(KeyValueExpressionType left, KeyValueExpressionType right)
    {
        if (!ScriptNumeric.IsNumericType(left) && !ScriptNumeric.IsNumericType(right))
            return false;

        return IsNumberOrString(left) && IsNumberOrString(right);
    }

    private static bool IsNumberOrString(KeyValueExpressionType type) =>
        type is KeyValueExpressionType.LongType or KeyValueExpressionType.DoubleType or KeyValueExpressionType.StringType;
}
