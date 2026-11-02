using System.Globalization;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// A number read from a script operand: a long, or a finite double.
/// </summary>
internal readonly struct ScriptNumber
{
    public readonly bool IsLong;

    public readonly long Long;

    public readonly double Double;

    public ScriptNumber(long value)
    {
        IsLong = true;
        Long = value;
        Double = 0;
    }

    public ScriptNumber(double value)
    {
        IsLong = false;
        Long = 0;
        Double = value;
    }

    /// <summary>The value as a double. A long above 2^53 loses its low bits here, as any conversion does.</summary>
    public double AsDouble => IsLong ? Long : Double;

    public bool IsZero => IsLong ? Long == 0 : Double == 0;

    public KeyValueExpressionResult ToResult() => IsLong ? new(Long) : new(Double);

    public override string ToString() => IsLong
        ? Long.ToString(CultureInfo.InvariantCulture)
        : Double.ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// The numeric rules that every operator and math function of the script language shares.
///
/// <para>A script number is a long or a finite double, and a result is either the mathematically
/// correct value or a script error that names the line:</para>
/// <list type="bullet">
/// <item>Long arithmetic that leaves the long range is an error. It never wraps, and it never changes
/// into a double, because a counter or a revision must not lose precision without notice.</item>
/// <item>A double that is not finite (NaN, or an infinity) is never a script value. An operation that
/// would give one is an error, and a string that spells one is not a numeric string. Thus a script
/// cannot store such a value, and NaN can never make a comparison false on both sides.</item>
/// <item>A long and a double compare by their exact values. Converting the long to a double first
/// would make 2^53 + 1 equal to 2^53.</item>
/// <item>A numeric string is parsed with the invariant culture, so the result does not depend on the
/// culture of the server.</item>
/// </list>
///
/// <para>The checks do not throw on the success path and allocate nothing; a message is built only
/// when the script fails.</para>
/// </summary>
internal static class ScriptNumeric
{
    /// <summary>2^63, the smallest double above long.MaxValue. (double)long.MaxValue rounds up to it.</summary>
    private const double TwoPow63 = 9223372036854775808.0;

    /// <summary>
    /// Reads a long or a double operand as itself, and a string operand as the number it spells.
    /// Anything else is not a number.
    /// </summary>
    public static bool TryRead(KeyValueExpressionResult value, out ScriptNumber number)
    {
        switch (value.Type)
        {
            case KeyValueExpressionType.LongType:
                number = new(value.LongValue);
                return true;

            case KeyValueExpressionType.DoubleType:
                number = new(value.DoubleValue);
                return true;

            case KeyValueExpressionType.StringType:
                return TryParse(value.StrValue, out number);

            default:
                number = default;
                return false;
        }
    }

    /// <summary>
    /// Reads the two operands of a binary numeric operator. At least one must be a long or a double, and
    /// the other may also be a string that spells a number. Two strings are not a numeric operation, so
    /// '1' + '2' stays an error rather than becoming 3.
    /// </summary>
    public static bool TryReadOperands(
        KeyValueExpressionResult left,
        KeyValueExpressionResult right,
        out ScriptNumber leftNumber,
        out ScriptNumber rightNumber
    )
    {
        rightNumber = default;

        if (!IsNumericType(left.Type) && !IsNumericType(right.Type))
        {
            leftNumber = default;
            return false;
        }

        return TryRead(left, out leftNumber) && TryRead(right, out rightNumber);
    }

    public static bool IsNumericType(KeyValueExpressionType type) =>
        type is KeyValueExpressionType.LongType or KeyValueExpressionType.DoubleType;

    /// <summary>
    /// Parses a numeric string with the invariant culture: as a long when it spells one, otherwise as a
    /// finite double. "NaN", "Infinity" and a value too large for a double are not numbers.
    /// </summary>
    public static bool TryParse(string? text, out ScriptNumber number)
    {
        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long asLong))
        {
            number = new(asLong);
            return true;
        }

        if (TryParseFiniteDouble(text, out double asDouble))
        {
            number = new(asDouble);
            return true;
        }

        number = default;
        return false;
    }

    /// <summary>Parses a finite double with the invariant culture.</summary>
    public static bool TryParseFiniteDouble(string? text, out double value)
    {
        return double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value) && double.IsFinite(value);
    }

    public static long Add(long left, long right, NodeAst ast)
    {
        long result = unchecked(left + right);

        // The sum overflowed when both operands have the same sign and the result has the other one.
        if (((left ^ result) & (right ^ result)) < 0)
            throw Overflow(left, "+", right, ast);

        return result;
    }

    public static long Subtract(long left, long right, NodeAst ast)
    {
        long result = unchecked(left - right);

        // The difference overflowed when the operands have different signs and the result does not have
        // the sign of the left operand.
        if (((left ^ right) & (left ^ result)) < 0)
            throw Overflow(left, "-", right, ast);

        return result;
    }

    public static long Multiply(long left, long right, NodeAst ast)
    {
        long high = Math.BigMul(left, right, out long low);

        // The 128-bit product fits in a long only when the high half is the sign extension of the low half.
        if (high != (low >> 63))
            throw Overflow(left, "*", right, ast);

        return low;
    }

    /// <summary>Integer division, truncated toward zero. The caller has already refused a zero divisor.</summary>
    public static long Divide(long left, long right, NodeAst ast)
    {
        // long.MinValue / -1 is the one quotient outside the range.
        if (left == long.MinValue && right == -1)
            throw Overflow(left, "/", right, ast);

        return left / right;
    }

    public static long Negate(long value, NodeAst ast)
    {
        // long.MinValue has no positive counterpart.
        if (value == long.MinValue)
            throw new KahunaScriptException("Cannot negate " + long.MinValue.ToString(CultureInfo.InvariantCulture), ast.yyline);

        return -value;
    }

    public static long Abs(long value, NodeAst ast)
    {
        if (value == long.MinValue)
            throw new KahunaScriptException("Integer overflow: abs(" + long.MinValue.ToString(CultureInfo.InvariantCulture) + ")", ast.yyline);

        return value < 0 ? -value : value;
    }

    /// <summary>Returns the result of a double operation, or refuses one that is not finite.</summary>
    public static double RequireFinite(double result, ScriptNumber left, string operatorText, ScriptNumber right, NodeAst ast)
    {
        if (double.IsFinite(result))
            return result;

        throw new KahunaScriptException(
            "Result is not a finite number: " + left + " " + operatorText + " " + right,
            ast.yyline
        );
    }

    /// <summary>Returns the result of a math function, or refuses one that is not finite.</summary>
    public static double RequireFiniteResult(double result, string function, NodeAst ast)
    {
        if (double.IsFinite(result))
            return result;

        throw new KahunaScriptException("Result of '" + function + "' function is not a finite number", ast.yyline);
    }

    /// <summary>
    /// Compares two numbers by their exact values: negative when left is smaller, zero when they are equal,
    /// positive when left is larger.
    /// </summary>
    public static int Compare(ScriptNumber left, ScriptNumber right)
    {
        if (left.IsLong && right.IsLong)
            return left.Long.CompareTo(right.Long);

        if (left.IsLong)
            return CompareLongToDouble(left.Long, right.Double);

        if (right.IsLong)
            return -CompareLongToDouble(right.Long, left.Double);

        // Neither value is NaN, so the plain operators give a total order, and 0.0 equals -0.0.
        if (left.Double < right.Double)
            return -1;

        return left.Double > right.Double ? 1 : 0;
    }

    /// <summary>
    /// Compares a long with a finite double exactly, without converting the long to a double.
    /// </summary>
    public static int CompareLongToDouble(long value, double other)
    {
        // Every long is below 2^63 and at or above -2^63.
        if (other >= TwoPow63)
            return -1;

        if (other < -TwoPow63)
            return 1;

        // The floor is a whole number in [-2^63, 2^63), so its conversion to long is exact.
        double floor = Math.Floor(other);
        long whole = (long)floor;

        if (value != whole)
            return value < whole ? -1 : 1;

        // The long equals the whole part. A fractional part makes the double the larger one.
        return other > floor ? -1 : 0;
    }

    /// <summary>
    /// Converts a finite double to a long by truncation toward zero, or reports that it is outside the long
    /// range. A plain cast would saturate the out-of-range value to long.MinValue or long.MaxValue.
    /// </summary>
    public static bool TryTruncateToLong(double value, out long result)
    {
        // The negated form also refuses NaN.
        if (!(value >= -TwoPow63 && value < TwoPow63))
        {
            result = 0;
            return false;
        }

        result = (long)value;
        return true;
    }

    private static KahunaScriptException Overflow(long left, string operatorText, long right, NodeAst ast)
    {
        return new(
            "Integer overflow: " + left.ToString(CultureInfo.InvariantCulture) + " " + operatorText + " " + right.ToString(CultureInfo.InvariantCulture),
            ast.yyline
        );
    }
}
