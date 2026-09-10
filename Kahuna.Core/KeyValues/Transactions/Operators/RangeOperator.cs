using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal sealed class RangeOperator
{
    /// <summary>
    /// Upper bound on the elements one range may produce. The range is materialized, so without a bound a
    /// twenty-byte script asks for a multi-gigabyte allocation, and the transaction timeout cannot stop it
    /// because it is one uninterrupted statement. A hundred thousand iterations is already far past any
    /// sensible loop inside a transaction.
    /// </summary>
    private const long MaxRangeElements = 100_000;

    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid left expression", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid right expression", ast.yyline);

        KeyValueExpressionResult left = KeyValueTransactionExpression.Eval(context, ast.leftAst);
        KeyValueExpressionResult right = KeyValueTransactionExpression.Eval(context, ast.rightAst);

        if (left.Type != KeyValueExpressionType.LongType || right.Type != KeyValueExpressionType.LongType)
            throw new KahunaScriptException("Invalid operands for range operator: " + left.Type + " - " + right.Type, ast.yyline);

        long from = left.LongValue;
        long to = right.LongValue;

        // Both bounds are inclusive: "10..15" is the six values 10 through 15.
        // A start above the end is an empty range rather than an error, so "0..count-1" is a loop that runs
        // no iterations when the collection is empty, instead of aborting the transaction.
        if (from > to)
            return new(new List<KeyValueExpressionResult>());

        // The span is measured unsigned so that a range spanning the whole long domain does not overflow the
        // subtraction before the limit can reject it. The bounds are ordered, so the cast is exact.
        ulong span = (ulong)to - (ulong)from;

        if (span >= MaxRangeElements)
            throw new KahunaScriptException($"Range of {span + 1} elements exceeds the limit of {MaxRangeElements}", ast.yyline);

        int count = (int)span + 1;

        List<KeyValueExpressionResult> result = new(count);

        // Indexing off the start keeps the last addition at exactly "to", so no increment ever runs past it.
        for (int index = 0; index < count; index++)
            result.Add(new(from + index));

        return new(result);
    }
}
