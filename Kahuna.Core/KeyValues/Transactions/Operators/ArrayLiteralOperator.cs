using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class ArrayLiteralOperator
{
    /// <summary>
    /// Builds a new array from a bracketed list of expressions, evaluating the elements in source order.
    ///
    /// <para>Every evaluation returns a fresh list, because the tree is cached and shared by every execution
    /// of the script; handing out one shared list would let one execution see another's array.</para>
    ///
    /// <para>The number of elements is bounded by the script source, but the values they hold are not: an
    /// element can be an array, and two elements can be the same array. The limit therefore applies to the
    /// total number of values, counting every value of every nested array, so a loop that wraps an array in
    /// itself is refused here, while it is still small, instead of when something expands it.</para>
    /// </summary>
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        NodeAst[] elements = GetElements(ast);

        List<KeyValueExpressionResult> values = new(elements.Length);

        for (int index = 0; index < elements.Length; index++)
            values.Add(KeyValueTransactionExpression.Eval(context, elements[index]));

        KeyValueExpressionResult result = new(values);

        if (result.TotalValueCount > ScriptArrayLimits.MaxElements)
            throw new KahunaScriptException(
                $"Array literal of {result.TotalValueCount} values, counting nested arrays, exceeds the limit of {ScriptArrayLimits.MaxElements}",
                ast.yyline);

        return result;
    }

    /// <summary>
    /// The element expressions of a literal in source order.
    ///
    /// <para>The element list is a left-leaning spine: each list node holds the elements before it on the
    /// left and one element on the right, and the first element is the leftmost leaf. A literal that holds
    /// one element has no list node at all, and an empty literal has no child. The spine is walked in a loop,
    /// so a long literal costs no recursion depth.</para>
    ///
    /// <para>Kept on the literal's node, because the parsed tree is cached and its shape never changes.
    /// Two concurrent executions can both build the array; both build the same one from the same tree, and
    /// the store is a single reference write, so the race is harmless. Nothing writes to the array after it
    /// is published.</para>
    /// </summary>
    private static NodeAst[] GetElements(NodeAst ast)
    {
        NodeAst[]? elements = Volatile.Read(ref ast.orderedChildrenMemo);

        if (elements is not null)
            return elements;

        NodeAst? list = ast.leftAst;

        if (list is null)
        {
            elements = [];
        }
        else
        {
            int count = 1;

            for (NodeAst node = list; node.nodeType == NodeType.ArrayElementList; node = node.leftAst!)
                count++;

            elements = new NodeAst[count];

            NodeAst current = list;

            for (int index = count - 1; index > 0; index--)
            {
                elements[index] = current.rightAst!;
                current = current.leftAst!;
            }

            elements[0] = current;
        }

        Volatile.Write(ref ast.orderedChildrenMemo, elements);

        return elements;
    }
}
