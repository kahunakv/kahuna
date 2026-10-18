using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Array literals: a bracketed, comma separated list of expressions that evaluates to a new array.
///
/// <para>A bracket that starts an expression is a literal, and a bracket that follows a complete expression
/// is an index. No statement starts with an expression, so the two forms never compete for the same
/// bracket.</para>
/// </summary>
public class TestKeyValueScriptArrayLiterals : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ScriptParserProcessor parser;

    public TestKeyValueScriptArrayLiterals(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        parser = new(new KahunaConfiguration(), kahunaLogger);
    }

    private static async Task AssertReturns(IKahuna kahuna, string script, string expected)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.True(resp.Type == KeyValueResponseType.Get, $"expected Get for \"{script}\", got {resp.Type}: {resp.Reason}");
        Assert.Equal(expected, Encoding.UTF8.GetString(resp.Value ?? []));
    }

    private static async Task AssertReturnsArray(IKahuna kahuna, string script, params string[] expected)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.True(resp.Type == KeyValueResponseType.Get, $"expected Get for \"{script}\", got {resp.Type}: {resp.Reason}");
        Assert.NotNull(resp.Values);

        string[] actual = new string[resp.Values.Count];

        for (int i = 0; i < actual.Length; i++)
            actual[i] = Encoding.UTF8.GetString(resp.Values[i].Value ?? []);

        Assert.Equal(expected, actual);
    }

    private static async Task AssertCommits(IKahuna kahuna, string script)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.True(resp.Type == KeyValueResponseType.Set, $"expected Set for \"{script}\", got {resp.Type}: {resp.Reason}");
    }

    private static async Task<string> AssertErrored(IKahuna kahuna, string script)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.True(resp.Type == KeyValueResponseType.Errored, $"expected Errored for \"{script}\", got {resp.Type}: {resp.Reason}");
        Assert.False(string.IsNullOrEmpty(resp.Reason), "an errored script must carry a reason");

        // A framework exception name in the reason means the failure escaped the script error path and the
        // caller has no line to look at.
        Assert.DoesNotContain("OutOfRangeException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("InvalidOperationException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("NullReferenceException", resp.Reason, StringComparison.Ordinal);

        return resp.Reason!;
    }

    /// <summary>
    /// The literal keeps every element in source order, an empty literal has no child, and a single element
    /// is the literal's only child with no list node around it.
    /// </summary>
    [Fact]
    public void TestLiteralParses()
    {
        NodeAst empty = parser.Parse("RETURN []");
        Assert.Equal(NodeType.ArrayLiteral, empty.leftAst!.nodeType);
        Assert.Null(empty.leftAst.leftAst);

        NodeAst single = parser.Parse("RETURN [1]");
        Assert.Equal(NodeType.IntegerType, single.leftAst!.leftAst!.nodeType);

        // The element list is a left-leaning spine, so the last element is the right child of the top node.
        NodeAst three = parser.Parse("RETURN [1, 'two', x]");
        NodeAst list = three.leftAst!.leftAst!;
        Assert.Equal(NodeType.ArrayElementList, list.nodeType);
        Assert.Equal(NodeType.Identifier, list.rightAst!.nodeType);
        Assert.Equal("two", list.leftAst!.rightAst!.yytext);
        Assert.Equal("1", list.leftAst.leftAst!.yytext);

        // A literal, then an index on it: the bracket after a complete expression is an index.
        NodeAst indexed = parser.Parse("RETURN [1, 2][0]");
        Assert.Equal(NodeType.ArrayIndex, indexed.leftAst!.nodeType);
        Assert.Equal(NodeType.ArrayLiteral, indexed.leftAst.leftAst!.nodeType);

        // A literal on the next line after a complete statement cannot start a statement of its own, so it
        // is only reachable as an index on the expression before it.
        NodeAst split = parser.Parse("LET a = b\nLET c = [1, 2]");
        Assert.Equal(NodeType.StmtList, split.nodeType);
        Assert.Equal(NodeType.Identifier, split.leftAst!.rightAst!.nodeType);
        Assert.Equal(NodeType.ArrayLiteral, split.rightAst!.rightAst!.nodeType);
    }

    /// <summary>
    /// A literal that is not closed, an element that is missing, and a trailing comma are syntax errors
    /// that name the line, the same as for the arguments of a function call.
    /// </summary>
    [Theory]
    [InlineData("LET x = [1, 2")]
    [InlineData("LET x = [,]")]
    [InlineData("LET x = [1,]")]
    [InlineData("LET x = [1, , 2]")]
    [InlineData("LET x = [, 1]")]
    [InlineData("LET x = 1, 2]")]
    public void TestMalformedLiteralIsRejected(string script)
    {
        KahunaScriptException error = Assert.Throws<KahunaScriptException>(() => parser.Parse(script));
        Assert.Contains("at line 1", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The element list is a spine with one level per element, so the depth limit that protects every
    /// recursive walker over the tree also caps the number of elements a literal can hold. A literal past
    /// the limit is a script error, not a crash.
    /// </summary>
    [Fact]
    public void TestLiteralPastTheDepthLimitIsRefused()
    {
        StringBuilder accepted = new("RETURN [0");

        for (int i = 1; i < 200; i++)
            accepted.Append(", ").Append(i);

        accepted.Append(']');

        Assert.Equal(NodeType.ArrayLiteral, parser.Parse(accepted.ToString()).leftAst!.nodeType);

        StringBuilder script = new("RETURN [0");

        for (int i = 1; i < 5000; i++)
            script.Append(", ").Append(i);

        script.Append(']');

        KahunaScriptException error = Assert.Throws<KahunaScriptException>(() => parser.Parse(script.ToString()));
        Assert.Contains("nested too deeply", error.Message, StringComparison.Ordinal);
    }

    [Theory, CombinatorialData]
    public async Task TestLiteralEvaluates([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturnsArray(kahuna1, "RETURN []");
            await AssertReturnsArray(kahuna1, "RETURN [1]", "1");
            await AssertReturnsArray(kahuna1, "RETURN [1, 2, 3]", "1", "2", "3");
            await AssertReturnsArray(kahuna1, "RETURN [\"hello\", false, 100.2, null]", "hello", "false", "100.2", "");

            await AssertReturns(kahuna1, "RETURN count([])", "0");
            await AssertReturns(kahuna1, "RETURN count([1, 2, 3])", "3");

            // Elements are full expressions, evaluated in source order against the current variables.
            await AssertReturnsArray(kahuna1, """
                LET a = 5
                RETURN [a, a * 2, a - 1, -a, upper('x'), (a + 1) * 2]
                """, "5", "10", "4", "-5", "X", "12");

            // Lower case and line breaks between the elements.
            await AssertReturnsArray(kahuna1, """
                let x = [
                  'a',
                  'b'
                ]
                return x
                """, "a", "b");

            // The documented indexing example, both on a variable and directly on the literal.
            await AssertReturns(kahuna1, """
                let items = ["a", "b", "c"]
                return items["1"]
                """, "b");

            await AssertReturns(kahuna1, "RETURN [\"a\", \"b\", \"c\"][\"1\"]", "b");
            await AssertReturns(kahuna1, "RETURN [10, 20, 30][2]", "30");

            // Nested literals are arrays of arrays, and indexing reaches into them.
            await AssertReturns(kahuna1, "RETURN count([[1], [2, 3]])", "2");
            await AssertReturns(kahuna1, "RETURN count([[1], [2, 3]][1])", "2");
            await AssertReturns(kahuna1, "RETURN [[1], [2, 3]][1][0]", "2");
            await AssertReturns(kahuna1, "RETURN is_array([[]][0])", "true");

            await AssertReturns(kahuna1, "RETURN is_array([1, 2, 3])", "true");
            await AssertReturns(kahuna1, "RETURN is_array([])", "true");

            await AssertReturns(kahuna1, "RETURN to_json([1, 2])", "[1,2]");
            await AssertReturns(kahuna1, "RETURN to_json([10, 42, 30])", "[10,42,30]");
            await AssertReturns(kahuna1, "RETURN to_json([])", "[]");
            await AssertReturns(kahuna1, "RETURN to_json(['a', true, 1.5, null])", "[\"a\",true,1.5,null]");
            await AssertReturns(kahuna1, "RETURN to_json([[1], [2, 3]])", "[[1],[2,3]]");

            // Each execution gets its own array: the tree is cached, so a shared list would leak between runs.
            await AssertReturns(kahuna1, "LET x = [1, 2] RETURN count(x)", "2");
            await AssertReturns(kahuna1, "LET x = [1, 2] RETURN count(x)", "2");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestLiteralInStatements([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // A FOR over a literal visits the elements in order: the digits come out in source order.
            await AssertReturns(kahuna1, """
                LET out = 0
                FOR x IN [3, 1, 2] DO
                  LET out = out * 10 + x
                END
                RETURN out
                """, "312");

            await AssertReturns(kahuna1, """
                LET total = 0
                FOR x IN [] DO
                  LET total = total + 1
                END
                RETURN total
                """, "0");

            // A literal feeds a write and a read back, in both key spaces.
            await AssertReturns(kahuna1, """
                LET total = 0
                FOR x IN [10, 20, 30] DO
                  LET total = total + x
                END
                SET lit_sum total
                LET v = GET lit_sum
                RETURN v
                """, "60");

            await AssertReturns(kahuna2, """
                LET names = ['a', 'b']
                ESET elit_name names[1]
                LET v = EGET elit_name
                RETURN v
                """, "b");

            // A literal as a SWITCH value and as an IF operand.
            await AssertReturns(kahuna1, """
                LET statuses = ["active", "inactive"]
                SWITCH statuses[1]
                  CASE 'active' THEN RETURN 1
                  CASE 'inactive' THEN RETURN 2
                END
                """, "2");

            await AssertReturns(kahuna1, "IF count([1, 2]) = 2 THEN RETURN 'yes' ELSE RETURN 'no' END", "yes");

            // An explicit transaction, so the literal runs on the general path too.
            await AssertCommits(kahuna1, """
                BEGIN
                  LET x = [1, 2, 3]
                  SET lit_tx x[2]
                  COMMIT
                END
                """);

            await AssertReturns(kahuna2, "GET lit_tx", "3");

            // Indexing still works on a variable and on a command result.
            await AssertReturns(kahuna1, "LET x = 1..10 RETURN x[9]", "10");
            await AssertReturns(kahuna1, "RETURN split('a,b', ',')[1]", "b");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A value of the wrong shape inside or around a literal is a script error with a reason, not a framework
    /// exception: an index past the end, and an array stored as a key's value.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestLiteralErrors([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string past = await AssertErrored(kahuna1, "RETURN [1, 2][2]");
            Assert.Contains("Index must be positive", past, StringComparison.Ordinal);

            await AssertErrored(kahuna1, "RETURN [][0]");

            await AssertErrored(kahuna1, "SET lit_arr [1, 2]");

            string syntax = await AssertErrored(kahuna1, "RETURN [1, 2");
            Assert.Contains("line 1", syntax, StringComparison.Ordinal);

            // The node still serves after every one of them.
            await AssertReturns(kahuna1, "RETURN [1][0]", "1");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The result sent back is a flat list of values, so an array inside the returned array travels as its
    /// JSON text. That conversion runs when the result is read, which can be after the commit, so it must
    /// work for every array a script can build, including one nested deeper than any call stack.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestNestedArrayResult([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturnsArray(kahuna1, "RETURN [[1], [2, 'b'], []]", "[1]", "[2,\"b\"]", "[]");
            await AssertReturnsArray(kahuna1, "RETURN [1, [true, null, 1.5]]", "1", "[true,null,1.5]");

            // A nested array as the last statement's value, with no RETURN.
            await AssertReturnsArray(kahuna1, "LET x = [[1, 2]]", "[1,2]");

            // A write, then a nested result: the write commits and the result still arrives.
            await AssertReturnsArray(kahuna1, """
                SET lit_nested 'v'
                RETURN [[1]]
                """, "[1]");

            await AssertReturns(kahuna1, "GET lit_nested", "v");

            // An array wrapped in itself once per iteration, far deeper than a recursive encoder survives.
            await AssertReturns(kahuna1, """
                LET a = []
                FOR i IN 1..20000 DO
                  LET a = [a]
                END
                RETURN count(a)
                """, "1");

            KeyValueTransactionResult deep = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes("""
                LET a = []
                FOR i IN 1..20000 DO
                  LET a = [a]
                END
                RETURN [a]
                """), null, null);

            Assert.True(deep.Type == KeyValueResponseType.Get, $"got {deep.Type}: {deep.Reason}");
            string json = Encoding.UTF8.GetString(deep.Value ?? []);
            Assert.Equal(20001 * 2, json.Length);
            Assert.StartsWith("[[[", json, StringComparison.Ordinal);

            KeyValueTransactionResult deepJson = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes("""
                LET a = []
                FOR i IN 1..20000 DO
                  LET a = [a]
                END
                RETURN to_json(a)
                """), null, null);

            Assert.True(deepJson.Type == KeyValueResponseType.Get, $"got {deepJson.Type}: {deepJson.Reason}");
            Assert.Equal(20001 * 2, (deepJson.Value ?? []).Length);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Two elements can be the same array, so a literal that holds an array twice, built in a loop, doubles
    /// its expanded size each iteration while it stays small in memory. The limit counts the expanded size,
    /// so the loop is refused at the literal, before anything writes the structure out.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestLiteralLimitCountsNestedValues([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string doubling = await AssertErrored(kahuna1, """
                LET a = [1]
                FOR i IN 1..64 DO
                  LET a = [a, a]
                END
                RETURN to_json(a)
                """);

            Assert.Contains("exceeds the limit of 100000", doubling, StringComparison.Ordinal);
            Assert.Contains("counting nested arrays", doubling, StringComparison.Ordinal);

            // A range is its own array; inside a literal its values count toward the literal's total.
            await AssertReturns(kahuna1, "RETURN count([1..99999])", "1");

            string range = await AssertErrored(kahuna1, "RETURN count([1..100000])");
            Assert.Contains("Array literal of 100001 values", range, StringComparison.Ordinal);

            // Fifteen doublings hold 3 * 2^15 - 2 = 98302 values, which is under the limit; a sixteenth is not.
            await AssertReturns(kahuna1, """
                LET a = [1]
                FOR i IN 1..15 DO
                  LET a = [a, a]
                END
                RETURN count(a)
                """, "2");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
