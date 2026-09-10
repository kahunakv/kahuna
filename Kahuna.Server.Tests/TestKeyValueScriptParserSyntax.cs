
using Kahuna.Server.Configuration;
using Kahuna.Server.ScriptParser;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Parser-level tests for the script language. These drive the scanner and parser directly, with no
/// cluster, so they pin down the shape of the tree and the wording of syntax errors rather than the
/// behavior of a running transaction.
/// </summary>
public sealed class TestKeyValueScriptParserSyntax
{
    private readonly ScriptParserProcessor parser;

    public TestKeyValueScriptParserSyntax(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        parser = new(new KahunaConfiguration(), loggerFactory.CreateLogger<IKahuna>());
    }

    private static int CountNodes(NodeAst? ast, NodeType nodeType)
    {
        if (ast is null)
            return 0;

        int count = ast.nodeType == nodeType ? 1 : 0;

        count += CountNodes(ast.leftAst, nodeType);
        count += CountNodes(ast.rightAst, nodeType);
        count += CountNodes(ast.extendedOne, nodeType);
        count += CountNodes(ast.extendedTwo, nodeType);
        count += CountNodes(ast.extendedThree, nodeType);
        count += CountNodes(ast.extendedFour, nodeType);

        return count;
    }

    private static NodeAst? FindNode(NodeAst? ast, NodeType nodeType)
    {
        if (ast is null)
            return null;

        if (ast.nodeType == nodeType)
            return ast;

        return FindNode(ast.leftAst, nodeType)
            ?? FindNode(ast.rightAst, nodeType)
            ?? FindNode(ast.extendedOne, nodeType)
            ?? FindNode(ast.extendedTwo, nodeType)
            ?? FindNode(ast.extendedThree, nodeType)
            ?? FindNode(ast.extendedFour, nodeType);
    }

    /// <summary>
    /// The option list reduction used to build its node from the comma token, which carries no subtree, so
    /// every option of a multi-option BEGIN was discarded and the transaction silently ran on defaults.
    /// </summary>
    [Fact]
    public void TestBeginKeepsEveryOption()
    {
        NodeAst one = parser.Parse("BEGIN (timeout=1000) SET a 1 END");
        Assert.Equal(1, CountNodes(one, NodeType.BeginOption));

        NodeAst two = parser.Parse("BEGIN (timeout=1000, locking=optimistic) SET a 1 END");
        Assert.Equal(2, CountNodes(two, NodeType.BeginOption));

        NodeAst three = parser.Parse("BEGIN (timeout=1000, locking=optimistic, autoCommit=true) SET a 1 END");
        Assert.Equal(3, CountNodes(three, NodeType.BeginOption));

        NodeAst seven = parser.Parse(
            "BEGIN (timeout=1000, locking=optimistic, autoCommit=true, asyncRelease=true, admissionWait=50, priority=high, snapshot=1) SET a 1 END");
        Assert.Equal(7, CountNodes(seven, NodeType.BeginOption));
    }

    /// <summary>
    /// A leading minus used to be folded into the numeric literal, so "5-3" lexed as two adjacent numbers
    /// and would not parse at all.
    /// </summary>
    [Fact]
    public void TestSubtractionWithoutSpacesParses()
    {
        NodeAst spaced = parser.Parse("LET x = 5 - 3");
        Assert.NotNull(FindNode(spaced, NodeType.Subtract));

        NodeAst tight = parser.Parse("LET x = 5-3");
        Assert.NotNull(FindNode(tight, NodeType.Subtract));

        NodeAst mixed = parser.Parse("LET x = 5- 3");
        Assert.NotNull(FindNode(mixed, NodeType.Subtract));
    }

    /// <summary>
    /// The grammar had no unary minus at all, so a negative value could only be written as a signed literal
    /// and a negated variable was unparseable.
    /// </summary>
    [Fact]
    public void TestUnaryMinusParses()
    {
        NodeAst literal = parser.Parse("RETURN -5");
        Assert.NotNull(FindNode(literal, NodeType.Negate));

        NodeAst variable = parser.Parse("RETURN -x");
        Assert.NotNull(FindNode(variable, NodeType.Negate));

        NodeAst call = parser.Parse("RETURN -count(x)");
        Assert.NotNull(FindNode(call, NodeType.Negate));

        // Unary minus binds tighter than multiplication, so this is (-2) * 3 and not -(2 * 3).
        NodeAst product = parser.Parse("RETURN -2 * 3");
        NodeAst? mult = FindNode(product, NodeType.Mult);
        Assert.NotNull(mult);
        Assert.Equal(NodeType.Negate, mult.leftAst!.nodeType);
    }

    /// <summary>
    /// Any character that started no pattern used to be dropped without a word, so a stray separator simply
    /// vanished and a typo surfaced as a confusing error somewhere further along.
    /// </summary>
    [Fact]
    public void TestUnknownCharacterIsReported()
    {
        KahunaScriptException semicolon = Assert.Throws<KahunaScriptException>(() => parser.Parse("SET a 1; SET b 2"));
        Assert.Contains("Unexpected character ';'", semicolon.Message, StringComparison.Ordinal);

        KahunaScriptException percent = Assert.Throws<KahunaScriptException>(() => parser.Parse("LET x = 5 % 2"));
        Assert.Contains("Unexpected character '%'", percent.Message, StringComparison.Ordinal);

        // The character is the cause and the syntax error it provokes is the symptom, so the character wins.
        Assert.DoesNotContain("TDIGIT", percent.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Newlines are now matched by a rule of their own rather than by the discard path that swallowed every
    /// unknown character, so a multi-line script must still parse and still report true line numbers.
    /// </summary>
    [Fact]
    public void TestMultiLineScriptStillParses()
    {
        NodeAst ast = parser.Parse("SET a 1\nSET b 2\nGET a");
        Assert.Equal(2, CountNodes(ast, NodeType.Set));

        NodeAst withCarriageReturns = parser.Parse("SET a 1\r\nSET b 2\r\nGET a");
        Assert.Equal(2, CountNodes(withCarriageReturns, NodeType.Set));

        KahunaScriptException error = Assert.Throws<KahunaScriptException>(() => parser.Parse("SET a 1\nSET b 2\nLET x = %"));
        Assert.Equal(3, error.Line);
    }

    /// <summary>
    /// The scanner has always accepted a hexadecimal literal; only evaluation rejected it.
    /// </summary>
    [Fact]
    public void TestHexLiteralParses()
    {
        NodeAst ast = parser.Parse("RETURN 0x1A");
        NodeAst? literal = FindNode(ast, NodeType.IntegerType);

        Assert.NotNull(literal);
        Assert.Equal("0x1A", literal.yytext);
    }
}
