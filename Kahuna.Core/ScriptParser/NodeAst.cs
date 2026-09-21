
#pragma warning disable CA1051

using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents a node within an abstract syntax tree (AST).
/// This class is used during script parsing and evaluation,
/// encapsulating the structure of the parsed script and its components.
/// </summary>
public sealed class NodeAst
{
    public readonly NodeType nodeType;

    public readonly NodeAst? leftAst;

    public readonly NodeAst? rightAst;

    public readonly NodeAst? extendedOne;

    public readonly NodeAst? extendedTwo;

    public readonly NodeAst? extendedThree;

    public readonly NodeAst? extendedFour;

    public readonly string? yytext;

    public readonly int yyline;

    /// <summary>
    /// The value of this node's literal, once something has asked for it.
    ///
    /// <para>A parsed tree is cached and shared by every later execution of the same script, and a
    /// literal's text never changes, so re-reading <see cref="yytext"/> into a number and wrapping the
    /// number again on every evaluation repeats work whose answer is fixed. The wrapper is immutable, so
    /// one instance serves every execution. Inside a loop this is the difference between one object per
    /// literal and one object per literal per iteration.</para>
    ///
    /// <para>Filled on first use rather than at parse time, because a literal in a branch that is not
    /// taken is never evaluated: parsing it early would turn an out-of-range literal in dead code into a
    /// script error. Two concurrent executions of the same tree can both fill it; both compute the same
    /// value from the same text, and the store is a single reference write, so the race is harmless. The
    /// field is written and read through <see cref="System.Threading.Volatile"/> so a reader cannot
    /// observe the reference before the object it points at.</para>
    ///
    /// <para>Owned by the expression evaluator, which is the only thing that knows how to read a literal
    /// of each type. Nothing else may write it.</para>
    /// </summary>
    internal KeyValueExpressionResult? literalMemo;

    /// <summary>
    /// The value of this node read as a revision number, once something has asked for it.
    ///
    /// <para>Separate from <see cref="literalMemo"/> because the two are read by different rules: a
    /// revision is a plain decimal integer, while an expression literal also accepts the hexadecimal form
    /// the scanner allows. One field serving both would make a node's meaning depend on which consumer
    /// reached it first.</para>
    /// </summary>
    internal KeyValueExpressionResult? revisionMemo;

    /// <summary>
    /// The statements of this statement list in execution order, once an execution has asked for them.
    ///
    /// <para>A statement list is a left-leaning spine with one node per statement. The executor runs a
    /// list as a flat loop, and the spine's shape is fixed at parse time, so the flattened form is built
    /// once per cached tree instead of once per execution. Set only on the root node of a list. The same
    /// publication rule as <see cref="literalMemo"/> applies: concurrent fills compute the same array,
    /// and the array is never written after it is stored.</para>
    /// </summary>
    internal NodeAst[]? statementsMemo;

    public NodeAst(
        NodeType nodeType,
        NodeAst? leftAst,
        NodeAst? rightAst,
        NodeAst? extendedOne,
        NodeAst? extendedTwo,
        NodeAst? extendedThree,
        NodeAst? extendedFour,
        string? yytext,
        int yyline
    )
    {
        this.nodeType = nodeType;
        this.leftAst = leftAst;
        this.rightAst = rightAst;
        this.extendedOne = extendedOne;
        this.extendedTwo = extendedTwo;
        this.extendedThree = extendedThree;
        this.extendedFour = extendedFour;
        this.yytext = yytext;
        this.yyline = yyline;

    }
}

#pragma warning restore CA1051