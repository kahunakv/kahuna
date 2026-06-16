
#pragma warning disable CA1051

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