
namespace Kahuna.Server.ScriptParser;

public sealed class NodeAst
{
    public readonly NodeType nodeType;

    public readonly NodeAst? leftAst;

    public readonly NodeAst? rightAst;

    public NodeAst? extendedOne;

    public NodeAst? extendedTwo;

    public NodeAst? extendedThree;

    public NodeAst? extendedFour;

    public readonly string? yytext;

    public NodeAst(
        NodeType nodeType,
        NodeAst? leftAst,
        NodeAst? rightAst,
        NodeAst? extendedOne,
        NodeAst? extendedTwo,
        NodeAst? extendedThree,
        NodeAst? extendedFour,
        string? yytext
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

        //if (leftAst is not null)
        //	Console.WriteLine("left={0}/{1}", leftAst.nodeType, leftAst.yytext);

        //if (rightAst is not null)
        //Console.WriteLine("right={0}/{1}", rightAst.nodeType, rightAst.yytext);

        //if (!string.IsNullOrEmpty(yytext))		
        //	Console.WriteLine("{0}: {1}", nodeType, yytext);
    }
}