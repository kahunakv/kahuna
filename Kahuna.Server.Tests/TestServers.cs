using Kahuna.Server.Configuration;
using Kahuna.Server.ScriptParser;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

public class TestServers
{
    [Fact]
    public void TestInvalidScriptReportsLineAndColumn()
    {
        ScriptParserProcessor parser = new(new KahunaConfiguration(), NullLogger<IKahuna>.Instance);

        KahunaScriptException ex = Assert.Throws<KahunaScriptException>(() =>
            parser.Parse("SET pp 'hello world'\nBROKEN command"));

        Assert.Equal(2, ex.Line);
        Assert.Equal(1, ex.Column);
        Assert.Contains("line 2, column 1", ex.Message);
        Assert.Contains("BROKEN", ex.Message);
        Assert.DoesNotContain("line 0", ex.Message);
    }

    /*[Fact]
    public void TestParserBegin()
    {
        NodeAst ast = ScriptParserProcessor.Parse("BEGIN LET x = GET yy END");
        
        Assert.Equal(NodeType.Begin, ast.nodeType);
        
        Assert.NotNull(ast.leftAst);
        Assert.Equal(NodeType.Get, ast.leftAst.nodeType);
    }
    
    [Fact]
    public void TestParserReturn()
    {
        NodeAst ast = ScriptParserProcessor.Parse("RETURN p");
        
        Assert.Equal(NodeType.Return, ast.nodeType);
        
        Assert.NotNull(ast.leftAst);
        Assert.Equal(NodeType.Identifier, ast.leftAst.nodeType);
    }*/
    
    
}
