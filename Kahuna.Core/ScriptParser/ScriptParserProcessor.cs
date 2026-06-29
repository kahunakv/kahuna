
using System.Buffers;
using System.Text;
using Kahuna.Server.Configuration;
using Microsoft.Extensions.ObjectPool;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Parses Kahuna Scripts
///
/// A pool of script parsers is created to avoid the overhead of creating a new parser for each script.
/// </summary>
internal sealed class ScriptParserProcessor
{
    private const int StackAllocThreshold = 4096;

    private static readonly DefaultObjectPoolProvider ScriptPoolProvider = new();

    private readonly ObjectPool<scriptParser> scriptParserPool;

    /// <summary>
    /// Processes script parsing operations using a pool of reusable script parsers.
    /// </summary>
    public ScriptParserProcessor(KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
        scriptParserPool = ScriptPoolProvider.Create(new ScriptParserObjectPolicy(configuration, logger));
    }   
    
    /// <summary>
    /// Parses a byte[] script and returns its AST
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>    
    /// <returns></returns>
    public NodeAst Parse(ReadOnlySpan<byte> script, string? hash)
    {
        scriptParser scriptParser = scriptParserPool.Get();

        try
        {
            return scriptParser.Parse(script, hash);
        }
        finally
        {
            scriptParserPool.Return(scriptParser);
        }                
    }
    
    /// <summary>
    /// Parses a string script and returns its AST
    /// </summary>
    /// <param name="script"></param>    
    /// <returns></returns>
    public NodeAst Parse(string script)
    {
        scriptParser scriptParser = scriptParserPool.Get();

        try
        {
            int byteCount = Encoding.UTF8.GetByteCount(script);
            byte[]? rented = byteCount > StackAllocThreshold ? ArrayPool<byte>.Shared.Rent(byteCount) : null;
            Span<byte> scriptBytes = rented is not null ? rented.AsSpan(0, byteCount) : stackalloc byte[byteCount];
            try
            {
                Encoding.UTF8.GetBytes(script.AsSpan(), scriptBytes);
                return scriptParser.Parse(scriptBytes, null);
            }
            finally
            {
                if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            }
        }
        finally
        {
            scriptParserPool.Return(scriptParser);
        }
    }
}