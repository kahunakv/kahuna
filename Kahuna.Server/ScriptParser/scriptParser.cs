
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.Configuration;
using Microsoft.IO;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Entrypoint for the Script Parser
/// </summary>
internal sealed partial class scriptParser
{
    private static readonly RecyclableMemoryStreamManager manager = new();

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;
    
    public static ConcurrentDictionary<string, ScriptCacheEntry> Cache { get; } = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public scriptParser(KahunaConfiguration configuration, ILogger<IKahuna> logger) : base(null)
    {
        this.configuration = configuration;
        this.logger = logger;
    }

    /// <summary>
    /// Parses or retrieves a script from the global cache
    /// </summary>
    /// <param name="inputBuffer"></param>
    /// <param name="hash"></param>
    /// <returns></returns>
    /// <exception cref="KahunaScriptException"></exception>
    public NodeAst Parse(ReadOnlySpan<byte> inputBuffer, string? hash)
    {
        if (!string.IsNullOrEmpty(hash) && Cache.TryGetValue(hash, out ScriptCacheEntry? cached))
        {
            cached.Expiration = DateTime.UtcNow + configuration.ScriptCacheExpiration;
            
            logger.LogDebug("Retrieved script from cache {Hash}", hash);
            
            return cached.Ast;
        }
        
        using RecyclableMemoryStream stream = manager.GetStream(inputBuffer); 
        
        scriptScanner scanner = new(stream);

        Scanner = scanner;
        
        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
            throw new KahunaScriptException(scanner.YYError + " at line " + scanner.yylloc.EndLine, scanner.yylloc.StartLine);

        NodeAst? root = CurrentSemanticValue.n;

        if (!string.IsNullOrEmpty(hash))
        {
            logger.LogDebug("Added script to cache {Hash}", hash);
            
            Cache.TryAdd(hash, new(hash, root, DateTime.UtcNow + configuration.ScriptCacheExpiration));
        }

        return root;
    }
}