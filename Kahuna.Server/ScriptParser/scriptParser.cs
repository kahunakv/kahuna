
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Text;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Entrypoint for the Script Parser
/// </summary>
internal sealed partial class scriptParser
{
    private static readonly ConcurrentDictionary<string, ScriptCacheEntry> _cache = new();
    
    private readonly ILogger<IKahuna> logger;

    public scriptParser(ILogger<IKahuna> logger) : base(null)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Parses or retrieves a script from the global cache
    /// </summary>
    /// <param name="inputBuffer"></param>
    /// <param name="hash"></param>
    /// <returns></returns>
    /// <exception cref="KahunaScriptException"></exception>
    public NodeAst Parse(byte[] inputBuffer, string? hash)
    {
        if (!string.IsNullOrEmpty(hash) && _cache.TryGetValue(hash, out ScriptCacheEntry? cached))
        {
            logger.LogDebug("Retrieved script from cache {Hash}", hash);
            
            return cached.Ast;
        }

        MemoryStream stream = new(inputBuffer);
        scriptScanner scanner = new(stream);

        Scanner = scanner;

        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
            throw new KahunaScriptException(scanner.YYError, scanner.yylloc.StartLine);

        NodeAst? root = CurrentSemanticValue.n;

        if (!string.IsNullOrEmpty(hash))
        {
            logger.LogDebug("Added script to cache {Hash}", hash);
            
            _cache.TryAdd(hash, new(hash, root, DateTime.UtcNow.AddMinutes(10)));
        }

        return root;
    }
}