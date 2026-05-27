
using Kahuna.Server.Configuration;
using Microsoft.Extensions.ObjectPool;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents an object pooling policy for the <see cref="scriptParser"/> class, defining
/// creation and return behavior for pooled objects. This policy is used to optimize the
/// resource usage of <see cref="scriptParser"/> instances by reusing them rather than
/// allocating new ones for each operation.
/// </summary>
internal sealed class ScriptParserObjectPolicy : IPooledObjectPolicy<scriptParser>
{
    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;
    
    public ScriptParserObjectPolicy(KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
        this.configuration = configuration;
        this.logger = logger;
    }
    
    public scriptParser Create()
    {
        return new(configuration, logger);
    }

    public bool Return(scriptParser obj)
    {        
        return true;
    }
}