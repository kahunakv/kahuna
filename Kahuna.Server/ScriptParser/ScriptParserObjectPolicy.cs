
using Kahuna.Server.Configuration;
using Microsoft.Extensions.ObjectPool;

namespace Kahuna.Server.ScriptParser;

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