
using Nixie;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents an actor responsible for evicting expired script parser entries from the script cache.
/// </summary>
public sealed class ScriptParserEvicterActor : IActor<ScriptParserEvicterRequest>
{
    private readonly ILogger<IKahuna> logger;
    
    public ScriptParserEvicterActor(IActorContext<ScriptParserEvicterActor, ScriptParserEvicterRequest> context, ILogger<IKahuna> logger)
    {
        this.logger = logger;
        
        context.ActorSystem.StartPeriodicTimer(
            context.Self, 
            "evict-script-parser",
            new(),
            TimeSpan.FromSeconds(300),
            TimeSpan.FromSeconds(300)
        );//throw new NotImplementedException();
    }
    
    public Task Receive(ScriptParserEvicterRequest message)
    {
        DateTime now = DateTime.UtcNow;

        foreach (KeyValuePair<string, ScriptCacheEntry> xv in scriptParser.Cache)
        {
            if (xv.Value.Expiration < now)
            {
                if (scriptParser.Cache.TryRemove(xv.Key, out _))
                    logger.LogDebug("Removed {Key} from script cache", xv.Key);
            }
        }
        
        return Task.CompletedTask;
    }
}