
using Kahuna;
using Nixie;

Console.WriteLine("  _           _                     ");
Console.WriteLine(" | | ____ _| |__  _   _ _ __   __ _ ");
Console.WriteLine(" | |/ / _` | '_ \\| | | | '_ \\ / _` |");
Console.WriteLine(" |   < (_| | | | | |_| | | | | (_| |");
Console.WriteLine(" |_|\\_\\__,_|_| |_|\\__,_|_| |_|\\__,_|");
Console.WriteLine("");

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(new ActorSystem());
builder.Services.AddSingleton<LockManager>();

WebApplication app = builder.Build();

app.MapPost("/v1/kahuna/lock", async (ExternLockRequest request, LockManager locks) =>
{
    if (string.IsNullOrEmpty(request.LockName))
        return new();

    LockResponseType response = await locks.TryLock(request.LockName ?? "", request.LockId ?? "");

    return new ExternLockResponse { Type = response };
});

app.MapPost("/v1/kahuna/unlock", async (ExternLockRequest request, LockManager locks) =>
{
    if (string.IsNullOrEmpty(request.LockName))
        return new();

    LockResponseType response = await locks.TryUnlock(request.LockName ?? "", request.LockId ?? "");

    return new ExternLockResponse { Type = response };
});

app.MapGet("/", () => "Kahuna.Server");

app.Run("http://*:2070");

/*ActorSystem system = new();

IActorRef<LockActor, LockRequest, LockResponse> greeter = system.Spawn<LockActor, LockRequest, LockResponse>();

string n1 = Guid.NewGuid().ToString();
string n2 = Guid.NewGuid().ToString();

Console.WriteLine(n1);
Console.WriteLine(n2);

var s1 = await greeter.Ask(new LockRequest(LockRequestType.TryLock, n1));
Console.WriteLine("Resp={0}", s1!.Type);

var s2 = await greeter.Ask(new LockRequest(LockRequestType.TryLock, n2));
Console.WriteLine("Resp={0}", s2!.Type);

var s3 = await greeter.Ask(new LockRequest(LockRequestType.TryUnlock, n1));
Console.WriteLine("Resp={0}", s3!.Type);

var s4 = await greeter.Ask(new LockRequest(LockRequestType.TryLock, n2));
Console.WriteLine("Resp={0}", s4!.Type);

await system.Wait();

public enum LockRequestType
{
    TryLock,
    TryUnlock
}

public enum LockResponseType
{
    Locked,
    Busy,
    Unlocked,
    Errored
}

public sealed class LockRequest
{
    public LockRequestType Type { get; }
    
    public string? Owner { get; }
    
    public LockRequest(LockRequestType type, string? owner)
    {
        Type = type;
        Owner = owner;
    }
}

public sealed class LockResponse
{
    public LockResponseType Type { get; }
    
    public LockResponse(LockResponseType type)
    {
        Type = type;
    }
}

public sealed class LockActor : IActor<LockRequest, LockResponse>
{
    private string? owner;
    
    public LockActor(IActorContext<LockActor, LockRequest, LockResponse> _)
    {
        
    }

    public async Task<LockResponse?> Receive(LockRequest message)
    {
        Console.WriteLine("Message: {0}", message.Type);
        
        await Task.CompletedTask;
        
        switch (message.Type)
        {
            case LockRequestType.TryLock:
                return TryLock(message);
            
            case LockRequestType.TryUnlock:
                return TryUnlock(message);
        }

        return new(LockResponseType.Errored);
    }
    
    private LockResponse TryLock(LockRequest message)
    {
        if (!string.IsNullOrEmpty(owner))
            return new(LockResponseType.Busy);

        owner = message.Owner;

        return new(LockResponseType.Locked);
    }
    
    private LockResponse TryUnlock(LockRequest message)
    {
        if (string.IsNullOrEmpty(owner))
            return new(LockResponseType.Errored);

        owner = null;

        return new(LockResponseType.Unlocked);
    }
}*/
