using CommandLine;

namespace Kahuna.Control;

public sealed class Options
{
    [Option('c', "connection-source", Required = false, HelpText = "Set the connection string")]
    public string? ConnectionSource { get; set; }
    
    [Option("format", Required = false, HelpText = "Defines the output format (console, json)")]
    public string? Format { get; set; }
    
    [Option("set", Required = false, HelpText = "Executes a 'set' command")]
    public string? Set { get; set; }
    
    [Option("get", Required = false, HelpText = "Executes a 'get' command")]
    public string? Get { get; set; }
    
    [Option("value", Required = false, HelpText = "Establish the parameter 'value'")]
    public string? Value { get; set; }
    
    [Option("lock", Required = false, HelpText = "Acquires a lock by the given name")]
    public string? Lock { get; set; }
    
    [Option("unlock", Required = false, HelpText = "Unlocks a lock by the given name")]
    public string? Unlock { get; set; }
    
    [Option("extend-lock", Required = false, HelpText = "Extends the lock by the given name")]
    public string? ExtendLock { get; set; }
}