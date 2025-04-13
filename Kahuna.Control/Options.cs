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
    
    [Option("extend", Required = false, HelpText = "Executes a 'extend' command")]
    public string? Extend { get; set; }
    
    [Option("value", Required = false, HelpText = "Establish the parameter 'value'")]
    public string? Value { get; set; }
    
    [Option("get-by-prefix", Required = false, HelpText = "Executes a 'get-by-prefix' command")]
    public string? GetByPrefix { get; set; }
    
    [Option("scan-by-prefix", Required = false, HelpText = "Executes a 'scan-by-prefix' command")]
    public string? ScanByPrefix { get; set; }
    
    [Option("expires", Required = false, HelpText = "Defines the 'expires' parameter")]
    public int Expires { get; set; }
    
    [Option("lock", Required = false, HelpText = "Acquires a lock by the given name")]
    public string? Lock { get; set; }
    
    [Option("unlock", Required = false, HelpText = "Unlocks a lock by the given name")]
    public string? Unlock { get; set; }
    
    [Option("extend-lock", Required = false, HelpText = "Extends the lock by the given name")]
    public string? ExtendLock { get; set; }
    
    [Option("owner", Required = false, HelpText = "References the current owner of a lock")]
    public string? Owner { get; set; }
}