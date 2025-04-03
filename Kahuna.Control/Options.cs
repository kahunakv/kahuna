using CommandLine;

namespace Kahuna.Control;

public sealed class Options
{
    [Option('c', "connection-source", Required = false, HelpText = "Set the connection string")]
    public string? ConnectionSource { get; set; }
    
    [Option("set", Required = false, HelpText = "Executes a 'set' command")]
    public string? Set { get; set; }
    
    [Option("value", Required = false, HelpText = "Establish the parameter 'value'")]
    public string? Value { get; set; }
}