using CommandLine;

namespace Kahuna;

public sealed class KahunaCommandLineOptions
{
    [Option('h', "host", Required = false, HelpText = "Host to bind incoming connections to")]
    public string? Host { get; set; }
    
    [Option('p', "port", Required = false, HelpText = "Port to bind incoming connections to")]
    public int Port { get; set; }
}