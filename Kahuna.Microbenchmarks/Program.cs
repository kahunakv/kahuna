using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

// Required so the top-level statements file has a Program type for FromAssembly.
public partial class Program;
