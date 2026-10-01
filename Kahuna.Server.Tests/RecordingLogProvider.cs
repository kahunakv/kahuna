using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Records every formatted log line that reaches it, with its level, so a test can assert on what a node said
/// through the same factory (and therefore the same minimum level) its output goes through. Instance-scoped:
/// unlike the process-wide meter, it sees only the loggers created from the factory it was added to.
/// </summary>
internal sealed class RecordingLogProvider : ILoggerProvider
{
    private readonly object gate = new();

    private readonly List<(LogLevel Level, string Message)> lines = [];

    /// <summary>Every recorded line containing <paramref name="fragment"/>, in order.</summary>
    public IReadOnlyList<(LogLevel Level, string Message)> Containing(string fragment)
    {
        lock (gate)
            return [.. lines.Where(line => line.Message.Contains(fragment, StringComparison.Ordinal))];
    }

    public ILogger CreateLogger(string categoryName) => new RecordingLogger(this);

    public void Dispose() { }

    private sealed class RecordingLogger(RecordingLogProvider owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            string message = formatter(state, exception);

            lock (owner.gate)
                owner.lines.Add((logLevel, message));
        }
    }
}
