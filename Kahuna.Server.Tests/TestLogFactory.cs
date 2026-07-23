using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

internal static class TestLogFactory
{
    private const string MinimumLevelEnvironmentVariable = "KAHUNA_TEST_LOG_LEVEL";

    // Default to Warning: piping Debug/Info-level logs (every actor message, Raft heartbeat, etc.) into
    // xUnit's ITestOutputHelper buffers all of it in memory for the whole run, which accumulates to multiple
    // GB across a large suite and OOM-kills the test host ("error while writing to logger(s)"). Warning keeps
    // the output (and memory) small. Set KAHUNA_TEST_LOG_LEVEL=Debug (or Information/Trace) to opt back in when
    // diagnosing a specific test.
    public static ILoggerFactory Create(ITestOutputHelper outputHelper, LogLevel defaultMinimumLevel = LogLevel.Warning, bool quietKommander = false)
    {
        LogLevel minimumLevel = GetMinimumLevel(defaultMinimumLevel);

        return LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .SetMinimumLevel(minimumLevel);

            if (quietKommander)
                builder.AddFilter("Kommander", Max(minimumLevel, LogLevel.Warning));
        });
    }

    private static LogLevel GetMinimumLevel(LogLevel defaultMinimumLevel)
    {
        string? configuredLevel = Environment.GetEnvironmentVariable(MinimumLevelEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(configuredLevel))
            return defaultMinimumLevel;

        return Enum.TryParse(configuredLevel, ignoreCase: true, out LogLevel parsedLevel)
            ? parsedLevel
            : defaultMinimumLevel;
    }

    private static LogLevel Max(LogLevel left, LogLevel right) => left > right ? left : right;
}
