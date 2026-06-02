using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

internal static class TestLogFactory
{
    private const string MinimumLevelEnvironmentVariable = "KAHUNA_TEST_LOG_LEVEL";

    public static ILoggerFactory Create(ITestOutputHelper outputHelper, LogLevel defaultMinimumLevel = LogLevel.Debug, bool quietKommander = false)
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
