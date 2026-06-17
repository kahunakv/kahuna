/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander;

namespace Kahuna.Server.Diagnostics.Logging;

public static partial class KahunaLoggerDiagnosticsExtensions
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Thread statistics at startup: minimum worker:{Workers} io:{IO}")]
    public static partial void LogThreadStatsMinimum(this ILogger<IRaft> logger, int workers, int iO);

    [LoggerMessage(Level = LogLevel.Information, Message = "Thread statistics at startup: maximum worker:{Workers} io:{IO}")]
    public static partial void LogThreadStatsMaximum(this ILogger<IRaft> logger, int workers, int iO);

    [LoggerMessage(Level = LogLevel.Information, Message = "Thread statistics at startup: available worker:{Workers} io:{IO}")]
    public static partial void LogThreadStatsAvailable(this ILogger<IRaft> logger, int workers, int iO);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Thread statistics: active worker:{Wokers} io:{Io}")]
    public static partial void LogThreadStatsActive(this ILogger<IRaft> logger, int wokers, int io);
}
