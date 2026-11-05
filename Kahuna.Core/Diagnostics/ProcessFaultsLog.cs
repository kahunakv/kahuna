
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Diagnostics;

/// <summary>Source-generated log messages of the process-fault policy and the out-of-memory restart breaker.</summary>
internal static partial class ProcessFaultsLog
{
    [LoggerMessage(Level = LogLevel.Critical, Message = "Node {Endpoint} tripped its out-of-memory restart breaker: {Run} consecutive recovery exits — {Reason}. The node restores its partitions and keeps voting, but withholds its candidacy for every partition it hosts and reports not ready until its memory limit is raised, what it retains is reduced (the durable-2PC retention floor and budget, the snapshot staging budget), or the ledger at {Ledger} is removed")]
    internal static partial void LogOutOfMemoryRestartBreakerTripped(this ILogger logger, string endpoint, int run, string reason, string ledger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Node {Endpoint}: {Run} of its last starts ended in an out-of-memory fail-fast early in recovery (last at {LastAt} in {Site}); {Threshold} in a row trips the restart breaker (0 = never). Ledger: {Ledger}")]
    internal static partial void LogOutOfMemoryRecoveryExitsRecorded(this ILogger logger, string endpoint, int run, int threshold, DateTimeOffset lastAt, string site, string ledger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Node {Endpoint}: the out-of-memory exit ledger at {Ledger} could not be read or armed; a crash loop in recovery will not be detected")]
    internal static partial void LogOutOfMemoryLedgerUnavailable(this ILogger logger, Exception exception, string endpoint, string ledger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Node {Endpoint} could not step down from partition #{PartitionId} after tripping its out-of-memory restart breaker: {Status}")]
    internal static partial void LogOutOfMemoryBreakerStepDownRefused(this ILogger logger, string endpoint, int partitionId, string status);

    [LoggerMessage(Level = LogLevel.Error, Message = "Node {Endpoint}: the out-of-memory restart breaker's containment failed; the writer's next tick retries it")]
    internal static partial void LogOutOfMemoryBreakerContainmentFailed(this ILogger logger, Exception exception, string endpoint);
}
