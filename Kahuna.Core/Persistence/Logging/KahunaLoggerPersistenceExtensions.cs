/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Server.Persistence.Logging;

public static partial class KahunaLoggerPersistenceExtensions
{
    [LoggerMessage(Level = LogLevel.Debug, Message = "Successfully checkpointed partition #{PartitionId}")]
    public static partial void LogSuccessfullyCheckpointedPartition(this ILogger<IKahuna> logger, int partitionId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Successfully stored batch of {Count} locks in {Elapsed}ms")]
    public static partial void LogSuccessfullyStoredLocks(this ILogger<IKahuna> logger, int count, long elapsed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Successfully stored batch of {Count} key-values in {Elapsed}ms")]
    public static partial void LogSuccessfullyStoredKeyValues(this ILogger<IKahuna> logger, int count, long elapsed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Pruned persistent key/value revisions: mode=targeted keys={Keys} deleted={Deleted} backlog={Backlog} elapsedMs={Elapsed} backend={Backend} retentionCount={RetentionCount} retentionAge={RetentionAge}")]
    public static partial void LogPrunedKeyValueRevisionsTargeted(this ILogger<IKahuna> logger, int keys, int deleted, bool backlog, long elapsed, string? backend, int retentionCount, TimeSpan retentionAge);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Pruned persistent key/value revisions: mode=sweep keys={Keys} deleted={Deleted} backlog={Backlog} elapsedMs={Elapsed} backend={Backend} retentionCount={RetentionCount} retentionAge={RetentionAge}")]
    public static partial void LogPrunedKeyValueRevisionsSweep(this ILogger<IKahuna> logger, int keys, int deleted, bool backlog, long elapsed, string? backend, int retentionCount, TimeSpan retentionAge);
}
