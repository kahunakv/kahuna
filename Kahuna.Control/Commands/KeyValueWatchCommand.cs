/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Control.Commands;

/// <summary>
/// Watches every key that starts with a prefix and prints each frame until the watch ends or the user presses
/// Ctrl+C. The <c>json</c> format prints one REST watch frame per line.
/// </summary>
public static class KeyValueWatchCommand
{
    public static async Task Execute(KahunaClient connection, string prefix, bool keysOnly, string? format, string? resumeFrom = null)
    {
        using CancellationTokenSource stop = new();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stop.Cancel();
        };

        await Run(connection, prefix, keysOnly, format, stop.Token, resumeFrom is null ? default : HistoryHoldCommand.ParseTimestamp(resumeFrom));
    }

    /// <summary>
    /// Watches <paramref name="prefix"/> and prints each frame until the watch ends or
    /// <paramref name="cancellationToken"/> is cancelled. The caller decides what cancels it.
    /// </summary>
    public static async Task Run(KahunaClient connection, string prefix, bool keysOnly, string? format, CancellationToken cancellationToken, Kommander.Time.HLCTimestamp resumeFrom = default)
    {
        try
        {
            await using KahunaWatch watch = await connection.WatchKeyValues(
                new(KeyValueWatchScope.ForPrefix(prefix), KeysOnly: keysOnly, ResumeFrom: resumeFrom),
                cancellationToken);

            if (format != "json")
                Console.WriteLine("watching '{0}' on {1} (partitions {2})", prefix, string.Join(", ", watch.Endpoints), string.Join(", ", watch.Partitions));

            await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(cancellationToken))
            {
                if (format == "json")
                {
                    Console.WriteLine(JsonSerializer.Serialize(KeyValueWatchWire.ToRest(frame), KahunaJsonContext.Default.KahunaWatchFrameResponse));
                    continue;
                }

                Console.WriteLine(Describe(frame));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private static string Describe(KeyValueWatchFrame frame)
    {
        if (frame.Event is { } e)
        {
            string value = e.Value is null ? "-" : Encoding.UTF8.GetString(e.Value);
            string origin = e.Origin is null ? "" : $" origin={e.Origin}";
            return $"{e.Kind.ToString().ToLowerInvariant()} r{e.Revision} p{e.PartitionId} {e.Key} {value}{origin}";
        }

        return frame.Type switch
        {
            KeyValueWatchFrameType.Progress => $"progress p{frame.PartitionId} {frame.Timestamp.N}:{frame.Timestamp.L}:{frame.Timestamp.C}",
            KeyValueWatchFrameType.TransactionEnd => $"transaction-end p{frame.PartitionId} {frame.TransactionId.N}:{frame.TransactionId.L}:{frame.TransactionId.C}",
            KeyValueWatchFrameType.Lagged => $"lagged ({frame.LagReason}) on partition {frame.PartitionId}: reopen the watch from the last progress timestamp",
            KeyValueWatchFrameType.HistoryUnavailable => $"history unavailable on partition {frame.PartitionId}: take a new snapshot",
            KeyValueWatchFrameType.Refused => $"refused: {frame.Refusal}",
            _ => frame.Type.ToString().ToLowerInvariant()
        };
    }
}
