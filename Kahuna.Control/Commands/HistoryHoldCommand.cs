/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Spectre.Console;

namespace Kahuna.Control.Commands;

/// <summary>
/// Acquires, renews, releases and lists history holds. A history hold keeps a partition's WAL retained from a timestamp
/// on every replica, so a watch can resume from that timestamp. The <c>json</c> format prints the REST answer.
/// </summary>
public static class HistoryHoldCommand
{
    public static async Task Acquire(KahunaClient connection, string holderId, string partitions, string? from, int leaseMs, string? format) =>
        Print(await connection.AcquireHistoryHolds(holderId, ParsePartitions(partitions), ParseTimestamp(from), leaseMs), format);

    public static async Task Renew(KahunaClient connection, string holdIds, string? from, int leaseMs, string? format) =>
        Print(await connection.RenewHistoryHolds(ParseIds(holdIds), leaseMs, from is null ? HLCTimestamp.Zero : ParseTimestamp(from)), format);

    public static async Task Release(KahunaClient connection, string holdIds, string? format) =>
        Print(await connection.ReleaseHistoryHolds(ParseIds(holdIds)), format);

    public static async Task List(KahunaClient connection, string? holderId, string? format) =>
        Print(await connection.ListHistoryHolds(holderId), format);

    /// <summary>Parses a timestamp written as <c>node:physical:counter</c> or as the physical milliseconds alone.</summary>
    public static HLCTimestamp ParseTimestamp(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            throw new ArgumentException("A timestamp is required, as node:physical:counter or as physical milliseconds.");

        string[] parts = text.Split(':');

        return parts.Length switch
        {
            1 => new HLCTimestamp(0, long.Parse(parts[0], CultureInfo.InvariantCulture), 0),
            3 => new HLCTimestamp(int.Parse(parts[0], CultureInfo.InvariantCulture), long.Parse(parts[1], CultureInfo.InvariantCulture), uint.Parse(parts[2], CultureInfo.InvariantCulture)),
            _ => throw new ArgumentException($"'{text}' is not a timestamp: write node:physical:counter or physical milliseconds.")
        };
    }

    private static List<int> ParsePartitions(string text) =>
        [.. text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(p => int.Parse(p, CultureInfo.InvariantCulture))];

    private static List<string> ParseIds(string text) =>
        [.. text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

    private static void Print(HistoryHoldAnswer answer, string? format)
    {
        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(HistoryHoldWire.ToRest(answer), KahunaJsonContext.Default.KahunaHistoryHoldResponse));
            return;
        }

        AnsiConsole.MarkupLine("[cyan]{0}[/]", Markup.Escape(answer.Type.ToString()));

        foreach (HistoryHoldInfo hold in answer.Holds)
        {
            Console.WriteLine(
                "{0} holder={1} partition={2} from={3}:{4}:{5} lease-expiry={6}",
                hold.HoldId, hold.HolderId, hold.PartitionId,
                hold.Timestamp.N, hold.Timestamp.L, hold.Timestamp.C,
                hold.LeaseExpiry.L);
        }
    }
}
