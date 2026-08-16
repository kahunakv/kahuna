using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

/// <summary>
/// Splits the range covering a key at exactly that key, and runs the merge pass.
/// <para>
/// Both are leader-only for the partition that owns the range map, so when no node is named each
/// connected endpoint is tried until one accepts — the operator generally does not know which node
/// currently leads. A refusal that is <i>not</i> "wrong leader" stops the loop: retrying a decision
/// like "that split key is invalid" against every other node only prints the same answer three times.
/// </para>
/// </summary>
public static class SplitRangeCommand
{
    public static async Task Split(
        KahunaClient connection, string keySpace, string splitKey, string[] endpoints, string? node, string? format)
    {
        string[] targets = !string.IsNullOrEmpty(node) ? [node] : endpoints.Length > 0 ? endpoints : [""];

        KahunaSplitRangeResponse? last = null;

        foreach (string endpoint in targets)
        {
            last = await connection.SplitRange(
                keySpace, splitKey, string.IsNullOrEmpty(endpoint) ? null : endpoint);

            // Only leadership is worth re-asking elsewhere; every other status is this cluster's
            // answer and will not change by node.
            if (last.Success || last.Status != "NotLeader")
                break;
        }

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(last!, KahunaJsonContext.Default.KahunaSplitRangeResponse));
            if (!last!.Success)
                Environment.ExitCode = 1;
            return;
        }

        if (last!.Success)
        {
            AnsiConsole.MarkupLine(
                "[green]Split:[/] [cyan]{0}[/] at [cyan]{1}[/] — the upper half is now served by partition "
              + "[cyan]{2}[/] at generation [cyan]{3}[/].",
                Markup.Escape(keySpace), Markup.Escape(splitKey), last.NewPartitionId, last.NewGeneration);
            AnsiConsole.MarkupLine("Read the result back with [cyan]--ranges --key-space {0}[/].", Markup.Escape(keySpace));
            return;
        }

        AnsiConsole.MarkupLine(
            "[red]Not split:[/] status [cyan]{0}[/] — {1}",
            Markup.Escape(last.Status),
            Markup.Escape(last.Reason ?? "no reason reported"));

        // The distinction a caller must not lose: a refusal is final, an indeterminate outcome means
        // the map may still change and nothing here knows whether it did.
        if (!last.Determinate)
            AnsiConsole.MarkupLine(
                "[yellow]This outcome is indeterminate[/] — the split may still land. Re-read "
              + "[cyan]--ranges --key-space {0}[/] before deciding what happened.", Markup.Escape(keySpace));

        if (!string.IsNullOrEmpty(last.LeaderHint))
            AnsiConsole.MarkupLine("Try [cyan]--node {0}[/].", Markup.Escape(last.LeaderHint));

        Environment.ExitCode = 1;
    }

    public static async Task Merge(KahunaClient connection, string[] endpoints, string? node, string? format)
    {
        string[] targets = !string.IsNullOrEmpty(node) ? [node] : endpoints.Length > 0 ? endpoints : [""];

        KahunaMergeRangesResponse? last = null;

        foreach (string endpoint in targets)
        {
            last = await connection.MergeRanges(string.IsNullOrEmpty(endpoint) ? null : endpoint);

            if (last.Success || last.Status != "NotLeader")
                break;
        }

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(last!, KahunaJsonContext.Default.KahunaMergeRangesResponse));
            if (!last!.Success)
                Environment.ExitCode = 1;
            return;
        }

        if (last!.Success)
        {
            // Zero here means "the pass ran and found nothing eligible", which is why it is phrased
            // as a completed pass rather than as a count on its own.
            AnsiConsole.MarkupLine(
                last.Merges == 0
                    ? "[green]Merge pass complete:[/] no adjacent ranges were small enough to fold."
                    : $"[green]Merge pass complete:[/] folded [cyan]{last.Merges}[/] adjacent pair(s).");
            return;
        }

        AnsiConsole.MarkupLine(
            "[red]No merge pass ran:[/] status [cyan]{0}[/] — {1}",
            Markup.Escape(last.Status),
            Markup.Escape(last.Reason ?? "no reason reported"));

        if (!last.Determinate)
            AnsiConsole.MarkupLine(
                "[yellow]This outcome is indeterminate[/] — merges already committed stay committed. Re-read "
              + "[cyan]--ranges[/].");

        if (!string.IsNullOrEmpty(last.LeaderHint))
            AnsiConsole.MarkupLine("Try [cyan]--node {0}[/].", Markup.Escape(last.LeaderHint));

        Environment.ExitCode = 1;
    }
}
