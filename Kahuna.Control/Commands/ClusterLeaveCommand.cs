using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class ClusterLeaveCommand
{
    /// <summary>
    /// Decommissions one node: it commits its own removal from the roster, so the cluster shrinks
    /// by consensus rather than after failure detection times it out. The node keeps running — stop
    /// it afterwards, and only once this reports the removal committed.
    /// <para>
    /// Exits non-zero when the node is still in the roster, so a scale-down script can stop instead
    /// of proceeding to kill a node that was refused permission to leave.
    /// </para>
    /// </summary>
    public static async Task Execute(KahunaClient connection, string nodeUrl, string? format)
    {
        KahunaClusterLeaveResponse response = await connection.LeaveCluster(nodeUrl);

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaClusterLeaveResponse));
        }
        else
        {
            string outcomeMarkup = response.Left
                ? $"[green]{Markup.Escape(response.Outcome)}[/]"
                : response.Retryable
                    ? $"[yellow]{Markup.Escape(response.Outcome)}[/]"
                    : $"[red]{Markup.Escape(response.Outcome)}[/]";

            AnsiConsole.MarkupLine("Node [cyan]{0}[/]  Outcome {1}  Membership version [cyan]{2}[/]",
                Markup.Escape(nodeUrl), outcomeMarkup, response.MembershipVersion);
            AnsiConsole.MarkupLine(Markup.Escape(response.Reason));

            // A node that left without draining took its replicas' redundancy with it, which an
            // operator watching only the outcome would not see.
            if (response.Left)
                AnsiConsole.MarkupLine(response.Drained
                    ? "[green]Replicas were evacuated onto surviving nodes before the removal committed.[/]"
                    : "[yellow]No replicas were evacuated: either no range was placed on this node, or its ranges are now short a replica until placement repairs them.[/]");

            if (!response.Left && response.Retryable)
                AnsiConsole.MarkupLine("[yellow]The node is still in the roster; re-check membership before stopping it.[/]");

            AnsiConsole.WriteLine();
        }

        if (!response.Left)
            Environment.ExitCode = 1;
    }
}
