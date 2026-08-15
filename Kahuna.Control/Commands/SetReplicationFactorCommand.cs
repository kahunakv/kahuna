using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class SetReplicationFactorCommand
{
    /// <summary>
    /// Commits a per-partition replication-factor override. The override is a meta-partition map
    /// mutation, so only that partition's leader accepts it; when no node is named this tries each
    /// connected endpoint until one commits, since the operator generally doesn't know which node
    /// currently leads.
    /// </summary>
    public static async Task Execute(
        KahunaClient connection, int partitionId, int replicationFactor, string[] endpoints, string? format)
    {
        KahunaSetReplicationFactorResponse? last = null;

        string[] targets = endpoints.Length > 0 ? endpoints : [""];
        foreach (string endpoint in targets)
        {
            last = await connection.SetReplicationFactor(
                partitionId, replicationFactor, string.IsNullOrEmpty(endpoint) ? null : endpoint);
            if (last.Success)
                break;
        }

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(last!, KahunaJsonContext.Default.KahunaSetReplicationFactorResponse));
            if (!last!.Success)
                Environment.ExitCode = 1;
            return;
        }

        if (last!.Success)
        {
            AnsiConsole.MarkupLine(
                "[green]Committed:[/] partition [cyan]{0}[/] replication factor override set to [cyan]{1}[/]{2} (generation [cyan]{3}[/]).",
                partitionId,
                replicationFactor,
                replicationFactor == 0 ? " (cleared; inherits the global factor)" : "",
                last.Generation);
            AnsiConsole.MarkupLine("The rebalancer moves replicas toward the new target on later passes.");
        }
        else
        {
            AnsiConsole.MarkupLine(
                "[red]Refused:[/] status [cyan]{0}[/] — {1}",
                Markup.Escape(last.Status),
                Markup.Escape(last.Reason ?? "no reason reported"));
            Environment.ExitCode = 1;
        }
    }
}
