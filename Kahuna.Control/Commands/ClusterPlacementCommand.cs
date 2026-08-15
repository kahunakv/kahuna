using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class ClusterPlacementCommand
{
    public static async Task Execute(KahunaClient connection, string? node, string? format)
    {
        KahunaClusterPlacementResponse placement = await connection.GetClusterPlacement(node);

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(placement, KahunaJsonContext.Default.KahunaClusterPlacementResponse));
            return;
        }

        AnsiConsole.MarkupLine(
            "Replication factor [cyan]{0}[/] ({1})  Rebalancer [cyan]{2}[/]  Node [cyan]{3}[/] hosts [cyan]{4}[/] of [cyan]{5}[/] partitions\n",
            placement.ReplicationFactor,
            placement.ReplicationFactor > 0 ? "per-partition placement" : "full replication",
            placement.RebalancerEnabled ? "enabled" : "disabled",
            Markup.Escape(placement.LocalEndpoint),
            placement.HostedPartitionCount,
            placement.Partitions.Count);

        Table table = new();
        table.AddColumn("Partition");
        table.AddColumn("State");
        table.AddColumn("Generation");
        table.AddColumn("Effective RF");
        table.AddColumn("Hosted here");
        table.AddColumn("Replicas");

        foreach (KahunaPartitionPlacementResponse p in placement.Partitions)
        {
            string stateMarkup = p.State switch
            {
                "Active"   => "[green]Active[/]",
                "Draining" => "[yellow]Draining[/]",
                "Removed"  => "[red]Removed[/]",
                _          => Markup.Escape(p.State)
            };

            string replicas = p.Replicas.Count == 0
                ? "[grey]all voters (full replication)[/]"
                : Markup.Escape(string.Join(", ", p.Replicas.Select(r => $"{r.Endpoint} ({r.Role})")));

            table.AddRow(
                p.PartitionId.ToString(),
                stateMarkup,
                p.Generation.ToString(),
                p.EffectiveReplicationFactor.ToString(),
                p.HostedLocally ? "[green]yes[/]" : "no",
                replicas);
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();
    }
}
