using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

/// <summary>
/// Prints the range-descriptor map a node has applied: which contiguous ranges exist per key space,
/// which partition serves each, and how that node routes the space.
/// </summary>
public static class RangesCommand
{
    public static async Task Execute(KahunaClient connection, string? keySpace, string? node, string? format)
    {
        KahunaRangeMapResponse map = await connection.GetRanges(keySpace, node);

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(map, KahunaJsonContext.Default.KahunaRangeMapResponse));
            return;
        }

        AnsiConsole.MarkupLine(
            "Node [cyan]{0}[/] {1}  [cyan]{2}[/] key space(s)\n",
            Markup.Escape(map.LocalEndpoint),
            map.Initialized ? "[green]initialized[/]" : "[yellow]not initialized (the map may be incomplete)[/]",
            map.KeySpaces.Count);

        if (map.KeySpaces.Count == 0)
        {
            AnsiConsole.MarkupLine("[grey]No key space is key-range routed on this node.[/]");
            AnsiConsole.WriteLine();
            return;
        }

        foreach (KahunaKeySpaceRangesResponse space in map.KeySpaces)
        {
            // The routing mode is node-local and unreplicated, so it belongs with the node heading
            // rather than looking like a property of the space cluster-wide.
            AnsiConsole.MarkupLine(
                "[bold]{0}[/]  routed by [cyan]{1}[/] on this node",
                Markup.Escape(space.KeySpace),
                Markup.Escape(space.RoutingMode));

            if (space.Descriptors.Count == 0)
            {
                AnsiConsole.MarkupLine(
                    "  [yellow]registered but not seeded[/] — no descriptor covers this space, so writes to it fail.");
                AnsiConsole.WriteLine();
                continue;
            }

            Table table = new();
            table.AddColumn("Start key");
            table.AddColumn("End key");
            table.AddColumn("Partition");
            table.AddColumn("Generation");

            foreach (KahunaRangeDescriptorResponse descriptor in space.Descriptors)
                table.AddRow(
                    descriptor.StartKey is null ? "[grey]-inf[/]" : Markup.Escape(descriptor.StartKey),
                    descriptor.EndKey is null ? "[grey]+inf[/]" : Markup.Escape(descriptor.EndKey),
                    descriptor.PartitionId.ToString(),
                    descriptor.Generation.ToString());

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
        }
    }
}
