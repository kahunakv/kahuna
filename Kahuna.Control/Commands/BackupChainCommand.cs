using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class BackupChainCommand
{
    public static async Task Execute(KahunaClient connection, Guid leafBackupId, string? format)
    {
        List<KahunaBackupInfo> chain = await connection.GetBackupChainAsync(leafBackupId);

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(chain, KahunaJsonContext.Default.ListKahunaBackupInfo));
            return;
        }

        AnsiConsole.MarkupLine("Backup chain for [cyan]{0}[/] ({1} entries)\n", leafBackupId, chain.Count);

        foreach (KahunaBackupInfo b in chain)
        {
            string arrow = b.Type == "Full" ? "[green]●[/]" : "[blue]├─[/]";
            AnsiConsole.MarkupLine("{0} [white]{1}[/]  {2}  partitions={3}", arrow, b.BackupId, b.Type, b.PartitionCount);
            AnsiConsole.MarkupLine("     Created: {0:u}", b.CreatedAtUtc);
        }

        AnsiConsole.WriteLine();
    }
}
