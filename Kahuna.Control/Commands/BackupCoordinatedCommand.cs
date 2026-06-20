using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class BackupCoordinatedCommand
{
    public static async Task Execute(KahunaClient connection, string? format)
    {
        KahunaBackupInfo info = await connection.TakeCoordinatedBackupAsync();

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(info, KahunaJsonContext.Default.KahunaBackupInfo));
            return;
        }

        AnsiConsole.MarkupLine("Coordinated backup [cyan]{0}[/] completed", info.BackupId);
        BackupFullCommand.PrintInfo(info);
    }
}
