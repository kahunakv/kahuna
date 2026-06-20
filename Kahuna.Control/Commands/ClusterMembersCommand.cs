using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

public static class ClusterMembersCommand
{
    public static async Task Execute(KahunaClient connection, string? format)
    {
        KahunaClusterMembershipResponse membership = await connection.GetClusterMembership();

        if (format == "json")
        {
            Console.WriteLine(JsonSerializer.Serialize(membership, KahunaJsonContext.Default.KahunaClusterMembershipResponse));
            return;
        }

        AnsiConsole.MarkupLine("Membership version [cyan]{0}[/]  Local role [cyan]{1}[/]\n",
            membership.MembershipVersion, membership.LocalRole);

        Table table = new();
        table.AddColumn("Endpoint");
        table.AddColumn("Node ID");
        table.AddColumn("Role");
        table.AddColumn("Joined Version");

        foreach (KahunaClusterMemberResponse m in membership.Members)
        {
            string roleMarkup = m.Role switch
            {
                "Voter"     => "[green]Voter[/]",
                "Learner"   => "[yellow]Learner[/]",
                "Leaving"   => "[red]Leaving[/]",
                _           => Markup.Escape(m.Role)
            };
            table.AddRow(
                Markup.Escape(m.Endpoint),
                m.NodeId.ToString(),
                roleMarkup,
                m.JoinedVersion.ToString()
            );
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();
    }
}
