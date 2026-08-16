using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Spectre.Console;

namespace Kahuna.Control.Commands;

/// <summary>
/// Puts a key space under key-range routing, or removes it again.
/// <para>
/// Unlike the other range commands this does <b>not</b> stop at the first node that accepts. Half of
/// registration — the routing-mode flip — is node-local and unreplicated, so a cluster where only one
/// node was told routes the space by key range there and hashes it everywhere else. Sending it to
/// every configured endpoint is the only way to leave the cluster consistent, and a partial failure
/// is reported per endpoint rather than hidden behind a single success.
/// </para>
/// </summary>
public static class RegisterKeyRangeCommand
{
    public static async Task Register(
        KahunaClient connection, string keySpace, string[] endpoints, string? node, string? format)
    {
        string[] targets = ResolveTargets(endpoints, node);
        List<(string Endpoint, KahunaRegisterKeyRangeResponse Response)> results = [];

        foreach (string endpoint in targets)
            results.Add((endpoint, await connection.RegisterKeyRange(
                keySpace, string.IsNullOrEmpty(endpoint) ? null : endpoint)));

        if (format == "json")
        {
            foreach ((string endpoint, KahunaRegisterKeyRangeResponse response) in results)
                Console.WriteLine(JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaRegisterKeyRangeResponse));

            if (results.Exists(static r => !r.Response.Success))
                Environment.ExitCode = 1;

            return;
        }

        foreach ((string endpoint, KahunaRegisterKeyRangeResponse response) in results)
        {
            string where = string.IsNullOrEmpty(endpoint) ? "the connected node" : endpoint;

            if (response.Success)
                AnsiConsole.MarkupLine(
                    "[green]{0}[/] {1} — {2} (routing [cyan]{3}[/], [cyan]{4}[/] descriptor(s))",
                    response.Seeded ? "Seeded:" : "Registered:",
                    Markup.Escape(where),
                    response.Seeded ? "this call committed the whole-space descriptor" : "a descriptor already existed",
                    Markup.Escape(response.RoutingMode),
                    response.DescriptorCount);
            else
                AnsiConsole.MarkupLine(
                    "[red]Refused:[/] {0} — status [cyan]{1}[/] — {2}",
                    Markup.Escape(where),
                    Markup.Escape(response.Status),
                    Markup.Escape(response.Reason ?? "no reason reported"));
        }

        if (results.Exists(static r => !r.Response.Success))
        {
            AnsiConsole.MarkupLine(
                "\n[red]Some nodes did not register the key space.[/] The routing mode is node-local: until every "
              + "node has it, some nodes route this space by key range and the rest still hash it.");
            Environment.ExitCode = 1;
            return;
        }

        if (!string.IsNullOrEmpty(node))
            AnsiConsole.MarkupLine(
                "\n[yellow]Only {0} was registered.[/] Repeat this on every node, or the cluster is left "
              + "half-configured.", Markup.Escape(node));
    }

    public static async Task Unregister(
        KahunaClient connection, string keySpace, string[] endpoints, string? node, string? format)
    {
        string[] targets = ResolveTargets(endpoints, node);
        List<(string Endpoint, KahunaRemoveKeyRangeResponse Response)> results = [];

        foreach (string endpoint in targets)
            results.Add((endpoint, await connection.RemoveKeyRange(
                keySpace, string.IsNullOrEmpty(endpoint) ? null : endpoint)));

        if (format == "json")
        {
            foreach ((string _, KahunaRemoveKeyRangeResponse response) in results)
                Console.WriteLine(JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaRemoveKeyRangeResponse));

            if (results.Exists(static r => !r.Response.Success))
                Environment.ExitCode = 1;

            return;
        }

        foreach ((string endpoint, KahunaRemoveKeyRangeResponse response) in results)
        {
            string where = string.IsNullOrEmpty(endpoint) ? "the connected node" : endpoint;

            if (response.Success)
                AnsiConsole.MarkupLine(
                    "[green]Removed:[/] {0} — [cyan]{1}[/] descriptor(s) remain",
                    Markup.Escape(where), response.DescriptorCount);
            else
                AnsiConsole.MarkupLine(
                    "[red]Refused:[/] {0} — status [cyan]{1}[/] — {2}",
                    Markup.Escape(where),
                    Markup.Escape(response.Status),
                    Markup.Escape(response.Reason ?? "no reason reported"));
        }

        if (results.Exists(static r => !r.Response.Success))
            Environment.ExitCode = 1;
    }

    /// <summary>
    /// Every configured endpoint, unless the operator named one. An empty entry means "whatever the
    /// connection resolves to", which is the single-endpoint case.
    /// </summary>
    private static string[] ResolveTargets(string[] endpoints, string? node)
    {
        if (!string.IsNullOrEmpty(node))
            return [node];

        return endpoints.Length > 0 ? endpoints : [""];
    }
}
