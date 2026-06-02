using CommandLine;

namespace Kahuna.Tests.Server;

public sealed class TestWalSyncWritesConfiguration
{
    [Fact]
    public void ServerCliDefaultsWalSyncWritesToEnabled()
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>([]);

        Assert.True(result.Tag == ParserResultType.Parsed);
        Assert.True(result.Value.GetWalSyncWrites());
    }

    [Fact]
    public void ServerCliDisableWalSyncWritesTurnsOffSyncWrites()
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>([
            "--disable-wal-sync-writes"
        ]);

        Assert.True(result.Tag == ParserResultType.Parsed);
        Assert.False(result.Value.GetWalSyncWrites());
    }

    [Fact]
    public void ServerCliExplicitWalSyncWritesKeepsSyncWritesEnabled()
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>([
            "--wal-sync-writes"
        ]);

        Assert.True(result.Tag == ParserResultType.Parsed);
        Assert.True(result.Value.GetWalSyncWrites());
    }

    [Fact]
    public void EmbeddedOptionsDefaultWalSyncWritesToEnabled()
    {
        EmbeddedKahunaOptions options = new();

        Assert.True(options.WalSyncWrites);
    }
}
