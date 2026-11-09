using Kahuna.Utils;

namespace Kahuna.Server.Tests;

public sealed class TestSpillableBufferStream : IDisposable
{
    private readonly string directory = Directory.CreateTempSubdirectory("spillable-").FullName;

    public void Dispose() => Directory.Delete(directory, recursive: true);

    private static byte[] Pattern(int length)
    {
        byte[] bytes = new byte[length];
        new Random(7).NextBytes(bytes);
        return bytes;
    }

    private string[] SpillFiles() => Directory.GetFiles(directory, "*" + SpillableBufferStream.SpillFileExtension);

    [Fact]
    public void StaysInMemoryUpToTheBudget()
    {
        byte[] content = Pattern(1024);
        using SpillableBufferStream stream = new(directory, 1024);

        stream.Write(content);

        Assert.False(stream.IsSpilled);
        Assert.Empty(SpillFiles());
        Assert.Equal(1024, stream.Length);
    }

    [Fact]
    public void MovesToAFilePastTheBudget_AndKeepsEveryByte()
    {
        byte[] content = Pattern(500_000);
        using SpillableBufferStream stream = new(directory, 100_000);

        // Past the budget in the middle of the content, in writes small and large.
        stream.Write(content, 0, 10);
        stream.WriteByte(content[10]);
        stream.Write(content.AsSpan(11, 200_000));
        stream.Write(content.AsSpan(200_011));

        Assert.True(stream.IsSpilled);
        Assert.Single(SpillFiles());
        Assert.Equal(content.Length, stream.Length);

        using MemoryStream copy = new();
        stream.CopyTo(copy);
        Assert.Equal(content, copy.ToArray());
    }

    [Fact]
    public async Task ReadsTheSameAfterTheSpill_ThroughEveryRoute()
    {
        byte[] content = Pattern(300_000);
        using SpillableBufferStream stream = new(directory, 1_000);
        stream.Write(content);

        byte[] viaAsync = new byte[content.Length];
        int filled = 0;
        int read;
        while ((read = await stream.ReadAsync(viaAsync.AsMemory(filled), TestContext.Current.CancellationToken)) > 0)
            filled += read;
        Assert.Equal(content.Length, filled);
        Assert.Equal(content, viaAsync);

        Assert.Equal(0, stream.Read(new byte[8], 0, 8));

        stream.Position = 5;
        Assert.Equal(content[5], stream.ReadByte());
        Assert.Equal(6, stream.Position);

        stream.Seek(-1, SeekOrigin.End);
        Assert.Equal(content[^1], stream.ReadByte());
        Assert.Equal(-1, stream.ReadByte());
    }

    [Fact]
    public void WritesAppendAfterAReadMovedTheCursor()
    {
        byte[] content = Pattern(200_000);
        using SpillableBufferStream stream = new(directory, 1_000);
        stream.Write(content.AsSpan(0, 100_000));

        Assert.Equal(4, stream.Read(new byte[4]));
        stream.Write(content.AsSpan(100_000));
        Assert.Equal(4, stream.Position);

        stream.Position = 0;
        using MemoryStream copy = new();
        stream.CopyTo(copy);
        Assert.Equal(content, copy.ToArray());
    }

    [Fact]
    public void WithNoDirectory_StaysInMemoryWhateverItHolds()
    {
        byte[] content = Pattern(300_000);
        using SpillableBufferStream stream = new(spillDirectory: null, 1_000);

        stream.Write(content);

        Assert.False(stream.IsSpilled);

        using MemoryStream copy = new();
        stream.CopyTo(copy);
        Assert.Equal(content, copy.ToArray());
    }

    [Fact]
    public void DisposingRemovesTheFile()
    {
        SpillableBufferStream stream = new(directory, 10);
        stream.Write(Pattern(1_000));
        Assert.Single(SpillFiles());

        stream.Dispose();

        Assert.Empty(SpillFiles());
        Assert.Throws<ObjectDisposedException>(() => stream.Length);
    }

    [Fact]
    public void AFailedSpill_LeavesTheContentInMemory()
    {
        byte[] content = Pattern(1_000);
        using SpillableBufferStream stream = new(Path.Combine(directory, "does-not-exist"), 1_000);
        stream.Write(content);

        Assert.Throws<DirectoryNotFoundException>(() => stream.WriteByte(1));

        Assert.False(stream.IsSpilled);
        Assert.Equal(content.Length, stream.Length);

        using MemoryStream copy = new();
        stream.CopyTo(copy);
        Assert.Equal(content, copy.ToArray());
    }
}
