namespace Kahuna.Client;

public sealed class KahunaSequenceRange
{
    public string Name { get; }

    public long Start { get; }

    public long End { get; }

    public int Count { get; }

    public long Revision { get; }

    public KahunaSequenceRange(string name, long start, long end, int count, long revision)
    {
        Name = name;
        Start = start;
        End = end;
        Count = count;
        Revision = revision;
    }
}
