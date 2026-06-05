
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

public class KeyValueGetByRangePageResult
{
    public List<KeyValueGetByBucketItem> Items { get; set; } = [];

    public string? NextCursor { get; set; }

    public bool HasMore { get; set; }

    public HLCTimestamp ReadTimestamp { get; set; }
}
