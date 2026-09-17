using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The statement whose response stopped a script before it could commit: the script keyword, the key or
/// prefix it addressed, the durability it ran under, and the response that stopped it. The script reports this
/// response as its own outcome, so a retryable answer reaches the client as retryable and the reason names the
/// key a client or a log needs to tell a stuck key from a transient blip.
/// </summary>
internal readonly record struct ScriptStatementFailure(
    string Statement,
    string Key,
    KeyValueDurability Durability,
    KeyValueResponseType Type)
{
    public string Describe() => $"{Statement} {Key} ({Durability}) returned {Type}";
}
