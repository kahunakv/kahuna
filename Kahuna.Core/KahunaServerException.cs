
using Kahuna.Shared.KeyValue;

namespace Kahuna;

/// <summary>
/// Represents an exception specific to the Kahuna server.
/// </summary>
public class KahunaServerException : Exception
{
    /// <summary>
    /// The key-value response type that caused the failure, when the failure wraps one.
    /// Lets a caller classify the error (for example, retry on <see cref="KeyValueResponseType.MustRetry"/>
    /// or <see cref="KeyValueResponseType.AdmissionRefused"/>) without parsing the message text.
    /// Null when the failure did not originate from a key-value response.
    /// </summary>
    public KeyValueResponseType? ResponseType { get; }

    public KahunaServerException(string message) : base(message)
    {
    }

    public KahunaServerException(string message, KeyValueResponseType responseType) : base(message)
    {
        ResponseType = responseType;
    }
}
