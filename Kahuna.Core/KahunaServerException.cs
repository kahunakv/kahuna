
namespace Kahuna;

/// <summary>
/// Represents an exception specific to the Kahuna server.
/// </summary>
public class KahunaServerException : Exception
{
    public KahunaServerException(string message) : base(message)
    {
    }
}