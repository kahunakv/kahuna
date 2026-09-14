
namespace Kahuna.Extensibility;

/// <summary>
/// Publishes user-defined functions from an extension assembly.
///
/// <para>The shipped server binary cannot be recompiled by the operator who runs it, so an extension
/// assembly declares one or more public, non-abstract implementations of this interface with a
/// parameterless constructor. The server loads the assembly named by <c>--extension-assembly</c>,
/// builds each implementation once, and calls <see cref="Register"/>.</para>
///
/// <para>A host that compiles against Kahuna does not need this interface. It registers directly on
/// the registry its options expose.</para>
/// </summary>
public interface IKahunaFunctionProvider
{
    /// <summary>
    /// Adds this provider's functions to the node's registry. Throw to refuse to start the node: a
    /// failure here fails startup, because a node that silently lacks one function is the outcome
    /// the fingerprint diagnostics exist to prevent.
    /// </summary>
    void Register(KahunaFunctionRegistry registry);
}
