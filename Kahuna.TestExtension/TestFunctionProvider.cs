
using Kahuna.Extensibility;

namespace Kahuna.TestExtension;

/// <summary>
/// The extension assembly the <c>--extension-assembly</c> tests load.
///
/// <para>It publishes two working functions by default. The environment variable named below makes
/// it fail in each of the ways the loader must report distinctly, so one fixture assembly covers
/// every failure mode without a project per mode. Nothing outside the tests sets that variable, so
/// the default path is what a real extension assembly looks like.</para>
/// </summary>
public sealed class TestFunctionProvider : IKahunaFunctionProvider
{
    /// <summary>Selects a failure mode. Unset means the provider behaves normally.</summary>
    public const string ModeVariable = "KAHUNA_TEST_EXTENSION_MODE";

    public TestFunctionProvider()
    {
        if (Environment.GetEnvironmentVariable(ModeVariable) == "ctor-throw")
            throw new InvalidOperationException("the provider constructor failed on purpose");
    }

    public void Register(KahunaFunctionRegistry registry)
    {
        switch (Environment.GetEnvironmentVariable(ModeVariable))
        {
            case "register-throw":
                throw new InvalidOperationException("registration failed on purpose");

            case "reserved-name":
                registry.Register("abs", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.Null);
                return;
        }

        registry.Register(
            "ext_double",
            static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From(args[0].AsLong() * 2),
            1,
            1);

        registry.Register(
            "ext_greet",
            static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From("hello " + args[0].AsString()),
            1,
            1);
    }
}
