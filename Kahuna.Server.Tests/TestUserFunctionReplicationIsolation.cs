using System.Reflection;

using Kahuna.Extensibility;
using Kahuna.Server.KeyValues.Transactions.Functions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Enforces the invariant the whole feature rests on: user code never runs on the replication,
/// restore or persistence path.
///
/// <para>Raft carries the value a function produced, never the call, so a node that does not have a
/// function registered can still apply the log, restore from a cold start, receive a snapshot, and
/// serve reads. That is what makes a per-process registry safe in a cluster and what makes a
/// function safe to retire. It is also invisible in a diff: a well-meaning change that gave a
/// restorer or a background writer access to the function table would break it silently, and every
/// existing test would still pass.</para>
///
/// <para>So it is checked mechanically. These tests walk the compiled assembly and assert that no
/// type in those namespaces mentions the function table or the extension API at all — not in a
/// field, a property, a method signature, or a constructor parameter.</para>
/// </summary>
public sealed class TestUserFunctionReplicationIsolation
{
    /// <summary>
    /// Namespace fragments that must never touch user code. The match is on a containing segment, so
    /// <c>Kahuna.Server.Replication</c> and anything nested under it are covered.
    /// </summary>
    private static readonly string[] ForbiddenNamespaces =
    [
        ".Replication",
        ".Persistence"
    ];

    /// <summary>
    /// Type names that must never appear on those paths, whatever namespace they sit in. The
    /// restorers and the replicator live under <c>KeyValues</c>, not under <c>Replication</c>, so they
    /// are named directly.
    /// </summary>
    private static readonly string[] ForbiddenTypeNames =
    [
        "KeyValueRestorer",
        "LockRestorer",
        "KeyValueReplicator",
        "LockReplicator",
        "BackgroundWriterActor",
        "ReplicationSerializer"
    ];

    private static readonly Type[] UserCodeTypes =
    [
        typeof(ScriptFunctionTable),
        typeof(ScriptFunctionEntry),
        typeof(ScriptFunctionStats),
        typeof(KahunaFunctionRegistry),
        typeof(KahunaFunctionDelegate),
        typeof(KahunaValue),
        typeof(IKahunaFunctionProvider)
    ];

    private static Type[] LoadCoreTypes()
    {
        // Every type of the assembly, including the internal ones: the invariant is about what the
        // code can reach, not about what it exposes.
        return typeof(ScriptFunctionTable).Assembly.GetTypes();
    }

    private static bool IsOnAForbiddenPath(Type type)
    {
        string fullName = type.FullName ?? type.Name;

        foreach (string fragment in ForbiddenNamespaces)
        {
            if (fullName.Contains(fragment + ".", StringComparison.Ordinal))
                return true;
        }

        foreach (string name in ForbiddenTypeNames)
        {
            if (type.Name.StartsWith(name, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static bool IsUserCodeType(Type? candidate)
    {
        if (candidate is null)
            return false;

        Type target = candidate.IsByRef || candidate.IsArray || candidate.IsPointer
            ? candidate.GetElementType() ?? candidate
            : candidate;

        if (target.IsGenericType)
        {
            foreach (Type argument in target.GetGenericArguments())
            {
                if (IsUserCodeType(argument))
                    return true;
            }

            target = target.GetGenericTypeDefinition();
        }

        return Array.IndexOf(UserCodeTypes, target) >= 0;
    }

    private const BindingFlags AllMembers =
        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    [Fact]
    public void TestNoReplicationOrPersistenceTypeReferencesUserCode()
    {
        List<string> violations = [];

        foreach (Type type in LoadCoreTypes())
        {
            if (!IsOnAForbiddenPath(type))
                continue;

            foreach (FieldInfo field in type.GetFields(AllMembers))
            {
                if (IsUserCodeType(field.FieldType))
                    violations.Add($"{type.FullName}.{field.Name} (field)");
            }

            foreach (PropertyInfo property in type.GetProperties(AllMembers))
            {
                if (IsUserCodeType(property.PropertyType))
                    violations.Add($"{type.FullName}.{property.Name} (property)");
            }

            foreach (MethodInfo method in type.GetMethods(AllMembers))
            {
                if (IsUserCodeType(method.ReturnType))
                    violations.Add($"{type.FullName}.{method.Name} (return type)");

                foreach (ParameterInfo parameter in method.GetParameters())
                {
                    if (IsUserCodeType(parameter.ParameterType))
                        violations.Add($"{type.FullName}.{method.Name}({parameter.Name}) (parameter)");
                }
            }

            foreach (ConstructorInfo constructor in type.GetConstructors(AllMembers))
            {
                foreach (ParameterInfo parameter in constructor.GetParameters())
                {
                    if (IsUserCodeType(parameter.ParameterType))
                        violations.Add($"{type.FullName}..ctor({parameter.Name}) (parameter)");
                }
            }
        }

        Assert.True(
            violations.Count == 0,
            "User code must never be reachable from the replication, restore or persistence path. "
            + "Raft carries the value a function produced, not the call, which is what lets a node "
            + "without the function still apply, restore and serve. Offending members: "
            + string.Join(", ", violations));
    }

    /// <summary>
    /// The scan above proves nothing if it inspects nothing, so this pins down that the filter really
    /// selects the paths it names and that the user-code detector really fires.
    /// </summary>
    [Fact]
    public void TestTheScanActuallyInspectsThosePaths()
    {
        int inspected = 0;

        foreach (Type type in LoadCoreTypes())
        {
            if (IsOnAForbiddenPath(type))
                inspected++;
        }

        Assert.True(inspected > 20, $"only {inspected} replication/persistence types were inspected, which is too few to be the real set");

        // The detector must recognise both the internal table and the published API, directly and
        // through a generic argument, or the scan above could pass while blind.
        Assert.True(IsUserCodeType(typeof(ScriptFunctionTable)));
        Assert.True(IsUserCodeType(typeof(KahunaValue)));
        Assert.True(IsUserCodeType(typeof(KahunaFunctionRegistry)));
        Assert.True(IsUserCodeType(typeof(List<KahunaValue>)));
        Assert.True(IsUserCodeType(typeof(ScriptFunctionTable[])));
        Assert.False(IsUserCodeType(typeof(string)));
        Assert.False(IsUserCodeType(typeof(List<int>)));
    }
}
