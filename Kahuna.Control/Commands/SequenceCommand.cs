/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using Kahuna.Client;
using Kahuna.Shared.Sequences;

namespace Kahuna.Control.Commands;

public static class SequenceCommand
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = new(JsonSerializerDefaults.Web);

    public static async Task Create(KahunaClient connection, string name, long initialValue, long increment, long? maxValue, int? blockSize, string? format)
    {
        KahunaSequence result = await connection.CreateSequence(name, initialValue, increment, maxValue, blockSize, SequenceDurability.Persistent);

        WriteSequence(result, "created", format);
    }

    /// <summary>
    /// Rewrites a sequence's parameters. The server holds the answer for one block lease before it
    /// reports success, so this command pauses for several seconds on purpose.
    /// </summary>
    public static async Task Update(KahunaClient connection, string name, SequenceUpdate update, string? format)
    {
        KahunaSequence result = await connection.UpdateSequence(name, update, SequenceDurability.Persistent);

        WriteSequence(result, "updated", format);
    }

    public static async Task Get(KahunaClient connection, string name, string? format)
    {
        KahunaSequence? result = await connection.GetSequence(name, SequenceDurability.Persistent);

        if (format == "json")
        {
            Console.WriteLine("{0}", JsonSerializer.Serialize(result, JsonSerializerOptions));
            return;
        }

        if (result is null)
        {
            Console.WriteLine("not found");
            return;
        }

        WriteSequence(result, "get", format);
    }

    public static async Task Next(KahunaClient connection, string name, string? idempotencyKey, string? format)
    {
        KahunaSequenceRange result = await connection.ReserveSequenceRange(name, 1, idempotencyKey, SequenceDurability.Persistent);

        WriteRange(result, "next", format);
    }

    public static async Task Reserve(KahunaClient connection, string name, int count, string? idempotencyKey, string? format)
    {
        KahunaSequenceRange result = await connection.ReserveSequenceRange(name, count, idempotencyKey, SequenceDurability.Persistent);

        WriteRange(result, "reserved", format);
    }

    public static async Task Delete(KahunaClient connection, string name, string? format)
    {
        bool result = await connection.DeleteSequence(name, SequenceDurability.Persistent);

        if (format == "json")
            Console.WriteLine("{0}", JsonSerializer.Serialize(new { name, deleted = result }, JsonSerializerOptions));
        else
            Console.WriteLine("{0}", result ? "deleted" : "not found");
    }

    private static void WriteSequence(KahunaSequence result, string action, string? format)
    {
        if (format == "json")
        {
            Console.WriteLine("{0}", JsonSerializer.Serialize(result, JsonSerializerOptions));
            return;
        }

        Console.WriteLine(
            "r{0} {1} {2} current {3} increment {4} max {5} block {6} incarnation {7}",
            result.Revision,
            action,
            result.Name,
            result.CurrentValue,
            result.Increment,
            result.MaxValue?.ToString() ?? "-",
            result.BlockSize?.ToString() ?? "default",
            result.Incarnation
        );
    }

    private static void WriteRange(KahunaSequenceRange result, string action, string? format)
    {
        if (format == "json")
        {
            Console.WriteLine("{0}", JsonSerializer.Serialize(result, JsonSerializerOptions));
            return;
        }

        if (result.Count == 1)
        {
            Console.WriteLine("r{0} {1} {2} {3}", result.Revision, action, result.Name, result.Start);
            return;
        }

        Console.WriteLine("r{0} {1} {2} {3}..{4} count {5}", result.Revision, action, result.Name, result.Start, result.End, result.Count);
    }
}
