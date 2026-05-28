namespace Kahuna.Shared.Sequences;

public readonly record struct SequenceAllocation(
    string Name,
    long Start,
    long End,
    int Count,
    long Revision
);
