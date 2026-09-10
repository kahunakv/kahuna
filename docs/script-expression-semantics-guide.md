# Script expression semantics guide

Kahuna script is the language a transaction script is written in. This guide covers the parts of it whose
behavior a script author must know exactly: what counts as a condition, how numbers compare, when an
operand is evaluated, how a range is bounded, and which statements a transaction refuses. It is not a
full language reference.

## Conditions are boolean, and only boolean

`IF`, `&&`, `||` and `!` all take a boolean. A number, a string, or a null in a condition is a script
error that names the line, not a silent false.

```
IF 1 THEN … END            -- error: expected a boolean, found LongType
IF x == 1 THEN … END       -- correct
IF count(items) > 0 THEN … -- correct
```

Use a comparison to turn a value into a condition. A cast function such as `to_bool` also produces a
boolean where that is what you mean.

The language had two rules before: `IF` required a boolean and quietly took the `ELSE` branch for
anything else, while `&&` treated any non-zero number as true. So `IF 1 THEN` ran the `ELSE` branch and
`IF 1 && 1 THEN` ran the `THEN` branch. One rule now covers both, and a condition that is not a boolean
is reported rather than guessed at.

## `&&` and `||` stop as soon as the answer is known

A false left operand of `&&` returns false, and the right operand is never evaluated. A true left
operand of `||` returns true, with the same effect. This is what makes the ordinary guard work:

```
IF d != 0 && n / d > 1 THEN … END
```

With `d` equal to zero the division never runs. The operand that is skipped is not type-checked either,
so `false && 1` is false rather than an error.

Expression evaluation has no side effect a script can depend on, so a skipped operand changes only the
work avoided and the errors not raised.

## Division by zero is an error for every numeric type

`1 / 0` and `1.0 / 0.0` both raise a script error that names the line. Neither produces an infinity, and
neither leaks a runtime exception to the caller. A zero divisor written as a numeric string, such as
`1 / '0'`, is the same error.

## Numeric equality is exact

`==` and `!=` compare numbers exactly.

```
RETURN 1 == 1.0009        -- false
RETURN 1 == 1.0           -- true
```

An earlier version compared doubles within a fixed tolerance of 0.001, so two clearly different values
read as equal and a script had no way to ask for an exact answer. That is a trap for counters, revisions,
and any quantity that must match precisely.

A script that wants a tolerance states its own:

```
RETURN nearly_equals(1, 1.0009, 0.001)     -- true
RETURN nearly_equals(1, 1.0009, 0.0001)    -- false
```

`nearly_equals(a, b, tolerance)` takes two numbers and a non-negative tolerance, and is true when the two
differ by no more than that tolerance.

## Ranges include both bounds

`a..b` is every integer from `a` through `b`, both included. `10..15` is the six values 10, 11, 12, 13,
14 and 15.

A start above the end is an empty range rather than an error, so a loop over `0..n-1` runs no iterations
when `n` is zero:

```
FOR i IN 0..count(items)-1 DO … END
```

A range is materialized when it is evaluated, so its length is capped at 100,000 elements. A larger range
is a script error. Iterate in batches if you genuinely need more.

## Literals and unary minus

An integer literal is written in decimal or in hexadecimal with a `0x` prefix. `0x1A` is 26. A
hexadecimal literal is a bit pattern, so one with the top bit set reads back negative, exactly as the
same literal does in C#.

A minus sign is an operator, never part of the literal. This means `5-3` is a subtraction whether or not
it is padded with spaces, and a value or a variable can be negated:

```
RETURN 5-3        -- 2
RETURN -5         -- -5
RETURN -x         -- negates the variable
RETURN -2 * 3     -- -6, because unary minus binds tighter than multiplication
```

A literal that is too large for its type is a script error naming the line, not an overflow exception.

## Every character must be recognized

A character that starts no token is a script error that names the character and its column. Nothing is
silently dropped:

```
SET a 1; SET b 2      -- error: Unexpected character ';'
LET x = 5 % 2         -- error: Unexpected character '%'
```

Statements are separated by nothing but whitespace; there is no statement terminator. When a script has
both a rejected character and a syntax error that follows from it, the character is reported, because it
is the cause and the syntax error is the symptom.

## String escapes

A backslash escape inside a string literal is decoded. Until now the scanner recognized these sequences
but passed them through unchanged, so `"a\nb"` was the four characters `a`, `\`, `n`, `b`, and a value
could not hold a line break at all: the escape did nothing, and a raw control character is not accepted
inside a literal.

These are the recognized escapes:

| Escape | Character |
| --- | --- |
| `\n` `\t` `\r` | line feed, tab, carriage return |
| `\a` `\b` `\f` `\v` `\0` | bell, backspace, form feed, vertical tab, null |
| `\\` | one backslash |
| `\"` `\'` `` \` `` | the matching quote or backtick |
| `\NNN` | the character with that one-to-three-digit octal code, so `\0` is null and `\101` is `A` |
| `\xHH` | the character with that two-digit hexadecimal code |
| `\uHHHH` | the character with that four-digit code |
| `\UHHHHHHHH` | the character with that eight-digit code |

Both quote forms and the backtick identifier form decode the same way.

Any other escape is a script error. Whether `\q` was meant as `q` or as `\q` is unknowable, and either
guess is wrong half the time. A literal that holds no backslash is untouched, and costs nothing.

## Script size limits

Two limits bound what one request may submit. Both are configurable, and both defaults sit far above any
script a person writes.

| Limit | Default | Option |
| --- | --- | --- |
| Script length, in bytes | 65536 | `--max-script-length` |
| Syntax tree depth | 256 | `--max-script-depth` |

An over-length script is refused before it is parsed. A script past the depth limit is a script error
that names the line.

One depth number covers both shapes that reach the limit, because a statement list is left-recursive:
a flat run of statements is itself a deep spine whose depth is the statement count, exactly as a chain of
operators is deep. Every walker over the tree uses one call frame per level, so an unbounded tree let a
single request exhaust the stack and abort the whole node. Raising the limit trades that margin away.

## `BEGIN` options

`BEGIN` takes a comma-separated option list, and every option in it applies:

```
BEGIN (locking=optimistic, timeout=20000)
  …
END
```

An earlier version discarded every option of a list holding two or more, so such a transaction ran on
defaults: `BEGIN (locking=optimistic, timeout=20000)` actually ran pessimistic with the default timeout.
Check any script that relies on a multi-option `BEGIN`, since it was not doing what it said.

Repeating an option is a script error. Which of two values the author meant is unknowable, and a
silently discarded option is the failure the option list exists to avoid.

`timeout` must be greater than zero. Zero is refused rather than read as "no limit": a transaction holds
locks and an admission slot for as long as it runs, and the deadline is the only thing that ends one that
never completes.

`admissionWait` is the separate budget for queueing to start, as distinct from `timeout`, which is how
long the transaction may then run. An explicit `admissionWait=0` means "start only if a slot is free
right now" and gives up immediately otherwise; omitting the option takes the operator's default. An
earlier version mapped an explicit zero onto the default, so a caller could not opt out of queueing at
all. A negative value is a script error.

## Array indexing

An index may be a whole number, a double holding a whole number, or a string holding one. A helper
already converted all three, but the bounds check inspected the original expression rather than the
converted index, so every non-integer subscript was reported as out of range.

A fractional index such as `arr[1.9]` is a script error rather than being truncated to `arr[1]`, and an
empty string is a script error rather than being read as `arr[0]`. Both are far more likely to be an
arithmetic mistake or an unset variable than a deliberate request for that element.

## Statements a transaction refuses

`SCAN BY PREFIX` and `ESCAN BY PREFIX` run outside a transaction only. Inside `BEGIN … END` they are a
script error that names the line.

A prefix scan fans out across every partition and carries no transaction identity, so within a
transaction it would read a snapshot blind to the transaction's own uncommitted writes, and it would take
no prefix lock to make the read repeatable. Refusing it is honest; returning a result that quietly
violates both properties is not.

`GET BY BUCKET` is the prefix read that does work inside a transaction. It carries the transaction id, so
it sees the transaction's own writes and records what it read.
