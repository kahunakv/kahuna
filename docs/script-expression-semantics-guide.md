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

The accepted options are:

| Option | Values | Default |
|---|---|---|
| `locking` | `pessimistic`, `optimistic` | `pessimistic` |
| `autoCommit` | `true`, `false`, `yes`, `no` | `false` |
| `asyncRelease` | `true`, `false`, `yes`, `no` | `false` |
| `timeout` | milliseconds, greater than zero | the server's default transaction timeout |
| `admissionWait` | milliseconds, zero or more | the server's default admission wait |
| `snapshot` | non-zero Unix epoch milliseconds | none (reads see the latest committed state) |
| `priority` | `background`, `low`, `normal`, `high`, `critical` | the priority the request carried |
| `readValidation` | `none`, `trackAndValidate` | `none` |
| `decisionDurability` | `bestEffort`, `durable` | `bestEffort` |

Option names and values are case-sensitive. An unknown name is a script error: an earlier version
dropped it, so `BEGIN (lockng=optimistic)` or `BEGIN (Locking=optimistic)` ran pessimistic with no
sign that anything was wrong.

Repeating an option is a script error. Which of two values the author meant is unknowable, and a
silently discarded option is the failure the option list exists to avoid.

`timeout` must be greater than zero. Zero is refused rather than read as "no limit": a transaction holds
locks and an admission slot for as long as it runs, and the deadline is the only thing that ends one that
never completes. A value above the server's maximum transaction timeout is clamped to that maximum, the
same as for an interactive transaction. An earlier version let a script run for whatever it asked.

`readValidation=trackAndValidate` checks every key the script read against concurrent writes at commit,
and aborts the transaction if one of them changed. An optimistic transaction always does this, whatever
the option says; the option adds the check to a pessimistic one. It cannot be combined with `snapshot`:
a read pinned to a past timestamp cannot see a write that lands after it, so the check would promise a
guarantee it cannot keep.

`decisionDurability=durable` writes the commit decision to durable storage before the script returns.
A durable transaction cannot modify an ephemeral key, so an `ESET`, `EDELETE` or `EEXTEND` that is part of
the commit aborts it.

A script transaction cannot be a yielding transaction. See the yielding transactions guide.

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

## String functions

Seven functions read inside a string. Every one of them compares characters ordinally: it compares the
characters themselves, never a locale's idea of how they sort or fold. Keys, identifiers and packed field
values are not language, and a culture-aware comparison would make one script answer differently on two
nodes whose locales differ.

| Function | Result |
| --- | --- |
| `substring(s, start)` | the rest of `s` from `start` |
| `substring(s, start, length)` | `length` characters of `s` from `start` |
| `starts_with(s, prefix)` | `true` when `s` begins with `prefix` |
| `ends_with(s, suffix)` | `true` when `s` ends with `suffix` |
| `index_of(s, needle)` | the position of the first `needle`, counted from zero, or `-1` |
| `split(s, separator)` | an array of the parts between the separators |
| `trim(s)` | `s` without its leading and trailing whitespace |

Every argument must be a string. A number or a boolean where a string belongs is a script error, which is
the rule `concat`, `upper` and `length` already follow.

Positions count characters from zero. A start equal to the length of the string is the position just past
the last character, and it reads as the empty string, so this works when the separator is the last
character:

```
RETURN substring('a:b:', index_of('a:b:', ':') + 1)   -- 'b:'
RETURN substring('hello', 5)                          -- ''
```

A start past that point, a length that runs past the end, a negative length, and a fractional position are
all script errors that name the line:

```
RETURN substring('hello', 6)        -- error: Start index must be between 0 and 5
RETURN substring('hello', 2, 4)     -- error: Length must not exceed the 3 characters after index 2
RETURN substring('hello', 1, -1)    -- error: Length must not be negative
RETURN substring('hello', 1.5)      -- error: Start index must be a whole number
```

The refusal is deliberate, for the same reason a fractional subscript is refused. A clamp would answer
with a part of the string the arithmetic never asked for, and the author would read a wrong value instead
of seeing the mistake that produced it.

`split` returns the array type the language already has, the one `GET BY BUCKET` and the range operator
produce, so `count()`, subscripting and `FOR … IN` all accept it:

```
LET parts = split(raw, ':')
LET epoch = to_int(parts[0])
FOR p IN split(raw, ':') DO … END
```

Four rules fix what `split` returns:

1. A string with no separator in it is one part, not an empty array.
2. A split of the empty string is one empty part.
3. Every empty part is kept. A leading separator, a trailing separator and two adjacent separators each
   produce an empty part, so the position of a field never depends on whether an earlier field was empty.
4. An empty separator is a script error. One part per character and no cut at all are both plausible
   readings of it, and either guess is wrong half the time.

The parts are counted before any of them is built, so the array is sized once and a string that would
produce more than 100,000 parts is refused before the list is allocated. That is the same limit a range
is held to, for the same reason.

## The cluster clock: `hlc()` and `current_time()`

`current_time()` is the wall clock of the node that ran the script, in milliseconds since the unix epoch.
`hlc()` is the physical component of the cluster's hybrid logical clock, on the same scale.
`hlc_counter()` is the logical counter of the same reading, which separates events that fall inside one
millisecond.

The rule is short. Any comparison that decides an order, an expiry or a deadline across nodes reads
`hlc()`. A stamp a person will read is what `current_time()` is for.

```
SET lease/{@id} hlc() + 30000          -- a deadline other nodes will compare
SET audit/{@id} current_time()         -- a stamp for a human to read
```

The difference matters because two nodes' wall clocks disagree, and a script does not choose which node
answers it. A deadline written on one node and compared on another is safe only while one node answers
every call for that key, and it changes clock source silently on a leader change. The hybrid logical clock
is the one both nodes advance, so a reading taken after another reading is never below it, wherever each
was taken.

One script execution observes one reading. Two calls to `hlc()` in one script return the same value, and
`hlc_counter()` always describes the same instant `hlc()` does, so a script cannot pair the milliseconds of
one timestamp with the counter of another. Reading the clock also advances it: a timestamp a node hands out
but does not record could be minted again, and two events that share one timestamp cannot be ordered.

Both functions take no argument. An argument is a script error that names the line.

`current_time()` is unchanged and is not deprecated.

## Statements a transaction refuses

`SCAN BY PREFIX` and `ESCAN BY PREFIX` run outside a transaction only. Inside `BEGIN … END` they are a
script error that names the line.

A prefix scan fans out across every partition and carries no transaction identity, so within a
transaction it would read a snapshot blind to the transaction's own uncommitted writes, and it would take
no prefix lock to make the read repeatable. Refusing it is honest; returning a result that quietly
violates both properties is not.

`GET BY BUCKET` is the prefix read that does work inside a transaction. It carries the transaction id, so
it sees the transaction's own writes and records what it read.

## User-defined functions

A deployment can extend the language with its own C# functions. A registered function is called
exactly like a built-in:

```
LET total = acme_price_with_tax(@amount, 'ES')
SET users/{@id}/checksum acme_crc32(@payload)
```

Three rules apply to every call site.

1. **Built-in names are reserved.** A registration under `abs`, `concat`, `to_int` or any other
   built-in name — including every alias, such as `to_integer` and `to_long` for the same function —
   is refused when the node starts. A script's built-ins therefore mean one fixed thing on every
   node, whatever a deployment registers.
2. **An unknown function is a deterministic `Errored`.** It is never `MustRetry` and never `Aborted`,
   so a client does not retry it. The message names the node and the fingerprint of its registered
   set, because the usual cause is one node of a cluster that loaded a different extension build.
3. **A function cannot appear in key position.** `SET acme_key() 'v'` is a syntax error. A key is an
   identifier, a quoted string or a placeholder, which is what lets a transaction plan its locks
   before it runs any expression.

A function that fails — it throws, or it receives the wrong number of arguments — rolls its
transaction back and returns `Errored` with the function named. Nothing is written and no lock is
held afterwards.

See the user-defined functions guide for how to write and install one.
