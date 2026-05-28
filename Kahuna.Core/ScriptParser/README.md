# ScriptParser

ScriptParser contains the Kahuna script language scanner, parser, AST models, parser cache, and cache eviction actor.

Generated files are produced from:

- `ScriptParser.Language.grammar.y`
- `ScriptParser.Language.analyzer.lex`

The parser produces `NodeAst` trees that transaction command classes execute through `KeyValues/Transactions`. Syntax and token changes usually require updates to the grammar, lexer, AST node types, and transaction commands together.

Avoid hand-editing generated parser/scanner output unless the generation toolchain is unavailable and the change is intentionally checked in.
