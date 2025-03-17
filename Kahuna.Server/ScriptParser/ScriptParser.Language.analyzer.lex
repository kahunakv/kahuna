%namespace Kahuna.Server.ScriptParser
%scannertype scriptScanner
%visibility internal
%tokentype Token

%option stack, minimize, parser, verbose, persistbuffer, noembedbuffers

TSet            (S|s)(E|e)(T|t)
TGet            (G|g)(E|e)(T|t)
LParen          \(
RParen          \)
LBrace          \{
RBrace          \}
Eol             (\r\n?|\n)
NotWh           [^ \t\r\n]
Space           [ \t]
Number          ("-"?[0-9]+)|("-"?[0][x][0-9A-Fa-f]+)
Decimal         ("-"?)([0-9]+)(\.)([0-9]+)
StrChs          [^\\\"\a\b\f\n\r\t\v\0]
StrChs2          [^\\\'\a\b\f\n\r\t\v\0]
DotChr          [^\r\n]
EscChr          \\{DotChr}
OctDig          [0-7]
HexDig          [0-9a-fA-F]
OctEsc          \\{OctDig}{3}
HexEsc          \\x{HexDig}{2}
UniEsc          \\u{HexDig}{4}
UNIESC          \\U{HexDig}{8}
String          \"({StrChs}|{EscChr}|{OctEsc}|{HexEsc}|{UniEsc}|{UNIESC})*\"
StringSingle    \'({StrChs2}|{EscChr}|{OctEsc}|{HexEsc}|{UniEsc}|{UNIESC})*\'
Identifier      [a-zA-Z_][a-zA-Z0-9_]*
EscIdentifier   (`)[a-zA-Z_][a-zA-Z0-9_]*(`)
Placeholder     (@)([a-zA-Z0-9_]+)
TAt             @
TAdd            \+
TMult           \*
TMinus          \-
TDiv            /
TComma          ,
TEquals         =
TNotEquals      <>
TNotEquals2     !=
TLess           <
TGreater        >
TLessEquals     <=
TGreaterEquals  >=

%{

%}

%%

/* Scanner body */

{Number}		{ yylval.s = yytext; return (int)Token.TDIGIT; }

{Decimal}		{ yylval.s = yytext; return (int)Token.TFLOAT; }

{String}		{ yylval.s = yytext; return (int)Token.TSTRING; }

{StringSingle}  { yylval.s = yytext; return (int)Token.TSTRING; }

{Space}+		/* skip */

{LParen} { return (int)Token.LPAREN; }

{RParen} { return (int)Token.RPAREN; }

{LBrace} { return (int)Token.LBRACE; }

{RBrace} { return (int)Token.RBRACE; }

{TGet} { return (int)Token.TGET; }

{TSet} { return (int)Token.TSET; }

{TEquals} { return (int)Token.TEQUALS; }

{TGreater} { return (int)Token.TGREATERTHAN; }

{TGreaterEquals} { return (int)Token.TGREATERTHANEQUALS; }

{TLess} { return (int)Token.TLESSTHAN; }

{TLessEquals} { return (int)Token.TLESSTHANEQUALS; }

{TNotEquals} { return (int)Token.TNOTEQUALS; }

{TNotEquals2} { return (int)Token.TNOTEQUALS; }

{Identifier} { yylval.s = yytext; return (int)Token.TIDENTIFIER; }

{Placeholder} { yylval.s = yytext; return (int)Token.TPLACEHOLDER; }

%%