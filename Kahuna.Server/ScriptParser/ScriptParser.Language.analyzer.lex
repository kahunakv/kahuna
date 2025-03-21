%namespace Kahuna.Server.ScriptParser
%scannertype scriptScanner
%visibility internal
%tokentype Token

%option stack, minimize, parser, verbose, persistbuffer, noembedbuffers

TLet            (L|l)(E|e)(T|t)
TSet            (S|s)(E|e)(T|t)
TGet            (G|g)(E|e)(T|t)
TEset           (E|e)(S|s)(E|e)(T|t)
TEget           (E|e)(G|g)(E|e)(T|t)
TIf             (I|i)(F|f)
TThen           (T|t)(H|h)(E|e)(N|n)
TElse           (E|e)(L|l)(S|s)(E|e)
TEnd            (E|e)(N|n)(D|d)
TNx             (N|n)(X|x)
TXx             (X|x)(X|x)
TEx             (E|e)(X|x)
TCmp            (C|c)(M|m)(P|p)
TCmpRev         (C|c)(M|m)(P|p)(R|r)(E|e)(V|v)
TBegin          (B|b)(E|e)(G|g)(I|i)(N|n)
TCommit         (C|c)(O|o)(M|m)(M|m)(I|i)(T|t)
TRollback       (R|r)(O|o)(L|l)(L|l)(B|b)(A|a)(C|c)(K|k)
TReturn         (R|r)(E|e)(T|t)(U|u)(R|r)(N|n)
TDelete         (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
TEDelete        (E|e)(D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
TExtend         (E|e)(X|x)(T|t)(E|e)(N|n)(D|d)
TEExtend        (E|e)(E|e)(X|x)(T|t)(E|e)(N|n)(D|d)
TTrue           (T|t)(R|r)(U|u)(E|e)
TFalse          (F|f)(A|a)(L|l)(S|s)(E|e)
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
TOr             \|\|
TAnd            &&

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

{TLet} { return (int)Token.TLET; }

{TGet} { return (int)Token.TGET; }

{TSet} { return (int)Token.TSET; }

{TEget} { return (int)Token.TEGET; }

{TEset} { return (int)Token.TESET; }

{TDelete} { return (int)Token.TDELETE; }

{TEDelete} { return (int)Token.TEDELETE; }

{TExtend} { return (int)Token.TEXTEND; }

{TEExtend} { return (int)Token.TEEXTEND; }

{TIf} { return (int)Token.TIF; }

{TElse} { return (int)Token.TELSE; }

{TThen} { return (int)Token.TTHEN; }

{TEnd} { return (int)Token.TEND; }

{TBegin} { return (int)Token.TBEGIN; }

{TRollback} { return (int)Token.TROLLBACK; }

{TCommit} { return (int)Token.TCOMMIT; }

{TTrue} { return (int)Token.TTRUE; }

{TFalse} { return (int)Token.TFALSE; }

{TNx} { return (int)Token.TNX; }

{TXx} { return (int)Token.TXX; }

{TEx} { return (int)Token.TEX; }

{TCmp} { return (int)Token.TCMP; }

{TCmpRev} { return (int)Token.TCMPREV; }

{TReturn} { return (int)Token.TRETURN; }

{TEquals} { return (int)Token.TEQUALS; }

{TGreater} { return (int)Token.TGREATERTHAN; }

{TGreaterEquals} { return (int)Token.TGREATERTHANEQUALS; }

{TAdd} { return (int)Token.TADD; }

{TMinus} { return (int)Token.TMINUS; }

{TMult} { return (int)Token.TMULT; }

{TDiv} { return (int)Token.TDIV; }

{TOr} { return (int)Token.TOR; }

{TAnd} { return (int)Token.TAND; }

{TLess} { return (int)Token.TLESSTHAN; }

{TLessEquals} { return (int)Token.TLESSTHANEQUALS; }

{TNotEquals} { return (int)Token.TNOTEQUALS; }

{TNotEquals2} { return (int)Token.TNOTEQUALS; }

{Identifier} { yylval.s = yytext; return (int)Token.TIDENTIFIER; }

{EscIdentifier} { yylval.s = yytext; return (int)Token.TESCIDENTIFIER; }

{Placeholder} { yylval.s = yytext; return (int)Token.TPLACEHOLDER; }

%%