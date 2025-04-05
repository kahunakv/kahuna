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
TSleep          (S|s)(L|l)(E|e)(E|e)(P|p)
TDelete         (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
TEDelete        (E|e)(D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
TExtend         (E|e)(X|x)(T|t)(E|e)(N|n)(D|d)
TEExtend        (E|e)(E|e)(X|x)(T|t)(E|e)(N|n)(D|d)
TExists         (E|e)(X|x)(I|i)(S|s)(T|t)(S|s)
TEExists        (E|e)(E|e)(X|x)(I|i)(S|s)(T|t)(S|s)
TTrue           (T|t)(R|r)(U|u)(E|e)
TFalse          (F|f)(A|a)(L|l)(S|s)(E|e)
TThrow          (T|t)(H|h)(R|r)(O|o)(W|w)
TFound          (F|f)(O|o)(U|u)(N|n)(D|d)
TAtWord         (A|a)(T|t)
TNotWord        (N|n)(O|o)(T|t)
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
EscIdentifier   (`)({StrChs2}|{EscChr}|{OctEsc}|{HexEsc}|{UniEsc}|{UNIESC})*(`)
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
TNot            !
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

{Number}		{ yylval.l = yyline; yylval.s = yytext; return (int)Token.TDIGIT; }

{Decimal}		{ yylval.l = yyline; yylval.s = yytext; return (int)Token.TFLOAT; }

{String}		{ yylval.l = yyline; yylval.s = yytext.Trim('\"'); return (int)Token.TSTRING; }

{StringSingle}  { yylval.l = yyline; yylval.s = yytext.Trim('\''); return (int)Token.TSTRING; }

{Space}+		/* skip */

{LParen} { yylval.l = yyline; return (int)Token.LPAREN; }

{RParen} { yylval.l = yyline; return (int)Token.RPAREN; }

{LBrace} { yylval.l = yyline; return (int)Token.LBRACE; }

{RBrace} { yylval.l = yyline; return (int)Token.RBRACE; }

{TLet} { yylval.l = yyline; return (int)Token.TLET; }

{TGet} { yylval.l = yyline; return (int)Token.TGET; }

{TSet} { yylval.l = yyline; return (int)Token.TSET; }

{TEget} { yylval.l = yyline; return (int)Token.TEGET; }

{TEset} { yylval.l = yyline; return (int)Token.TESET; }

{TDelete} { yylval.l = yyline; return (int)Token.TDELETE; }

{TEDelete} { yylval.l = yyline; return (int)Token.TEDELETE; }

{TExists} { yylval.l = yyline; return (int)Token.TEXISTS; }

{TEExists} { yylval.l = yyline; return (int)Token.TEEXISTS; }

{TExtend} { yylval.l = yyline; return (int)Token.TEXTEND; }

{TEExtend} { yylval.l = yyline; return (int)Token.TEEXTEND; }

{TIf} { yylval.l = yyline; return (int)Token.TIF; }

{TElse} { yylval.l = yyline; return (int)Token.TELSE; }

{TThen} { yylval.l = yyline; return (int)Token.TTHEN; }

{TEnd} { yylval.l = yyline; return (int)Token.TEND; }

{TBegin} { yylval.l = yyline; return (int)Token.TBEGIN; }

{TRollback} { yylval.l = yyline; return (int)Token.TROLLBACK; }

{TCommit} { yylval.l = yyline; return (int)Token.TCOMMIT; }

{TTrue} { yylval.l = yyline; return (int)Token.TTRUE; }

{TFalse} { yylval.l = yyline; return (int)Token.TFALSE; }

{TNx} { yylval.l = yyline; return (int)Token.TNX; }

{TXx} { yylval.l = yyline; return (int)Token.TXX; }

{TEx} { yylval.l = yyline; return (int)Token.TEX; }

{TCmp} { yylval.l = yyline; return (int)Token.TCMP; }

{TCmpRev} { yylval.l = yyline; return (int)Token.TCMPREV; }

{TReturn} { yylval.l = yyline; return (int)Token.TRETURN; }

{TSleep} { yylval.l = yyline; return (int)Token.TSLEEP; }

{TThrow} { yylval.l = yyline; return (int)Token.TTHROW; }

{TFound} { yylval.l = yyline; return (int)Token.TFOUND; }

{TEquals} { yylval.l = yyline; return (int)Token.TEQUALS; }

{TGreater} { yylval.l = yyline; return (int)Token.TGREATERTHAN; }

{TGreaterEquals} { yylval.l = yyline; return (int)Token.TGREATERTHANEQUALS; }

{TAdd} { yylval.l = yyline; return (int)Token.TADD; }

{TMinus} { yylval.l = yyline; return (int)Token.TMINUS; }

{TMult} { yylval.l = yyline; return (int)Token.TMULT; }

{TDiv} { yylval.l = yyline; return (int)Token.TDIV; }

{TOr} { yylval.l = yyline; return (int)Token.TOR; }

{TAnd} { yylval.l = yyline; return (int)Token.TAND; }

{TLess} { yylval.l = yyline; return (int)Token.TLESSTHAN; }

{TNot} { yylval.l = yyline; return (int)Token.TNOT; }

{TNotWord} { yylval.l = yyline; return (int)Token.TNOT; }

{TLessEquals} { yylval.l = yyline; return (int)Token.TLESSTHANEQUALS; }

{TNotEquals} { yylval.l = yyline; return (int)Token.TNOTEQUALS; }

{TNotEquals2} { yylval.l = yyline; return (int)Token.TNOTEQUALS; }

{TAt} { yylval.l = yyline; return (int)Token.TAT; }

{TAtWord} { yylval.l = yyline; return (int)Token.TAT; }

{Identifier} { yylval.l = yyline; yylval.s = yytext; return (int)Token.TIDENTIFIER; }

{EscIdentifier} { yylval.l = yyline; yylval.s = yytext.Trim('`'); return (int)Token.TIDENTIFIER; }

{Placeholder} { yylval.l = yyline; yylval.s = yytext; return (int)Token.TPLACEHOLDER; }

%%