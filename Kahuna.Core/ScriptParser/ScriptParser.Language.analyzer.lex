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
TFor            (F|f)(O|o)(R|r)
TDo             (D|d)(O|o)
TIn             (I|i)(N|n)
TNx             (N|n)(X|x)
TXx             (X|x)(X|x)
TEx             (E|e)(X|x)
TCmp            (C|c)(M|m)(P|p)
TCmpRev         (C|c)(M|m)(P|p)(R|r)(E|e)(V|v)
TNoRev          (N|n)(O|o)(R|r)(E|e)(V|v)
TBegin          (B|b)(E|e)(G|g)(I|i)(N|n)
TCommit         (C|c)(O|o)(M|m)(M|m)(I|i)(T|t)
TRollback       (R|r)(O|o)(L|l)(L|l)(B|b)(A|a)(C|c)(K|k)
TReturn         (R|r)(E|e)(T|t)(U|u)(R|r)(N|n)
TSleep          (S|s)(L|l)(E|e)(E|e)(P|p)
TDelete         (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
TDel            (D|d)(E|e)(L|l)
TEDelete        (E|e)(D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
TEDel           (E|e)(D|d)(E|e)(L|l)
TExtend         (E|e)(X|x)(T|t)(E|e)(N|n)(D|d)
TEExtend        (E|e)(E|e)(X|x)(T|t)(E|e)(N|n)(D|d)
TExists         (E|e)(X|x)(I|i)(S|s)(T|t)(S|s)
TEExists        (E|e)(E|e)(X|x)(I|i)(S|s)(T|t)(S|s)
TTrue           (T|t)(R|r)(U|u)(E|e)
TFalse          (F|f)(A|a)(L|l)(S|s)(E|e)
TThrow          (T|t)(H|h)(R|r)(O|o)(W|w)
TFound          (F|f)(O|o)(U|u)(N|n)(D|d)
TNull           (N|n)(U|u)(L|l)(L|l)
TAtWord         (A|a)(T|t)
TAs             (A|a)(S|s)
TOf             (O|o)(F|f)
TNotWord        (N|n)(O|o)(T|t)
TScan           (S|s)(C|c)(A|a)(N|n)
TEScan          (E|e)(S|s)(C|c)(A|a)(N|n)
TBy             (B|b)(Y|y)
TBucket         (B|b)(U|u)(C|c)(K|k)(E|e)(T|t)
TPrefix         (P|p)(R|r)(E|e)(F|f)(I|i)(X|x)
LParen          \(
RParen          \)
LBrace          \{
RBrace          \}
LSquareBrace    \[
RSquareBrace    \]
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
TDoubleDot      \.\.
TDoubleEquals   ==
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

{Number}		{ SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; yylval.s = yytext; return (int)Token.TDIGIT; }

{Decimal}		{ SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; yylval.s = yytext; return (int)Token.TFLOAT; }

{String}		{ SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; yylval.s = yytext.Trim('\"'); return (int)Token.TSTRING; }

{StringSingle}  { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; yylval.s = yytext.Trim('\''); return (int)Token.TSTRING; }

{Space}+		/* skip */

{LParen} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.LPAREN; }

{RParen} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.RPAREN; }

{LBrace} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.LBRACE; }

{RBrace} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.RBRACE; }

{LSquareBrace} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.LSQUAREBRACE; }

{RSquareBrace} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.RSQUAREBRACE; }

{TComma} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TCOMMA; }

{TLet} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TLET; }

{TGet} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TGET; }

{TSet} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TSET; }

{TEget} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEGET; }

{TEset} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TESET; }

{TDelete} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TDELETE; }

{TDel} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TDELETE; }

{TEDelete} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEDELETE; }

{TEDel} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEDELETE; }

{TExists} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEXISTS; }

{TEExists} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEEXISTS; }

{TExtend} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEXTEND; }

{TEExtend} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEEXTEND; }

{TIf} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TIF; }

{TElse} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TELSE; }

{TThen} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TTHEN; }

{TEnd} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEND; }

{TFor} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TFOR; }

{TDo} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TDO; }

{TIn} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TIN; }

{TBegin} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TBEGIN; }

{TRollback} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TROLLBACK; }

{TCommit} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TCOMMIT; }

{TTrue} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TTRUE; }

{TFalse} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TFALSE; }

{TNx} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TNX; }

{TXx} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TXX; }

{TEx} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEX; }

{TCmp} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TCMP; }

{TCmpRev} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TCMPREV; }

{TNoRev} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TNOREV; }

{TReturn} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TRETURN; }

{TSleep} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TSLEEP; }

{TThrow} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TTHROW; }

{TFound} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TFOUND; }

{TNull} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TNULL; }

{TEquals} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TEQUALS; }

{TDoubleEquals} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TDOUBLEEQUALS; }

{TGreater} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TGREATERTHAN; }

{TGreaterEquals} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TGREATERTHANEQUALS; }

{TAdd} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TADD; }

{TMinus} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TMINUS; }

{TMult} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TMULT; }

{TDiv} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TDIV; }

{TOr} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TOR; }

{TAnd} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TAND; }

{TLess} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TLESSTHAN; }

{TNot} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TNOT; }

{TNotWord} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TNOT; }

{TLessEquals} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TLESSTHANEQUALS; }

{TNotEquals} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TNOTEQUALS; }

{TNotEquals2} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TNOTEQUALS; }

{TDoubleDot} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TDOUBLEDOT; }

{TAt} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TAT; }

{TAtWord} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TAT; }

{TAs} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TAS; }

{TOf} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TOF; }

{TScan} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TSCAN; }

{TEScan} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TESCAN; }

{TBy} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TBY; }

{TPrefix} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TPREFIX; }

{TBucket} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; return (int)Token.TBUCKET; }

{Identifier} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; yylval.s = yytext; return (int)Token.TIDENTIFIER; }

{EscIdentifier} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; yylval.s = yytext.Trim('`'); return (int)Token.TIDENTIFIER; }

{Placeholder} { SetTokenLocation(yyline, yycol, yyleng); yylval.l = yyline; yylval.s = yytext; return (int)Token.TPLACEHOLDER; }

%%