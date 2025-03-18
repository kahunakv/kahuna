// This code was generated by the Gardens Point Parser Generator
// Copyright (c) Wayne Kelly, John Gough, QUT 2005-2014
// (see accompanying GPPGcopyright.rtf)

// GPPG version 1.5.3
// DateTime: 3/18/2025 1:35:49 PM
// Input file <ScriptParser/ScriptParser.Language.grammar.y - 3/18/2025 1:35:44 PM>

// options: no-lines gplex

using System;
using System.Collections.Generic;
using System.CodeDom.Compiler;
using System.Globalization;
using System.Text;
using QUT.Gppg;

namespace Kahuna.Server.ScriptParser
{
internal enum Token {error=2,EOF=3,TOR=4,TAND=5,TLIKE=6,
    TILIKE=7,TEQUALS=8,TNOTEQUALS=9,TLESSTHAN=10,TGREATERTHAN=11,TLESSTHANEQUALS=12,
    TGREATERTHANEQUALS=13,TADD=14,TMINUS=15,TMULT=16,LPAREN=17,RPAREN=18,
    TCOMMA=19,TDIV=20,LBRACE=21,RBRACE=22,TBEGIN=23,TROLLBACK=24,
    TCOMMIT=25,TSET=26,TGET=27,TESET=28,TEGET=29,TIF=30,
    TELSE=31,TTHEN=32,TEND=33,TNX=34,TXX=35,TEX=36,
    TRETURN=37,TDIGIT=38,TFLOAT=39,TSTRING=40,TIDENTIFIER=41,TPLACEHOLDER=42};

internal partial struct ValueType
{ 
        public NodeAst n;
        public string s;
}
// Abstract base class for GPLEX scanners
[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.3")]
internal abstract class ScanBase : AbstractScanner<ValueType,LexLocation> {
  private LexLocation __yylloc = new LexLocation();
  public override LexLocation yylloc { get { return __yylloc; } set { __yylloc = value; } }
  protected virtual bool yywrap() { return true; }
}

// Utility class for encapsulating token information
[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.3")]
internal class ScanObj {
  public int token;
  public ValueType yylval;
  public LexLocation yylloc;
  public ScanObj( int t, ValueType val, LexLocation loc ) {
    this.token = t; this.yylval = val; this.yylloc = loc;
  }
}

[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.3")]
internal partial class scriptParser: ShiftReduceParser<ValueType, LexLocation>
{
#pragma warning disable 649
  private static Dictionary<int, string> aliases;
#pragma warning restore 649
  private static Rule[] rules = new Rule[48];
  private static State[] states = new State[77];
  private static string[] nonTerms = new string[] {
      "stmt_list", "$accept", "stmt", "set_stmt", "eset_stmt", "get_stmt", "eget_stmt", 
      "if_stmt", "begin_stmt", "commit_stmt", "rollback_stmt", "return_stmt", 
      "identifier", "expression", "int", "set_not_exists", "set_value", "string", 
      };

  static scriptParser() {
    states[0] = new State(new int[]{26,5,28,40,27,48,41,35,29,57,30,60,23,66,25,70,24,72,37,74},new int[]{-1,1,-3,76,-4,4,-5,39,-6,47,-13,50,-7,56,-8,59,-9,65,-10,69,-11,71,-12,73});
    states[1] = new State(new int[]{3,2,26,5,28,40,27,48,41,35,29,57,30,60,23,66,25,70,24,72,37,74},new int[]{-3,3,-4,4,-5,39,-6,47,-13,50,-7,56,-8,59,-9,65,-10,69,-11,71,-12,73});
    states[2] = new State(-1);
    states[3] = new State(-2);
    states[4] = new State(-4);
    states[5] = new State(new int[]{41,35},new int[]{-13,6});
    states[6] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,7,-13,34,-15,36,-18,37});
    states[7] = new State(new int[]{36,8,8,15,9,17,10,19,11,21,12,23,13,25,5,27,4,29,34,11,35,12,3,-13,26,-13,28,-13,27,-13,41,-13,29,-13,30,-13,23,-13,25,-13,24,-13,37,-13,33,-13},new int[]{-16,14});
    states[8] = new State(new int[]{38,13},new int[]{-15,9});
    states[9] = new State(new int[]{34,11,35,12,3,-14,26,-14,28,-14,27,-14,41,-14,29,-14,30,-14,23,-14,25,-14,24,-14,37,-14,33,-14},new int[]{-16,10});
    states[10] = new State(-15);
    states[11] = new State(-17);
    states[12] = new State(-18);
    states[13] = new State(-46);
    states[14] = new State(-16);
    states[15] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,16,-13,34,-15,36,-18,37});
    states[16] = new State(new int[]{8,-33,9,-33,10,19,11,21,12,23,13,25,5,-33,4,-33,36,-33,34,-33,35,-33,3,-33,26,-33,28,-33,27,-33,41,-33,29,-33,30,-33,23,-33,25,-33,24,-33,37,-33,33,-33,18,-33,32,-33});
    states[17] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,18,-13,34,-15,36,-18,37});
    states[18] = new State(new int[]{8,-34,9,-34,10,19,11,21,12,23,13,25,5,-34,4,-34,36,-34,34,-34,35,-34,3,-34,26,-34,28,-34,27,-34,41,-34,29,-34,30,-34,23,-34,25,-34,24,-34,37,-34,33,-34,18,-34,32,-34});
    states[19] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,20,-13,34,-15,36,-18,37});
    states[20] = new State(-35);
    states[21] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,22,-13,34,-15,36,-18,37});
    states[22] = new State(-36);
    states[23] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,24,-13,34,-15,36,-18,37});
    states[24] = new State(-37);
    states[25] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,26,-13,34,-15,36,-18,37});
    states[26] = new State(-38);
    states[27] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,28,-13,34,-15,36,-18,37});
    states[28] = new State(new int[]{8,15,9,17,10,19,11,21,12,23,13,25,5,-39,4,-39,36,-39,34,-39,35,-39,3,-39,26,-39,28,-39,27,-39,41,-39,29,-39,30,-39,23,-39,25,-39,24,-39,37,-39,33,-39,18,-39,32,-39});
    states[29] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,30,-13,34,-15,36,-18,37});
    states[30] = new State(new int[]{8,15,9,17,10,19,11,21,12,23,13,25,5,27,4,-40,36,-40,34,-40,35,-40,3,-40,26,-40,28,-40,27,-40,41,-40,29,-40,30,-40,23,-40,25,-40,24,-40,37,-40,33,-40,18,-40,32,-40});
    states[31] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,32,-13,34,-15,36,-18,37});
    states[32] = new State(new int[]{18,33,8,15,9,17,10,19,11,21,12,23,13,25,5,27,4,29});
    states[33] = new State(-41);
    states[34] = new State(-42);
    states[35] = new State(-45);
    states[36] = new State(-43);
    states[37] = new State(-44);
    states[38] = new State(-47);
    states[39] = new State(-5);
    states[40] = new State(new int[]{41,35},new int[]{-13,41});
    states[41] = new State(new int[]{41,35,40,38,38,13},new int[]{-17,42,-13,44,-18,45,-15,46});
    states[42] = new State(new int[]{38,13,3,-19,26,-19,28,-19,27,-19,41,-19,29,-19,30,-19,23,-19,25,-19,24,-19,37,-19,33,-19},new int[]{-15,43});
    states[43] = new State(-20);
    states[44] = new State(-21);
    states[45] = new State(-22);
    states[46] = new State(-23);
    states[47] = new State(-6);
    states[48] = new State(new int[]{41,35},new int[]{-13,49});
    states[49] = new State(-24);
    states[50] = new State(new int[]{8,51});
    states[51] = new State(new int[]{27,52,29,54});
    states[52] = new State(new int[]{41,35},new int[]{-13,53});
    states[53] = new State(-25);
    states[54] = new State(new int[]{41,35},new int[]{-13,55});
    states[55] = new State(-27);
    states[56] = new State(-7);
    states[57] = new State(new int[]{41,35},new int[]{-13,58});
    states[58] = new State(-26);
    states[59] = new State(-8);
    states[60] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,61,-13,34,-15,36,-18,37});
    states[61] = new State(new int[]{32,62,8,15,9,17,10,19,11,21,12,23,13,25,5,27,4,29});
    states[62] = new State(new int[]{26,5,28,40,27,48,41,35,29,57,30,60,23,66,25,70,24,72,37,74},new int[]{-1,63,-3,76,-4,4,-5,39,-6,47,-13,50,-7,56,-8,59,-9,65,-10,69,-11,71,-12,73});
    states[63] = new State(new int[]{33,64,26,5,28,40,27,48,41,35,29,57,30,60,23,66,25,70,24,72,37,74},new int[]{-3,3,-4,4,-5,39,-6,47,-13,50,-7,56,-8,59,-9,65,-10,69,-11,71,-12,73});
    states[64] = new State(-28);
    states[65] = new State(-9);
    states[66] = new State(new int[]{26,5,28,40,27,48,41,35,29,57,30,60,23,66,25,70,24,72,37,74},new int[]{-1,67,-3,76,-4,4,-5,39,-6,47,-13,50,-7,56,-8,59,-9,65,-10,69,-11,71,-12,73});
    states[67] = new State(new int[]{33,68,26,5,28,40,27,48,41,35,29,57,30,60,23,66,25,70,24,72,37,74},new int[]{-3,3,-4,4,-5,39,-6,47,-13,50,-7,56,-8,59,-9,65,-10,69,-11,71,-12,73});
    states[68] = new State(-29);
    states[69] = new State(-10);
    states[70] = new State(-30);
    states[71] = new State(-11);
    states[72] = new State(-31);
    states[73] = new State(-12);
    states[74] = new State(new int[]{17,31,41,35,38,13,40,38},new int[]{-14,75,-13,34,-15,36,-18,37});
    states[75] = new State(new int[]{8,15,9,17,10,19,11,21,12,23,13,25,5,27,4,29,3,-32,26,-32,28,-32,27,-32,41,-32,29,-32,30,-32,23,-32,25,-32,24,-32,37,-32,33,-32});
    states[76] = new State(-3);

    for (int sNo = 0; sNo < states.Length; sNo++) states[sNo].number = sNo;

    rules[1] = new Rule(-2, new int[]{-1,3});
    rules[2] = new Rule(-1, new int[]{-1,-3});
    rules[3] = new Rule(-1, new int[]{-3});
    rules[4] = new Rule(-3, new int[]{-4});
    rules[5] = new Rule(-3, new int[]{-5});
    rules[6] = new Rule(-3, new int[]{-6});
    rules[7] = new Rule(-3, new int[]{-7});
    rules[8] = new Rule(-3, new int[]{-8});
    rules[9] = new Rule(-3, new int[]{-9});
    rules[10] = new Rule(-3, new int[]{-10});
    rules[11] = new Rule(-3, new int[]{-11});
    rules[12] = new Rule(-3, new int[]{-12});
    rules[13] = new Rule(-4, new int[]{26,-13,-14});
    rules[14] = new Rule(-4, new int[]{26,-13,-14,36,-15});
    rules[15] = new Rule(-4, new int[]{26,-13,-14,36,-15,-16});
    rules[16] = new Rule(-4, new int[]{26,-13,-14,-16});
    rules[17] = new Rule(-16, new int[]{34});
    rules[18] = new Rule(-16, new int[]{35});
    rules[19] = new Rule(-5, new int[]{28,-13,-17});
    rules[20] = new Rule(-5, new int[]{28,-13,-17,-15});
    rules[21] = new Rule(-17, new int[]{-13});
    rules[22] = new Rule(-17, new int[]{-18});
    rules[23] = new Rule(-17, new int[]{-15});
    rules[24] = new Rule(-6, new int[]{27,-13});
    rules[25] = new Rule(-6, new int[]{-13,8,27,-13});
    rules[26] = new Rule(-7, new int[]{29,-13});
    rules[27] = new Rule(-7, new int[]{-13,8,29,-13});
    rules[28] = new Rule(-8, new int[]{30,-14,32,-1,33});
    rules[29] = new Rule(-9, new int[]{23,-1,33});
    rules[30] = new Rule(-10, new int[]{25});
    rules[31] = new Rule(-11, new int[]{24});
    rules[32] = new Rule(-12, new int[]{37,-14});
    rules[33] = new Rule(-14, new int[]{-14,8,-14});
    rules[34] = new Rule(-14, new int[]{-14,9,-14});
    rules[35] = new Rule(-14, new int[]{-14,10,-14});
    rules[36] = new Rule(-14, new int[]{-14,11,-14});
    rules[37] = new Rule(-14, new int[]{-14,12,-14});
    rules[38] = new Rule(-14, new int[]{-14,13,-14});
    rules[39] = new Rule(-14, new int[]{-14,5,-14});
    rules[40] = new Rule(-14, new int[]{-14,4,-14});
    rules[41] = new Rule(-14, new int[]{17,-14,18});
    rules[42] = new Rule(-14, new int[]{-13});
    rules[43] = new Rule(-14, new int[]{-15});
    rules[44] = new Rule(-14, new int[]{-18});
    rules[45] = new Rule(-13, new int[]{41});
    rules[46] = new Rule(-15, new int[]{38});
    rules[47] = new Rule(-18, new int[]{40});
  }

  protected override void Initialize() {
    this.InitSpecialTokens((int)Token.error, (int)Token.EOF);
    this.InitStates(states);
    this.InitRules(rules);
    this.InitNonTerminals(nonTerms);
  }

  protected override void DoAction(int action)
  {
#pragma warning disable 162, 1522
    switch (action)
    {
      case 2: // stmt_list -> stmt_list, stmt
{ CurrentSemanticValue.n = new(NodeType.StmtList, ValueStack[ValueStack.Depth-2].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 3: // stmt_list -> stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 4: // stmt -> set_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 5: // stmt -> eset_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 6: // stmt -> get_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 7: // stmt -> eget_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 8: // stmt -> if_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 9: // stmt -> begin_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 10: // stmt -> commit_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 11: // stmt -> rollback_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 12: // stmt -> return_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 13: // set_stmt -> TSET, identifier, expression
{ CurrentSemanticValue.n = new(NodeType.Set, ValueStack[ValueStack.Depth-2].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 14: // set_stmt -> TSET, identifier, expression, TEX, int
{ CurrentSemanticValue.n = new(NodeType.Set, ValueStack[ValueStack.Depth-4].n, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-2].n, null, null, null, null); }
        break;
      case 15: // set_stmt -> TSET, identifier, expression, TEX, int, set_not_exists
{ CurrentSemanticValue.n = new(NodeType.Set, ValueStack[ValueStack.Depth-5].n, ValueStack[ValueStack.Depth-4].n, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-2].n, null, null, null); }
        break;
      case 16: // set_stmt -> TSET, identifier, expression, set_not_exists
{ CurrentSemanticValue.n = new(NodeType.Set, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-2].n, null, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 17: // set_not_exists -> TNX
{ CurrentSemanticValue.n = new(NodeType.SetNotExists, null, null, null, null, null, null, null); }
        break;
      case 18: // set_not_exists -> TXX
{ CurrentSemanticValue.n = new(NodeType.SetExists, null, null, null, null, null, null, null); }
        break;
      case 19: // eset_stmt -> TESET, identifier, set_value
{ CurrentSemanticValue.n = new(NodeType.Eset, ValueStack[ValueStack.Depth-2].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 20: // eset_stmt -> TESET, identifier, set_value, int
{ CurrentSemanticValue.n = new(NodeType.Eset, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-2].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null); }
        break;
      case 21: // set_value -> identifier
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 22: // set_value -> string
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 23: // set_value -> int
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 24: // get_stmt -> TGET, identifier
{ CurrentSemanticValue.n = new(NodeType.Get, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null, null); }
        break;
      case 25: // get_stmt -> identifier, TEQUALS, TGET, identifier
{ CurrentSemanticValue.n = new(NodeType.Get, ValueStack[ValueStack.Depth-1].n, ValueStack[ValueStack.Depth-4].n, null, null, null, null, null); }
        break;
      case 26: // eget_stmt -> TEGET, identifier
{ CurrentSemanticValue.n = new(NodeType.Eget, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null, null); }
        break;
      case 27: // eget_stmt -> identifier, TEQUALS, TEGET, identifier
{ CurrentSemanticValue.n = new(NodeType.Eget, ValueStack[ValueStack.Depth-1].n, ValueStack[ValueStack.Depth-4].n, null, null, null, null, null); }
        break;
      case 28: // if_stmt -> TIF, expression, TTHEN, stmt_list, TEND
{ CurrentSemanticValue.n = new(NodeType.If, ValueStack[ValueStack.Depth-4].n, ValueStack[ValueStack.Depth-2].n, null, null, null, null, null); }
        break;
      case 29: // begin_stmt -> TBEGIN, stmt_list, TEND
{ CurrentSemanticValue.n = new(NodeType.Begin, ValueStack[ValueStack.Depth-2].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 30: // commit_stmt -> TCOMMIT
{ CurrentSemanticValue.n = new(NodeType.Commit, null, null, null, null, null, null, null); }
        break;
      case 31: // rollback_stmt -> TROLLBACK
{ CurrentSemanticValue.n = new(NodeType.Rollback, null, null, null, null, null, null, null); }
        break;
      case 32: // return_stmt -> TRETURN, expression
{ CurrentSemanticValue.n = new(NodeType.Return, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null, null); }
        break;
      case 33: // expression -> expression, TEQUALS, expression
{ CurrentSemanticValue.n = new(NodeType.Equals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 34: // expression -> expression, TNOTEQUALS, expression
{ CurrentSemanticValue.n = new(NodeType.NotEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 35: // expression -> expression, TLESSTHAN, expression
{ CurrentSemanticValue.n = new(NodeType.LessThan, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 36: // expression -> expression, TGREATERTHAN, expression
{ CurrentSemanticValue.n = new(NodeType.GreaterThan, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 37: // expression -> expression, TLESSTHANEQUALS, expression
{ CurrentSemanticValue.n = new(NodeType.LessThanEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 38: // expression -> expression, TGREATERTHANEQUALS, expression
{ CurrentSemanticValue.n = new(NodeType.GreaterThanEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 39: // expression -> expression, TAND, expression
{ CurrentSemanticValue.n = new(NodeType.And, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 40: // expression -> expression, TOR, expression
{ CurrentSemanticValue.n = new(NodeType.Or, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null, null, null); }
        break;
      case 41: // expression -> LPAREN, expression, RPAREN
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-2].n; }
        break;
      case 42: // expression -> identifier
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 43: // expression -> int
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 44: // expression -> string
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 45: // identifier -> TIDENTIFIER
{ CurrentSemanticValue.n = new(NodeType.Identifier, null, null, null, null, null, null, CurrentSemanticValue.s); }
        break;
      case 46: // int -> TDIGIT
{ CurrentSemanticValue.n = new(NodeType.Integer, null, null, null, null, null, null, CurrentSemanticValue.s); }
        break;
      case 47: // string -> TSTRING
{ CurrentSemanticValue.n = new(NodeType.String, null, null, null, null, null, null, CurrentSemanticValue.s); }
        break;
    }
#pragma warning restore 162, 1522
  }

  protected override string TerminalToString(int terminal)
  {
    if (aliases != null && aliases.ContainsKey(terminal))
        return aliases[terminal];
    else if (((Token)terminal).ToString() != terminal.ToString(CultureInfo.InvariantCulture))
        return ((Token)terminal).ToString();
    else
        return CharToString((char)terminal);
  }

}
}
