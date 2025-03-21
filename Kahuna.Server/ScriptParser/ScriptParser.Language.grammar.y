%namespace Kahuna.Server.ScriptParser
%partial
%parsertype scriptParser
%visibility internal
%tokentype Token

%union { 
        public NodeAst n;
        public string s;
}

%start stmt_list
%visibility internal

%left TOR
%left TAND
%left TLIKE TILIKE
%left TEQUALS TNOTEQUALS
%left TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS
%left TADD TMINUS
%left TMULT TDIV

%token LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV LBRACE RBRACE
%token TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS  
%token TBEGIN TROLLBACK TCOMMIT TLET TSET TGET TESET TEGET TDELETE TEDELETE TEXTEND TEEXTEND 
%token TIF TELSE TTHEN TEND TNX TXX TEX TCMP TCMPREV
%token TRETURN TDIGIT TFLOAT TSTRING TIDENTIFIER TESCIDENTIFIER TPLACEHOLDER TTRUE TFALSE 

%%

stmt_list : stmt_list stmt { $$.n = new(NodeType.StmtList, $1.n, $2.n, null, null, null, null, null); }
          | stmt { $$.n = $1.n; }
          ;

stmt    : set_stmt { $$.n = $1.n; }
        | eset_stmt { $$.n = $1.n; } 
        | get_stmt { $$.n = $1.n; }
        | eget_stmt { $$.n = $1.n; }
        | delete_stmt { $$.n = $1.n; }
        | edelete_stmt { $$.n = $1.n; }
        | extend_stmt { $$.n = $1.n; }
        | eextend_stmt { $$.n = $1.n; }
        | let_stmt { $$.n = $1.n; }
        | if_stmt { $$.n = $1.n; } 
        | begin_stmt { $$.n = $1.n; }
        | commit_stmt { $$.n = $1.n; }
        | rollback_stmt { $$.n = $1.n; }
        | return_stmt { $$.n = $1.n; }
        ;

set_stmt : TSET identifier expression { $$.n = new(NodeType.Set, $2.n, $3.n, null, null, null, null, null); }         
          | TSET identifier expression set_cmp { $$.n = new(NodeType.Set, $2.n, $3.n, null, null, $4.n, null, null); }
          | TSET identifier expression TEX int { $$.n = new(NodeType.Set, $2.n, $3.n, $5.n, null, null, null, null); }
          | TSET identifier expression set_cmp TEX int { $$.n = new(NodeType.Set, $2.n, $3.n, $6.n, null, $4.n, null, null); }
          | TSET identifier expression TEX int set_not_exists { $$.n = new(NodeType.Set, $2.n, $3.n, $5.n, $6.n, null, null, null); }
          | TSET identifier expression set_not_exists { $$.n = new(NodeType.Set, $2.n, $3.n, null, $4.n, null, null, null); }
          ; 
         
set_not_exists : TNX { $$.n = new(NodeType.SetNotExists, null, null, null, null, null, null, null); }
               | TXX { $$.n = new(NodeType.SetExists, null, null, null, null, null, null, null); }
               ;
               
set_cmp : TCMP expression { $$.n = new(NodeType.SetCmp, $2.n, null, null, null, null, null, null); }
        | TCMPREV expression { $$.n = new(NodeType.SetCmpRev, $2.n, null, null, null, null, null, null); }                 
        ;
         
eset_stmt : TESET identifier expression { $$.n = new(NodeType.Eset, $2.n, $3.n, null, null, null, null, null); }         
          | TESET identifier expression set_cmp { $$.n = new(NodeType.Eset, $2.n, $3.n, null, null, $4.n, null, null); }
          | TESET identifier expression TEX int { $$.n = new(NodeType.Eset, $2.n, $3.n, $5.n, null, null, null, null); }
          | TESET identifier expression set_cmp TEX int { $$.n = new(NodeType.Eset, $2.n, $3.n, $6.n, null, $4.n, null, null); }
          | TESET identifier expression TEX int set_not_exists { $$.n = new(NodeType.Eset, $2.n, $3.n, $5.n, $6.n, null, null, null); }
          | TESET identifier expression set_not_exists { $$.n = new(NodeType.Eset, $2.n, $3.n, null, $4.n, null, null, null); }
          ;                  
         
get_stmt : TGET identifier { $$.n = new(NodeType.Get, $2.n, null, null, null, null, null, null); }
         | TLET identifier TEQUALS TGET identifier { $$.n = new(NodeType.Get, $5.n, $2.n, null, null, null, null, null); }
         ;
         
eget_stmt : TEGET identifier { $$.n = new(NodeType.Eget, $2.n, null, null, null, null, null, null); }
          | TLET identifier TEQUALS TEGET identifier { $$.n = new(NodeType.Eget, $5.n, $2.n, null, null, null, null, null); }
          ;
          
delete_stmt : TDELETE identifier { $$.n = new(NodeType.Delete, $2.n, null, null, null, null, null, null); }
            ;
            
edelete_stmt : TEDELETE identifier { $$.n = new(NodeType.Edelete, $2.n, null, null, null, null, null, null); }
            ;
            
extend_stmt : TEXTEND identifier int { $$.n = new(NodeType.Extend, $2.n, $3.n, null, null, null, null, null); }
            ;
            
eextend_stmt : TEEXTEND identifier int { $$.n = new(NodeType.Extend, $2.n, $3.n, null, null, null, null, null); }
            ;
            
let_stmt : TLET identifier TEQUALS expression { $$.n = new(NodeType.Let, $2.n, $4.n, null, null, null, null, null); }                   
          ;                     
         
if_stmt : TIF expression TTHEN stmt_list TEND { $$.n = new(NodeType.If, $2.n, $4.n, null, null, null, null, null); }
        | TIF expression TTHEN stmt_list TELSE stmt_list TEND { $$.n = new(NodeType.If, $2.n, $4.n, $6.n, null, null, null, null); }
        ;
        
begin_stmt : TBEGIN stmt_list TEND { $$.n = new(NodeType.Begin, $2.n, $3.n, null, null, null, null, null); }     
           ;
           
commit_stmt : TCOMMIT { $$.n = new(NodeType.Commit, null, null, null, null, null, null, null); }     
           ;           
           
rollback_stmt : TROLLBACK { $$.n = new(NodeType.Rollback, null, null, null, null, null, null, null); }     
           ;
           
return_stmt : TRETURN expression { $$.n = new(NodeType.Return, $2.n, null, null, null, null, null, null); }
            | TRETURN { $$.n = new(NodeType.Return, null, null, null, null, null, null, null); }     
            ;
         
expression : expression TEQUALS expression { $$.n = new(NodeType.Equals, $1.n, $3.n, null, null, null, null, null); }
           | expression TNOTEQUALS expression { $$.n = new(NodeType.NotEquals, $1.n, $3.n, null, null, null, null, null); }
           | expression TLESSTHAN expression { $$.n = new(NodeType.LessThan, $1.n, $3.n, null, null, null, null, null); }
           | expression TGREATERTHAN expression { $$.n = new(NodeType.GreaterThan, $1.n, $3.n, null, null, null, null, null); }
           | expression TLESSTHANEQUALS expression { $$.n = new(NodeType.LessThanEquals, $1.n, $3.n, null, null, null, null, null); }
           | expression TGREATERTHANEQUALS expression { $$.n = new(NodeType.GreaterThanEquals, $1.n, $3.n, null, null, null, null, null); }
           | expression TAND expression { $$.n = new(NodeType.And, $1.n, $3.n, null, null, null, null, null); }
           | expression TOR expression { $$.n = new(NodeType.Or, $1.n, $3.n, null, null, null, null, null); }
           | expression TADD expression { $$.n = new(NodeType.Add, $1.n, $3.n, null, null, null, null, null); }
           | expression TMINUS expression { $$.n = new(NodeType.Subtract, $1.n, $3.n, null, null, null, null, null); }
           | expression TMULT expression { $$.n = new(NodeType.Mult, $1.n, $3.n, null, null, null, null, null); }
           | expression TDIV expression { $$.n = new(NodeType.Div, $1.n, $3.n, null, null, null, null, null); }
           | LPAREN expression RPAREN { $$.n = $2.n; }
           | fcall_expr { $$.n = $1.n; } 
           | identifier { $$.n = $1.n; }
           | int { $$.n = $1.n; }
           | float { $$.n = $1.n; }
           | string { $$.n = $1.n; }
           | boolean { $$.n = $1.n; }
           ;
           
fcall_expr : identifier LPAREN RPAREN { $$.n = new(NodeType.FuncCall, $1.n, null, null, null, null, null, null); }
           | identifier LPAREN fcall_argument_list RPAREN { $$.n = new(NodeType.FuncCall, $1.n, $3.n, null, null, null, null, null); }
           ;

fcall_argument_list  : fcall_argument_list TCOMMA fcall_argument_item { $$.n = new(NodeType.ArgumentList, $1.n, $3.n, null, null, null, null, null); }
                     | fcall_argument_item { $$.n = $1.n; $$.s = $1.s; }
                     ;

fcall_argument_item : expression { $$.n = $1.n; $$.s = $1.s; }
                    ;           
           
identifier : TIDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, $$.s); }
           ;

int : TDIGIT { $$.n = new(NodeType.Integer, null, null, null, null, null, null, $$.s); }
    ;
    
float : TFLOAT { $$.n = new(NodeType.Float, null, null, null, null, null, null, $$.s); }
      ;  
      
boolean : TTRUE { $$.n = new(NodeType.Boolean, null, null, null, null, null, null, "true"); }
        | TFALSE { $$.n = new(NodeType.Boolean, null, null, null, null, null, null, "false"); }
        ;     

string : TSTRING { $$.n = new(NodeType.String, null, null, null, null, null, null, $$.s); }
       ;

%%