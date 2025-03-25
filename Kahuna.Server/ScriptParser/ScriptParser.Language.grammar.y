%namespace Kahuna.Server.ScriptParser
%partial
%parsertype scriptParser
%visibility internal
%tokentype Token

%union { 
        public NodeAst n;
        public string s;
        public int l;
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
%right TNOT

%token LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV LBRACE RBRACE
%token TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS  
%token TBEGIN TROLLBACK TCOMMIT TLET TSET TGET TESET TEGET TDELETE TEDELETE TEXTEND TEEXTEND 
%token TIF TELSE TTHEN TEND TNX TXX TEX TCMP TCMPREV
%token TRETURN TSLEEP TDIGIT TFLOAT TSTRING TIDENTIFIER TESCIDENTIFIER TPLACEHOLDER TTRUE TFALSE TAT

%%

stmt_list : stmt_list stmt { $$.n = new(NodeType.StmtList, $1.n, $2.n, null, null, null, null, null, $1.l); }
          | stmt { $$.n = $1.n; $$.l = $1.l; }
          ;

stmt    : set_stmt { $$.n = $1.n; $$.l = $1.l; }
        | eset_stmt { $$.n = $1.n; $$.l = $1.l; } 
        | get_stmt { $$.n = $1.n; $$.l = $1.l; }
        | eget_stmt { $$.n = $1.n; $$.l = $1.l; }
        | delete_stmt { $$.n = $1.n; $$.l = $1.l; }
        | edelete_stmt { $$.n = $1.n; $$.l = $1.l; }
        | extend_stmt { $$.n = $1.n; $$.l = $1.l; }
        | eextend_stmt { $$.n = $1.n; $$.l = $1.l; }
        | let_stmt { $$.n = $1.n; $$.l = $1.l; }
        | if_stmt { $$.n = $1.n; $$.l = $1.l; } 
        | begin_stmt { $$.n = $1.n; $$.l = $1.l; }
        | commit_stmt { $$.n = $1.n; $$.l = $1.l; }
        | rollback_stmt { $$.n = $1.n; $$.l = $1.l; }
        | return_stmt { $$.n = $1.n; $$.l = $1.l; }
        | sleep_stmt { $$.n = $1.n; $$.l = $1.l; }
        ;

set_stmt : TSET identifier expression { $$.n = new(NodeType.Set, $2.n, $3.n, null, null, null, null, null, $1.l); }         
          | TSET identifier expression set_cmp { $$.n = new(NodeType.Set, $2.n, $3.n, null, null, $4.n, null, null, $1.l); }
          | TSET identifier expression TEX int { $$.n = new(NodeType.Set, $2.n, $3.n, $5.n, null, null, null, null, $1.l); }
          | TSET identifier expression set_cmp TEX int { $$.n = new(NodeType.Set, $2.n, $3.n, $6.n, null, $4.n, null, null, $1.l); }
          | TSET identifier expression TEX int set_not_exists { $$.n = new(NodeType.Set, $2.n, $3.n, $5.n, $6.n, null, null, null, $1.l); }
          | TSET identifier expression set_not_exists { $$.n = new(NodeType.Set, $2.n, $3.n, null, $4.n, null, null, null, $1.l); }
          ; 
         
set_not_exists : TNX { $$.n = new(NodeType.SetNotExists, null, null, null, null, null, null, null, $1.l); }
               | TXX { $$.n = new(NodeType.SetExists, null, null, null, null, null, null, null, $1.l); }
               ;
               
set_cmp : TCMP expression { $$.n = new(NodeType.SetCmp, $2.n, null, null, null, null, null, null, $1.l); }
        | TCMPREV expression { $$.n = new(NodeType.SetCmpRev, $2.n, null, null, null, null, null, null, $1.l); }                 
        ;
         
eset_stmt : TESET identifier expression { $$.n = new(NodeType.Eset, $2.n, $3.n, null, null, null, null, null, $1.l); }         
          | TESET identifier expression set_cmp { $$.n = new(NodeType.Eset, $2.n, $3.n, null, null, $4.n, null, null, $1.l); }
          | TESET identifier expression TEX int { $$.n = new(NodeType.Eset, $2.n, $3.n, $5.n, null, null, null, null, $1.l); }
          | TESET identifier expression set_cmp TEX int { $$.n = new(NodeType.Eset, $2.n, $3.n, $6.n, null, $4.n, null, null, $1.l); }
          | TESET identifier expression TEX int set_not_exists { $$.n = new(NodeType.Eset, $2.n, $3.n, $5.n, $6.n, null, null, null, $1.l); }
          | TESET identifier expression set_not_exists { $$.n = new(NodeType.Eset, $2.n, $3.n, null, $4.n, null, null, null, $1.l); }
          ;                  
         
get_stmt : TGET identifier { $$.n = new(NodeType.Get, $2.n, null, null, null, null, null, null, $1.l); }
         | TLET identifier TEQUALS TGET identifier { $$.n = new(NodeType.Get, $5.n, $2.n, null, null, null, null, null, $1.l); }
         | TGET identifier TAT int { $$.n = new(NodeType.Get, $2.n, null, $4.n, null, null, null, null, $1.l); }
         | TLET identifier TEQUALS TGET identifier TAT int { $$.n = new(NodeType.Get, $5.n, $2.n, $7.n, null, null, null, null, $1.l); }
         ;
         
eget_stmt : TEGET identifier { $$.n = new(NodeType.Eget, $2.n, null, null, null, null, null, null, $1.l); }
          | TLET identifier TEQUALS TEGET identifier { $$.n = new(NodeType.Eget, $5.n, $2.n, null, null, null, null, null, $1.l); }
          | TEGET identifier TAT int { $$.n = new(NodeType.Eget, $2.n, null, $4.n, null, null, null, null, $1.l); }
          | TLET identifier TEQUALS TEGET identifier TAT int { $$.n = new(NodeType.Eget, $5.n, $2.n, $7.n, null, null, null, null, $1.l); }
          ;
          
delete_stmt : TDELETE identifier { $$.n = new(NodeType.Delete, $2.n, null, null, null, null, null, null, $1.l); }
            ;
            
edelete_stmt : TEDELETE identifier { $$.n = new(NodeType.Edelete, $2.n, null, null, null, null, null, null, $1.l); }
            ;
            
extend_stmt : TEXTEND identifier int { $$.n = new(NodeType.Extend, $2.n, $3.n, null, null, null, null, null, $1.l); }
            ;
            
eextend_stmt : TEEXTEND identifier int { $$.n = new(NodeType.Eextend, $2.n, $3.n, null, null, null, null, null, $1.l); }
            ;
            
let_stmt : TLET identifier TEQUALS expression { $$.n = new(NodeType.Let, $2.n, $4.n, null, null, null, null, null, $1.l); }                   
          ;                     
         
if_stmt : TIF expression TTHEN stmt_list TEND { $$.n = new(NodeType.If, $2.n, $4.n, null, null, null, null, null, $1.l); }
        | TIF expression TTHEN stmt_list TELSE stmt_list TEND { $$.n = new(NodeType.If, $2.n, $4.n, $6.n, null, null, null, null, $1.l); }
        ;
        
begin_stmt : TBEGIN stmt_list TEND { $$.n = new(NodeType.Begin, $2.n, null, null, null, null, null, null, $1.l); }     
           | TBEGIN LPAREN begin_options RPAREN stmt_list TEND { $$.n = new(NodeType.Begin, $5.n, $3.n, null, null, null, null, null, $1.l); }
           ;
           
begin_options : begin_options TCOMMA begin_option { $$.n = new(NodeType.BeginOptionList, $2.n, null, null, null, null, null, null, $1.l); }
              | begin_option { $$.n = $1.n; $$.l = $1.l; }
              ;
             
begin_option : identifier TEQUALS simple_expr { $$.n = new(NodeType.BeginOption, $1.n, $3.n, null, null, null, null, null, $1.l); }             
             ; 
           
commit_stmt : TCOMMIT { $$.n = new(NodeType.Commit, null, null, null, null, null, null, null, $1.l); }     
           ;           
           
rollback_stmt : TROLLBACK { $$.n = new(NodeType.Rollback, null, null, null, null, null, null, null, $1.l); }     
           ;
           
return_stmt : TRETURN expression { $$.n = new(NodeType.Return, $2.n, null, null, null, null, null, null, $1.l); }
            | TRETURN { $$.n = new(NodeType.Return, null, null, null, null, null, null, null, $1.l); }     
            ;
            
sleep_stmt : TSLEEP int { $$.n = new(NodeType.Sleep, $2.n, null, null, null, null, null, null, $1.l); }
           ;
         
expression : expression TEQUALS expression { $$.n = new(NodeType.Equals, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TNOTEQUALS expression { $$.n = new(NodeType.NotEquals, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TLESSTHAN expression { $$.n = new(NodeType.LessThan, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TGREATERTHAN expression { $$.n = new(NodeType.GreaterThan, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TLESSTHANEQUALS expression { $$.n = new(NodeType.LessThanEquals, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TGREATERTHANEQUALS expression { $$.n = new(NodeType.GreaterThanEquals, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TAND expression { $$.n = new(NodeType.And, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TOR expression { $$.n = new(NodeType.Or, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TADD expression { $$.n = new(NodeType.Add, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TMINUS expression { $$.n = new(NodeType.Subtract, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TMULT expression { $$.n = new(NodeType.Mult, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TDIV expression { $$.n = new(NodeType.Div, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | TNOT expression { $$.n = new(NodeType.Not, $2.n, null, null, null, null, null, null, $1.l); }
           | LPAREN expression RPAREN { $$.n = $2.n; $$.l = $2.l; }           
           | fcall_expr { $$.n = $1.n; $$.l = $1.l; } 
           | identifier { $$.n = $1.n; $$.l = $1.l; }
           | placeholder { $$.n = $1.n; $$.l = $1.l; }
           | int { $$.n = $1.n; $$.l = $1.l; }
           | float { $$.n = $1.n; $$.l = $1.l; }
           | string { $$.n = $1.n; $$.l = $1.l; }
           | boolean { $$.n = $1.n; $$.l = $1.l; }
           ;
           
fcall_expr : identifier LPAREN RPAREN { $$.n = new(NodeType.FuncCall, $1.n, null, null, null, null, null, null, $1.l); }
           | identifier LPAREN fcall_argument_list RPAREN { $$.n = new(NodeType.FuncCall, $1.n, $3.n, null, null, null, null, null, $1.l); }
           ;

fcall_argument_list  : fcall_argument_list TCOMMA fcall_argument_item { $$.n = new(NodeType.ArgumentList, $1.n, $3.n, null, null, null, null, null, $1.l); }
                     | fcall_argument_item { $$.n = $1.n; $$.s = $1.s; $$.l = $1.l; }
                     ;

fcall_argument_item : expression { $$.n = $1.n; $$.s = $1.s; $$.l = $1.l; }
                    ;
                    
simple_expr : identifier { $$.n = $1.n; $$.l = $1.l; }
            | int { $$.n = $1.n; $$.l = $1.l; }
            | float { $$.n = $1.n; $$.l = $1.l; }
            | string { $$.n = $1.n; $$.l = $1.l; }
            | boolean { $$.n = $1.n; $$.l = $1.l; }
            | placeholder { $$.n = $1.n; $$.l = $1.l; }
            ;            
           
identifier : TIDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, $$.s, $1.l); }
           ;
           
placeholder : TPLACEHOLDER { $$.n = new(NodeType.Placeholder, null, null, null, null, null, null, $$.s, $1.l); }
           ;           

int : TDIGIT { $$.n = new(NodeType.Integer, null, null, null, null, null, null, $$.s, $1.l); }
    ;
    
float : TFLOAT { $$.n = new(NodeType.Float, null, null, null, null, null, null, $$.s, $1.l); }
      ;  
      
boolean : TTRUE { $$.n = new(NodeType.Boolean, null, null, null, null, null, null, "true", $1.l); }
        | TFALSE { $$.n = new(NodeType.Boolean, null, null, null, null, null, null, "false", $1.l); }
        ;     

string : TSTRING { $$.n = new(NodeType.String, null, null, null, null, null, null, $$.s, $1.l); }
       ;

%%