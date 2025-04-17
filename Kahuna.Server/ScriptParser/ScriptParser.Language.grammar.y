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

%left TDOUBLEDOT
%left TOR
%left TAND
%left TLIKE TILIKE
%left TEQUALS TNOTEQUALS TDOUBLEEQUALS
%left TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS
%left TADD TMINUS
%left TMULT TDIV
%right TNOT

%token LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV LBRACE RBRACE
%token TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS TDOUBLEQUALS
%token TBEGIN TROLLBACK TCOMMIT TLET TSET TGET TESET TEGET TDELETE TEDELETE TEXTEND TEEXTEND TEXISTS TEEXISTS
%token TIF TELSE TTHEN TEND TNX TXX TEX TCMP TCMPREV TTHROW TFOUND TFOR TDO TIN
%token TRETURN TSLEEP TDIGIT TFLOAT TSTRING TIDENTIFIER TESCIDENTIFIER TPLACEHOLDER TTRUE TFALSE TNULL TAT TPREFIX TBY

%%

stmt_list : stmt_list stmt { $$.n = new(NodeType.StmtList, $1.n, $2.n, null, null, null, null, null, $1.l); }
          | stmt { $$.n = $1.n; $$.l = $1.l; }
          ;

stmt    : set_stmt { $$.n = $1.n; $$.l = $1.l; }
        | eset_stmt { $$.n = $1.n; $$.l = $1.l; } 
        | get_stmt { $$.n = $1.n; $$.l = $1.l; }
        | eget_stmt { $$.n = $1.n; $$.l = $1.l; }
        | exists_stmt { $$.n = $1.n; $$.l = $1.l; }
        | eexists_stmt { $$.n = $1.n; $$.l = $1.l; }
        | delete_stmt { $$.n = $1.n; $$.l = $1.l; }
        | edelete_stmt { $$.n = $1.n; $$.l = $1.l; }
        | extend_stmt { $$.n = $1.n; $$.l = $1.l; }
        | eextend_stmt { $$.n = $1.n; $$.l = $1.l; }
        | get_by_prefix_stmt { $$.n = $1.n; $$.l = $1.l; }
        | eget_by_prefix_stmt { $$.n = $1.n; $$.l = $1.l; }
        | let_stmt { $$.n = $1.n; $$.l = $1.l; }
        | for_stmt { $$.n = $1.n; $$.l = $1.l; }
        | if_stmt { $$.n = $1.n; $$.l = $1.l; } 
        | begin_stmt { $$.n = $1.n; $$.l = $1.l; }
        | commit_stmt { $$.n = $1.n; $$.l = $1.l; }
        | rollback_stmt { $$.n = $1.n; $$.l = $1.l; }
        | return_stmt { $$.n = $1.n; $$.l = $1.l; }
        | sleep_stmt { $$.n = $1.n; $$.l = $1.l; }
        | throw_stmt { $$.n = $1.n; $$.l = $1.l; }
        ;

set_stmt : TSET key_name expression { $$.n = new(NodeType.Set, $2.n, $3.n, null, null, null, null, null, $1.l); }         
          | TSET key_name expression set_cmp { $$.n = new(NodeType.Set, $2.n, $3.n, null, null, $4.n, null, null, $1.l); }
          | TSET key_name expression TEX int { $$.n = new(NodeType.Set, $2.n, $3.n, $5.n, null, null, null, null, $1.l); }
          | TSET key_name expression set_cmp TEX int { $$.n = new(NodeType.Set, $2.n, $3.n, $6.n, null, $4.n, null, null, $1.l); }
          | TSET key_name expression TEX int set_not_exists { $$.n = new(NodeType.Set, $2.n, $3.n, $5.n, $6.n, null, null, null, $1.l); }
          | TSET key_name expression set_not_exists { $$.n = new(NodeType.Set, $2.n, $3.n, null, $4.n, null, null, null, $1.l); }
          ; 
         
set_not_exists : TNX { $$.n = new(NodeType.SetNotExists, null, null, null, null, null, null, null, $1.l); }
               | TXX { $$.n = new(NodeType.SetExists, null, null, null, null, null, null, null, $1.l); }
               ;
               
set_cmp : TCMP expression { $$.n = new(NodeType.SetCmp, $2.n, null, null, null, null, null, null, $1.l); }
        | TCMPREV expression { $$.n = new(NodeType.SetCmpRev, $2.n, null, null, null, null, null, null, $1.l); }                 
        ;
         
eset_stmt : TESET key_name expression { $$.n = new(NodeType.Eset, $2.n, $3.n, null, null, null, null, null, $1.l); }         
          | TESET key_name expression set_cmp { $$.n = new(NodeType.Eset, $2.n, $3.n, null, null, $4.n, null, null, $1.l); }
          | TESET key_name expression TEX int { $$.n = new(NodeType.Eset, $2.n, $3.n, $5.n, null, null, null, null, $1.l); }
          | TESET key_name expression set_cmp TEX int { $$.n = new(NodeType.Eset, $2.n, $3.n, $6.n, null, $4.n, null, null, $1.l); }
          | TESET key_name expression TEX int set_not_exists { $$.n = new(NodeType.Eset, $2.n, $3.n, $5.n, $6.n, null, null, null, $1.l); }
          | TESET key_name expression set_not_exists { $$.n = new(NodeType.Eset, $2.n, $3.n, null, $4.n, null, null, null, $1.l); }
          ;                  
         
get_stmt : TGET key_name { $$.n = new(NodeType.Get, $2.n, null, null, null, null, null, null, $1.l); }
         | TLET identifier TEQUALS TGET key_name { $$.n = new(NodeType.Get, $5.n, $2.n, null, null, null, null, null, $1.l); }
         | TGET key_name TAT int { $$.n = new(NodeType.Get, $2.n, null, $4.n, null, null, null, null, $1.l); }
         | TLET identifier TEQUALS TGET key_name TAT int { $$.n = new(NodeType.Get, $5.n, $2.n, $7.n, null, null, null, null, $1.l); }
         ;
         
eget_stmt : TEGET key_name { $$.n = new(NodeType.Eget, $2.n, null, null, null, null, null, null, $1.l); }
          | TLET identifier TEQUALS TEGET key_name { $$.n = new(NodeType.Eget, $5.n, $2.n, null, null, null, null, null, $1.l); }
          | TEGET key_name TAT int { $$.n = new(NodeType.Eget, $2.n, null, $4.n, null, null, null, null, $1.l); }
          | TLET identifier TEQUALS TEGET key_name TAT int { $$.n = new(NodeType.Eget, $5.n, $2.n, $7.n, null, null, null, null, $1.l); }
          ;
                   
exists_stmt : TEXISTS key_name { $$.n = new(NodeType.Exists, $2.n, null, null, null, null, null, null, $1.l); }
       | TLET identifier TEQUALS TEXISTS key_name { $$.n = new(NodeType.Exists, $5.n, $2.n, null, null, null, null, null, $1.l); }
       | TEXISTS key_name TAT int { $$.n = new(NodeType.Exists, $2.n, null, $4.n, null, null, null, null, $1.l); }
       | TLET identifier TEQUALS TEXISTS key_name TAT int { $$.n = new(NodeType.Exists, $5.n, $2.n, $7.n, null, null, null, null, $1.l); }
       ;
       
eexists_stmt : TEEXISTS key_name { $$.n = new(NodeType.Eexists, $2.n, null, null, null, null, null, null, $1.l); }
        | TLET identifier TEQUALS TEEXISTS key_name { $$.n = new(NodeType.Eexists, $5.n, $2.n, null, null, null, null, null, $1.l); }
        | TEEXISTS key_name TAT int { $$.n = new(NodeType.Eexists, $2.n, null, $4.n, null, null, null, null, $1.l); }
        | TLET identifier TEQUALS TEEXISTS key_name TAT int { $$.n = new(NodeType.Eexists, $5.n, $2.n, $7.n, null, null, null, null, $1.l); }
        ;
          
delete_stmt : TDELETE key_name { $$.n = new(NodeType.Delete, $2.n, null, null, null, null, null, null, $1.l); }
            ;
            
edelete_stmt : TEDELETE key_name { $$.n = new(NodeType.Edelete, $2.n, null, null, null, null, null, null, $1.l); }
            ;
            
extend_stmt : TEXTEND key_name int { $$.n = new(NodeType.Extend, $2.n, $3.n, null, null, null, null, null, $1.l); }
            ;
            
eextend_stmt : TEEXTEND key_name int { $$.n = new(NodeType.Eextend, $2.n, $3.n, null, null, null, null, null, $1.l); }
            ;
            
get_by_prefix_stmt : TGET TBY TPREFIX key_name { $$.n = new(NodeType.GetByPrefix, $4.n, null, null, null, null, null, null, $1.l); }
                   | TLET identifier TEQUALS TGET TBY TPREFIX key_name { $$.n = new(NodeType.GetByPrefix, $7.n, $2.n, null, null, null, null, null, $1.l); }    
                   ;
                   
eget_by_prefix_stmt : TEGET TBY TPREFIX key_name { $$.n = new(NodeType.EgetByPrefix, $4.n, null, null, null, null, null, null, $1.l); }    
                   | TLET identifier TEQUALS TGET TBY TPREFIX key_name { $$.n = new(NodeType.EgetByPrefix, $7.n, $2.n, null, null, null, null, null, $1.l); }
                   ;            
            
key_name : identifier { $$.n = $1.n; $$.l = $1.l; }
         | placeholder  { $$.n = $1.n; $$.l = $1.l; }
         ;
            
let_stmt : TLET identifier TEQUALS expression { $$.n = new(NodeType.Let, $2.n, $4.n, null, null, null, null, null, $1.l); }                   
          ;                     
         
if_stmt : TIF expression TTHEN stmt_list TEND { $$.n = new(NodeType.If, $2.n, $4.n, null, null, null, null, null, $1.l); }
        | TIF expression TTHEN stmt_list TELSE stmt_list TEND { $$.n = new(NodeType.If, $2.n, $4.n, $6.n, null, null, null, null, $1.l); }
        ;
        
for_stmt : TFOR identifier TIN expression TDO stmt_list TEND { $$.n = new(NodeType.For, $2.n, $4.n, $6.n, null, null, null, null, $1.l); }
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
           
throw_stmt : TTHROW expression { $$.n = new(NodeType.Throw, $2.n, null, null, null, null, null, null, $1.l); }   
           ;        
         
expression : expression TEQUALS expression { $$.n = new(NodeType.Equals, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | expression TDOUBLEEQUALS expression { $$.n = new(NodeType.Equals, $1.n, $3.n, null, null, null, null, null, $1.l); }
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
           | expression TDOUBLEDOT expression { $$.n = new(NodeType.Range, $1.n, $3.n, null, null, null, null, null, $1.l); }
           | TNOT expression { $$.n = new(NodeType.Not, $2.n, null, null, null, null, null, null, $1.l); }
           | TNOT TSET { $$.n = new(NodeType.NotSet, null, null, null, null, null, null, null, $1.l); }
           | TNOT TFOUND { $$.n = new(NodeType.NotFound, null, null, null, null, null, null, null, $1.l); }
           | LPAREN expression RPAREN { $$.n = $2.n; $$.l = $2.l; }           
           | fcall_expr { $$.n = $1.n; $$.l = $1.l; } 
           | identifier { $$.n = $1.n; $$.l = $1.l; }
           | placeholder { $$.n = $1.n; $$.l = $1.l; }
           | int { $$.n = $1.n; $$.l = $1.l; }
           | float { $$.n = $1.n; $$.l = $1.l; }
           | string { $$.n = $1.n; $$.l = $1.l; }
           | boolean { $$.n = $1.n; $$.l = $1.l; }
           | null { $$.n = $1.n; $$.l = $1.l; }
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
            | null { $$.n = $1.n; $$.l = $1.l; }
            | placeholder { $$.n = $1.n; $$.l = $1.l; }
            ;            
           
identifier : TIDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, $$.s, $1.l); }
           ;
           
placeholder : TPLACEHOLDER { $$.n = new(NodeType.Placeholder, null, null, null, null, null, null, $$.s, $1.l); }
           ;           

int : TDIGIT { $$.n = new(NodeType.IntegerType, null, null, null, null, null, null, $$.s, $1.l); }
    ;
    
float : TFLOAT { $$.n = new(NodeType.FloatType, null, null, null, null, null, null, $$.s, $1.l); }
      ;  
      
boolean : TTRUE { $$.n = new(NodeType.BooleanType, null, null, null, null, null, null, "true", $1.l); }
        | TFALSE { $$.n = new(NodeType.BooleanType, null, null, null, null, null, null, "false", $1.l); }
        ;     

string : TSTRING { $$.n = new(NodeType.StringType, null, null, null, null, null, null, $$.s, $1.l); }
       ;
       
null : TNULL { $$.n = new(NodeType.NullType, null, null, null, null, null, null, null, $1.l); }
     ;

%%