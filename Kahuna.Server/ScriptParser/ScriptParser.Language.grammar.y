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
%left TMULT

%token TDIGIT TFLOAT TSTRING TIDENTIFIER TPLACEHOLDER LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV 
%token TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS 
%token TSET TGET TESET TEGET LBRACE RBRACE TIF TELSE TTHEN TEND TNX TXX 

%%

stmt_list : stmt_list stmt { $$.n = new(NodeType.StmtList, $1.n, $2.n, null, null, null, null, null); }
          | stmt { $$.n = $1.n; }
          ;

stmt    : set_stmt { $$.n = $1.n; }
        | eset_stmt { $$.n = $1.n; } 
        | get_stmt { $$.n = $1.n; }
        | eget_stmt { $$.n = $1.n; }
        | if_stmt { $$.n = $1.n; }          
        ;

set_stmt : TSET identifier set_value { $$.n = new(NodeType.Set, $2.n, $3.n, null, null, null, null, null); }         
         | TSET identifier set_value int { $$.n = new(NodeType.Set, $2.n, $3.n, $4.n, null, null, null, null); }
         | TSET identifier set_value int set_not_exists { $$.n = new(NodeType.Set, $2.n, $3.n, $4.n, $5.n, null, null, null); }
         | TSET identifier set_value set_not_exists { $$.n = new(NodeType.Set, $2.n, $3.n, null, $4.n, null, null, null); }
         ;
         
set_not_exists : TNX { $$.n = new(NodeType.SetNotExists, null, null, null, null, null, null, null); }
               | TXX { $$.n = new(NodeType.SetExists, null, null, null, null, null, null, null); }
               ;
         
eset_stmt : TESET identifier set_value { $$.n = new(NodeType.Eset, $2.n, $3.n, null, null, null, null, null); }         
         | TESET identifier set_value int { $$.n = new(NodeType.Eset, $2.n, $3.n, $4.n, null, null, null, null); }         
         ;
         
set_value : identifier { $$.n = $1.n; }
          | string { $$.n = $1.n; }
          | int { $$.n = $1.n; }
          ;                  
         
get_stmt : TGET identifier { $$.n = new(NodeType.Get, $2.n, null, null, null, null, null, null); }
         | identifier TEQUALS TGET identifier { $$.n = new(NodeType.Get, $4.n, $1.n, null, null, null, null, null); }
         ;
         
eget_stmt : TEGET identifier { $$.n = new(NodeType.Eget, $2.n, null, null, null, null, null, null); }
          | identifier TEQUALS TEGET identifier { $$.n = new(NodeType.Eget, $4.n, $1.n, null, null, null, null, null); }
          ;
         
if_stmt : TIF expression TTHEN stmt_list TEND { $$.n = new(NodeType.If, $2.n, $4.n, null, null, null, null, null); }
        ;
         
expression : expression TEQUALS expression { $$.n = new(NodeType.Equals, $1.n, $3.n, null, null, null, null, null); }
           | expression TNOTEQUALS expression { $$.n = new(NodeType.NotEquals, $1.n, $3.n, null, null, null, null, null); }
           | expression TLESSTHAN expression { $$.n = new(NodeType.LessThan, $1.n, $3.n, null, null, null, null, null); }
           | expression TGREATERTHAN expression { $$.n = new(NodeType.GreaterThan, $1.n, $3.n, null, null, null, null, null); }
           | expression TLESSTHANEQUALS expression { $$.n = new(NodeType.LessThanEquals, $1.n, $3.n, null, null, null, null, null); }
           | expression TGREATERTHANEQUALS expression { $$.n = new(NodeType.GreaterThanEquals, $1.n, $3.n, null, null, null, null, null); }
           | expression TAND expression { $$.n = new(NodeType.And, $1.n, $3.n, null, null, null, null, null); }
           | expression TOR expression { $$.n = new(NodeType.Or, $1.n, $3.n, null, null, null, null, null); }
           | LPAREN expression RPAREN { $$.n = $2.n; }
           | identifier { $$.n = $1.n; }
           | int { $$.n = $1.n; }
           | string { $$.n = $1.n; }
           ;
           
identifier : TIDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, $$.s); }
           ;

int     : TDIGIT { $$.n = new(NodeType.Integer, null, null, null, null, null, null, $$.s); }
        ;

string  : TSTRING { $$.n = new(NodeType.String, null, null, null, null, null, null, $$.s); }
        ;

%%