%namespace Kahuna.Server.ScriptParser
%partial
%parsertype scriptParser
%visibility internal
%tokentype Token

%union { 
        public NodeAst n;
        public string s;
}

%start list
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
%token TSET TGET LBRACE RBRACE

%%

list    : list stat { $$.n = new(NodeType.StmtList, $1.n, $2.n, null, null, null, null, null); }
        | stat { $$.n = $1.n; }
        ;

stat    : set_stmt { $$.n = $1.n; }
        | get_stmt { $$.n = $1.n; }  
        ;

set_stmt : TSET identifier set_value { $$.n = new(NodeType.Set, $2.n, $3.n, null, null, null, null, null); }         
         | TSET identifier set_value int { $$.n = new(NodeType.Set, $2.n, $3.n, $4.n, null, null, null, null); }         
         ;
         
get_stmt : TGET identifier { $$.n = new(NodeType.Get, $2.n, null, null, null, null, null, null); }
         ;    
         
set_value : identifier { $$.n = $1.n; }
          | string { $$.n = $1.n; }          
          ; 
           
identifier  : TIDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, $$.s); }
            ;

int     : TDIGIT { $$.n = new(NodeType.Integer, null, null, null, null, null, null, $$.s); }
        ;

string  : TSTRING { $$.n = new(NodeType.String, null, null, null, null, null, null, $$.s); }
        ;

%%