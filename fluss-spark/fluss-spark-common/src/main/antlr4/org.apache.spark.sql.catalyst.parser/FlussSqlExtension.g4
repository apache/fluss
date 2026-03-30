/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar FlussSqlExtension;

@lexer::members {
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }

  public boolean isHint() {
    int nextChar = _input.LA(1);
    if (nextChar == '+') {
      return true;
    } else {
      return false;
    }
  }
}

singleStatement
    : statement ';'* EOF
    ;

statement
    : CALL multipartIdentifier '(' (callArgument (',' callArgument)*)? ')'  #call
    ;

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : predicated
    | NOT booleanExpression
    | EXISTS '(' query ')'
    | booleanExpression AND booleanExpression
    | booleanExpression OR booleanExpression
    ;

predicated
    : valueExpression
    ;

valueExpression
    : primaryExpression
    ;

primaryExpression
    : constant                                                        #constantDefault
    | functionName '(' (expression (',' expression)*)? ')'            #functionCall
    | '(' expression ')'                                              #parenthesizedExpression
    ;

functionName
    : multipartIdentifier
    | identifier
    ;

query
    : .+?
    ;

constant
    : number                          #numericLiteral
    | booleanValue                    #booleanLiteral
    | STRING+                         #stringLiteral
    | identifier STRING               #typeConstructor
    ;

booleanValue
    : TRUE | FALSE
    ;

number
    : MINUS? EXPONENT_VALUE           #exponentLiteral
    | MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? FLOAT_LITERAL            #floatLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

nonReserved
    : CALL | TRUE | FALSE | NOT | AND | OR | EXISTS
    ;

// Keywords
CALL: 'CALL';
TRUE: 'TRUE';
FALSE: 'FALSE';
NOT: 'NOT';
AND: 'AND';
OR: 'OR';
EXISTS: 'EXISTS';

// Operators
MINUS: '-';

// Literals
STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : INTEGER_VALUE 'L'
    ;

SMALLINT_LITERAL
    : INTEGER_VALUE 'S'
    ;

TINYINT_LITERAL
    : INTEGER_VALUE 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

EXPONENT_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT {isValidDecimal()}?
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS {isValidDecimal()}?
    ;

FLOAT_LITERAL
    : DIGIT+ EXPONENT? 'F'
    | DECIMAL_DIGITS EXPONENT? 'F' {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

// Whitespace and comments
SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for any characters we didn't match
UNRECOGNIZED
    : .
    ;
