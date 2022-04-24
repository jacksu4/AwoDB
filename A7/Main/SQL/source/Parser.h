/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_YY_STORAGE_HOME_Q_QZ40_COMP530_CODE_A7_MAIN_SQL_SOURCE_PARSER_H_INCLUDED
# define YY_YY_STORAGE_HOME_Q_QZ40_COMP530_CODE_A7_MAIN_SQL_SOURCE_PARSER_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    INTEGER = 258,
    IDENTIFIER = 259,
    DBL = 260,
    STR = 261,
    SELECT = 262,
    FROM = 263,
    WHERE = 264,
    AS = 265,
    BY = 266,
    AND = 267,
    OR = 268,
    NOT = 269,
    SUM = 270,
    AVG = 271,
    GROUP = 272,
    INT = 273,
    BOOL = 274,
    BPLUSTREE = 275,
    CREATE = 276,
    DOUBLE = 277,
    STRING = 278,
    ON = 279,
    TABLE = 280
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 13 "/storage-home/q/qz40/comp530_code/A7/Main/SQL/source/Parser.y" /* yacc.c:1909  */

	struct SQLStatement *myStatement;
	struct SFWQuery *mySelectQuery;
	struct CreateTable *myCreateTable;
	struct FromList *myFromList;
	struct AttList *myAttList;
	struct Value *myValue;
	struct ValueList *allValues;
	struct CNF *myCNF;	
	int myInt;
	char *myChar;
	double myDouble;

#line 94 "/storage-home/q/qz40/comp530_code/A7/Main/SQL/source/Parser.h" /* yacc.c:1909  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (void *scanner, struct SQLStatement **myStatement);

#endif /* !YY_YY_STORAGE_HOME_Q_QZ40_COMP530_CODE_A7_MAIN_SQL_SOURCE_PARSER_H_INCLUDED  */
