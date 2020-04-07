
// Generated from /home/matt/Work/pushdownDB/normal/normal-sql/grammar/NormalSQL.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace normal::sql {


class  NormalSQLParser : public antlr4::Parser {
public:
  enum {
    SCOL = 1, DOT = 2, OPEN_PAR = 3, CLOSE_PAR = 4, COMMA = 5, ASSIGN = 6, 
    STAR = 7, PLUS = 8, MINUS = 9, TILDE = 10, PIPE2 = 11, DIV = 12, MOD = 13, 
    LT2 = 14, GT2 = 15, AMP = 16, PIPE = 17, LT = 18, LT_EQ = 19, GT = 20, 
    GT_EQ = 21, EQ = 22, NOT_EQ1 = 23, NOT_EQ2 = 24, K_ABORT = 25, K_ACTION = 26, 
    K_ADD = 27, K_AFTER = 28, K_ALL = 29, K_ALTER = 30, K_ANALYZE = 31, 
    K_AND = 32, K_AS = 33, K_ASC = 34, K_ATTACH = 35, K_AUTOINCREMENT = 36, 
    K_BEFORE = 37, K_BEGIN = 38, K_BETWEEN = 39, K_BY = 40, K_CASCADE = 41, 
    K_CASE = 42, K_CAST = 43, K_CHECK = 44, K_COLLATE = 45, K_COLUMN = 46, 
    K_COMMIT = 47, K_CONFLICT = 48, K_CONSTRAINT = 49, K_CREATE = 50, K_CROSS = 51, 
    K_CURRENT_DATE = 52, K_CURRENT_TIME = 53, K_CURRENT_TIMESTAMP = 54, 
    K_DATABASE = 55, K_DEFAULT = 56, K_DEFERRABLE = 57, K_DEFERRED = 58, 
    K_DELETE = 59, K_DESC = 60, K_DETACH = 61, K_DISTINCT = 62, K_DROP = 63, 
    K_EACH = 64, K_ELSE = 65, K_END = 66, K_ESCAPE = 67, K_EXCEPT = 68, 
    K_EXCLUSIVE = 69, K_EXISTS = 70, K_EXPLAIN = 71, K_FAIL = 72, K_FOR = 73, 
    K_FOREIGN = 74, K_FROM = 75, K_FULL = 76, K_GLOB = 77, K_GROUP = 78, 
    K_HAVING = 79, K_IF = 80, K_IGNORE = 81, K_IMMEDIATE = 82, K_IN = 83, 
    K_INDEX = 84, K_INDEXED = 85, K_INITIALLY = 86, K_INNER = 87, K_INSERT = 88, 
    K_INSTEAD = 89, K_INTERSECT = 90, K_INTO = 91, K_IS = 92, K_ISNULL = 93, 
    K_JOIN = 94, K_KEY = 95, K_LEFT = 96, K_LIKE = 97, K_LIMIT = 98, K_MATCH = 99, 
    K_NATURAL = 100, K_NO = 101, K_NOT = 102, K_NOTNULL = 103, K_NULL = 104, 
    K_OF = 105, K_OFFSET = 106, K_ON = 107, K_OR = 108, K_ORDER = 109, K_OUTER = 110, 
    K_PLAN = 111, K_PRAGMA = 112, K_PRIMARY = 113, K_QUERY = 114, K_RAISE = 115, 
    K_RECURSIVE = 116, K_REFERENCES = 117, K_REGEXP = 118, K_REINDEX = 119, 
    K_RELEASE = 120, K_RENAME = 121, K_REPLACE = 122, K_RESTRICT = 123, 
    K_RIGHT = 124, K_ROLLBACK = 125, K_ROW = 126, K_SAVEPOINT = 127, K_SELECT = 128, 
    K_SET = 129, K_TABLE = 130, K_TEMP = 131, K_TEMPORARY = 132, K_THEN = 133, 
    K_TO = 134, K_TRANSACTION = 135, K_TRIGGER = 136, K_UNION = 137, K_UNIQUE = 138, 
    K_UPDATE = 139, K_USING = 140, K_VACUUM = 141, K_VALUES = 142, K_VIEW = 143, 
    K_VIRTUAL = 144, K_WHEN = 145, K_WHERE = 146, K_WITH = 147, K_WITHOUT = 148, 
    IDENTIFIER = 149, NUMERIC_LITERAL = 150, BIND_PARAMETER = 151, STRING_LITERAL = 152, 
    BLOB_LITERAL = 153, SINGLE_LINE_COMMENT = 154, MULTILINE_COMMENT = 155, 
    SPACES = 156, UNEXPECTED_CHAR = 157
  };

  enum {
    RuleParse = 0, RuleError = 1, RuleSql_stmt_list = 2, RuleSql_stmt = 3, 
    RuleCompound_select_stmt = 4, RuleFactored_select_stmt = 5, RuleSimple_select_stmt = 6, 
    RuleSelect_stmt = 7, RuleSelect_or_values = 8, RuleType_name = 9, RuleExpr = 10, 
    RuleRaise_function = 11, RuleIndexed_column = 12, RuleWith_clause = 13, 
    RuleQualified_table_name = 14, RuleOrdering_term = 15, RulePragma_value = 16, 
    RuleCommon_table_expression = 17, RuleResult_column = 18, RuleTable_or_subquery = 19, 
    RuleJoin_clause = 20, RuleJoin_operator = 21, RuleJoin_constraint = 22, 
    RuleSelect_core = 23, RuleCompound_operator = 24, RuleCte_table_name = 25, 
    RuleSigned_number = 26, RuleLiteral_value = 27, RuleUnary_operator = 28, 
    RuleError_message = 29, RuleColumn_alias = 30, RuleKeyword = 31, RuleName = 32, 
    RuleFunction_name = 33, RuleDatabase_name = 34, RuleTable_name = 35, 
    RuleTable_or_index_name = 36, RuleNew_table_name = 37, RuleColumn_name = 38, 
    RuleCollation_name = 39, RuleForeign_table = 40, RuleIndex_name = 41, 
    RuleTrigger_name = 42, RuleView_name = 43, RuleModule_name = 44, RulePragma_name = 45, 
    RuleSavepoint_name = 46, RuleTable_alias = 47, RuleTransaction_name = 48, 
    RuleAny_name = 49
  };

  NormalSQLParser(antlr4::TokenStream *input);
  ~NormalSQLParser();

  virtual std::string getGrammarFileName() const override;
  virtual const antlr4::atn::ATN& getATN() const override { return _atn; };
  virtual const std::vector<std::string>& getTokenNames() const override { return _tokenNames; }; // deprecated: use vocabulary instead.
  virtual const std::vector<std::string>& getRuleNames() const override;
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;


  class ParseContext;
  class ErrorContext;
  class Sql_stmt_listContext;
  class Sql_stmtContext;
  class Compound_select_stmtContext;
  class Factored_select_stmtContext;
  class Simple_select_stmtContext;
  class Select_stmtContext;
  class Select_or_valuesContext;
  class Type_nameContext;
  class ExprContext;
  class Raise_functionContext;
  class Indexed_columnContext;
  class With_clauseContext;
  class Qualified_table_nameContext;
  class Ordering_termContext;
  class Pragma_valueContext;
  class Common_table_expressionContext;
  class Result_columnContext;
  class Table_or_subqueryContext;
  class Join_clauseContext;
  class Join_operatorContext;
  class Join_constraintContext;
  class Select_coreContext;
  class Compound_operatorContext;
  class Cte_table_nameContext;
  class Signed_numberContext;
  class Literal_valueContext;
  class Unary_operatorContext;
  class Error_messageContext;
  class Column_aliasContext;
  class KeywordContext;
  class NameContext;
  class Function_nameContext;
  class Database_nameContext;
  class Table_nameContext;
  class Table_or_index_nameContext;
  class New_table_nameContext;
  class Column_nameContext;
  class Collation_nameContext;
  class Foreign_tableContext;
  class Index_nameContext;
  class Trigger_nameContext;
  class View_nameContext;
  class Module_nameContext;
  class Pragma_nameContext;
  class Savepoint_nameContext;
  class Table_aliasContext;
  class Transaction_nameContext;
  class Any_nameContext; 

  class  ParseContext : public antlr4::ParserRuleContext {
  public:
    ParseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EOF();
    std::vector<Sql_stmt_listContext *> sql_stmt_list();
    Sql_stmt_listContext* sql_stmt_list(size_t i);
    std::vector<ErrorContext *> error();
    ErrorContext* error(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ParseContext* parse();

  class  ErrorContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *unexpected_charToken = nullptr;
    ErrorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNEXPECTED_CHAR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ErrorContext* error();

  class  Sql_stmt_listContext : public antlr4::ParserRuleContext {
  public:
    Sql_stmt_listContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Sql_stmtContext *> sql_stmt();
    Sql_stmtContext* sql_stmt(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SCOL();
    antlr4::tree::TerminalNode* SCOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Sql_stmt_listContext* sql_stmt_list();

  class  Sql_stmtContext : public antlr4::ParserRuleContext {
  public:
    Sql_stmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Compound_select_stmtContext *compound_select_stmt();
    Factored_select_stmtContext *factored_select_stmt();
    Simple_select_stmtContext *simple_select_stmt();
    Select_stmtContext *select_stmt();
    antlr4::tree::TerminalNode *K_EXPLAIN();
    antlr4::tree::TerminalNode *K_QUERY();
    antlr4::tree::TerminalNode *K_PLAN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Sql_stmtContext* sql_stmt();

  class  Compound_select_stmtContext : public antlr4::ParserRuleContext {
  public:
    Compound_select_stmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Select_coreContext *> select_core();
    Select_coreContext* select_core(size_t i);
    antlr4::tree::TerminalNode *K_WITH();
    std::vector<Common_table_expressionContext *> common_table_expression();
    Common_table_expressionContext* common_table_expression(size_t i);
    antlr4::tree::TerminalNode *K_ORDER();
    antlr4::tree::TerminalNode *K_BY();
    std::vector<Ordering_termContext *> ordering_term();
    Ordering_termContext* ordering_term(size_t i);
    antlr4::tree::TerminalNode *K_LIMIT();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> K_UNION();
    antlr4::tree::TerminalNode* K_UNION(size_t i);
    std::vector<antlr4::tree::TerminalNode *> K_INTERSECT();
    antlr4::tree::TerminalNode* K_INTERSECT(size_t i);
    std::vector<antlr4::tree::TerminalNode *> K_EXCEPT();
    antlr4::tree::TerminalNode* K_EXCEPT(size_t i);
    antlr4::tree::TerminalNode *K_RECURSIVE();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *K_OFFSET();
    std::vector<antlr4::tree::TerminalNode *> K_ALL();
    antlr4::tree::TerminalNode* K_ALL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Compound_select_stmtContext* compound_select_stmt();

  class  Factored_select_stmtContext : public antlr4::ParserRuleContext {
  public:
    Factored_select_stmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Select_coreContext *> select_core();
    Select_coreContext* select_core(size_t i);
    antlr4::tree::TerminalNode *K_WITH();
    std::vector<Common_table_expressionContext *> common_table_expression();
    Common_table_expressionContext* common_table_expression(size_t i);
    std::vector<Compound_operatorContext *> compound_operator();
    Compound_operatorContext* compound_operator(size_t i);
    antlr4::tree::TerminalNode *K_ORDER();
    antlr4::tree::TerminalNode *K_BY();
    std::vector<Ordering_termContext *> ordering_term();
    Ordering_termContext* ordering_term(size_t i);
    antlr4::tree::TerminalNode *K_LIMIT();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *K_RECURSIVE();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *K_OFFSET();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Factored_select_stmtContext* factored_select_stmt();

  class  Simple_select_stmtContext : public antlr4::ParserRuleContext {
  public:
    Simple_select_stmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Select_coreContext *select_core();
    antlr4::tree::TerminalNode *K_WITH();
    std::vector<Common_table_expressionContext *> common_table_expression();
    Common_table_expressionContext* common_table_expression(size_t i);
    antlr4::tree::TerminalNode *K_ORDER();
    antlr4::tree::TerminalNode *K_BY();
    std::vector<Ordering_termContext *> ordering_term();
    Ordering_termContext* ordering_term(size_t i);
    antlr4::tree::TerminalNode *K_LIMIT();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *K_RECURSIVE();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *K_OFFSET();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Simple_select_stmtContext* simple_select_stmt();

  class  Select_stmtContext : public antlr4::ParserRuleContext {
  public:
    Select_stmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Select_or_valuesContext *> select_or_values();
    Select_or_valuesContext* select_or_values(size_t i);
    antlr4::tree::TerminalNode *K_WITH();
    std::vector<Common_table_expressionContext *> common_table_expression();
    Common_table_expressionContext* common_table_expression(size_t i);
    std::vector<Compound_operatorContext *> compound_operator();
    Compound_operatorContext* compound_operator(size_t i);
    antlr4::tree::TerminalNode *K_ORDER();
    antlr4::tree::TerminalNode *K_BY();
    std::vector<Ordering_termContext *> ordering_term();
    Ordering_termContext* ordering_term(size_t i);
    antlr4::tree::TerminalNode *K_LIMIT();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *K_RECURSIVE();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *K_OFFSET();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Select_stmtContext* select_stmt();

  class  Select_or_valuesContext : public antlr4::ParserRuleContext {
  public:
    Select_or_valuesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *K_SELECT();
    std::vector<Result_columnContext *> result_column();
    Result_columnContext* result_column(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *K_FROM();
    antlr4::tree::TerminalNode *K_WHERE();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *K_GROUP();
    antlr4::tree::TerminalNode *K_BY();
    antlr4::tree::TerminalNode *K_DISTINCT();
    antlr4::tree::TerminalNode *K_ALL();
    std::vector<Table_or_subqueryContext *> table_or_subquery();
    Table_or_subqueryContext* table_or_subquery(size_t i);
    Join_clauseContext *join_clause();
    antlr4::tree::TerminalNode *K_HAVING();
    antlr4::tree::TerminalNode *K_VALUES();
    std::vector<antlr4::tree::TerminalNode *> OPEN_PAR();
    antlr4::tree::TerminalNode* OPEN_PAR(size_t i);
    std::vector<antlr4::tree::TerminalNode *> CLOSE_PAR();
    antlr4::tree::TerminalNode* CLOSE_PAR(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Select_or_valuesContext* select_or_values();

  class  Type_nameContext : public antlr4::ParserRuleContext {
  public:
    Type_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NameContext *> name();
    NameContext* name(size_t i);
    antlr4::tree::TerminalNode *OPEN_PAR();
    std::vector<Signed_numberContext *> signed_number();
    Signed_numberContext* signed_number(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR();
    antlr4::tree::TerminalNode *COMMA();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Type_nameContext* type_name();

  class  ExprContext : public antlr4::ParserRuleContext {
  public:
    ExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Literal_valueContext *literal_value();
    antlr4::tree::TerminalNode *BIND_PARAMETER();
    Column_nameContext *column_name();
    Table_nameContext *table_name();
    std::vector<antlr4::tree::TerminalNode *> DOT();
    antlr4::tree::TerminalNode* DOT(size_t i);
    Database_nameContext *database_name();
    Unary_operatorContext *unary_operator();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    Function_nameContext *function_name();
    antlr4::tree::TerminalNode *OPEN_PAR();
    antlr4::tree::TerminalNode *CLOSE_PAR();
    antlr4::tree::TerminalNode *STAR();
    antlr4::tree::TerminalNode *K_DISTINCT();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *K_CAST();
    antlr4::tree::TerminalNode *K_AS();
    Type_nameContext *type_name();
    Select_stmtContext *select_stmt();
    antlr4::tree::TerminalNode *K_EXISTS();
    antlr4::tree::TerminalNode *K_NOT();
    antlr4::tree::TerminalNode *K_CASE();
    antlr4::tree::TerminalNode *K_END();
    std::vector<antlr4::tree::TerminalNode *> K_WHEN();
    antlr4::tree::TerminalNode* K_WHEN(size_t i);
    std::vector<antlr4::tree::TerminalNode *> K_THEN();
    antlr4::tree::TerminalNode* K_THEN(size_t i);
    antlr4::tree::TerminalNode *K_ELSE();
    Raise_functionContext *raise_function();
    antlr4::tree::TerminalNode *PIPE2();
    antlr4::tree::TerminalNode *DIV();
    antlr4::tree::TerminalNode *MOD();
    antlr4::tree::TerminalNode *PLUS();
    antlr4::tree::TerminalNode *MINUS();
    antlr4::tree::TerminalNode *LT2();
    antlr4::tree::TerminalNode *GT2();
    antlr4::tree::TerminalNode *AMP();
    antlr4::tree::TerminalNode *PIPE();
    antlr4::tree::TerminalNode *LT();
    antlr4::tree::TerminalNode *LT_EQ();
    antlr4::tree::TerminalNode *GT();
    antlr4::tree::TerminalNode *GT_EQ();
    antlr4::tree::TerminalNode *ASSIGN();
    antlr4::tree::TerminalNode *EQ();
    antlr4::tree::TerminalNode *NOT_EQ1();
    antlr4::tree::TerminalNode *NOT_EQ2();
    antlr4::tree::TerminalNode *K_IS();
    antlr4::tree::TerminalNode *K_IN();
    antlr4::tree::TerminalNode *K_LIKE();
    antlr4::tree::TerminalNode *K_GLOB();
    antlr4::tree::TerminalNode *K_MATCH();
    antlr4::tree::TerminalNode *K_REGEXP();
    antlr4::tree::TerminalNode *K_AND();
    antlr4::tree::TerminalNode *K_OR();
    antlr4::tree::TerminalNode *K_BETWEEN();
    antlr4::tree::TerminalNode *K_COLLATE();
    Collation_nameContext *collation_name();
    antlr4::tree::TerminalNode *K_ESCAPE();
    antlr4::tree::TerminalNode *K_ISNULL();
    antlr4::tree::TerminalNode *K_NOTNULL();
    antlr4::tree::TerminalNode *K_NULL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ExprContext* expr();
  ExprContext* expr(int precedence);
  class  Raise_functionContext : public antlr4::ParserRuleContext {
  public:
    Raise_functionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *K_RAISE();
    antlr4::tree::TerminalNode *OPEN_PAR();
    antlr4::tree::TerminalNode *CLOSE_PAR();
    antlr4::tree::TerminalNode *K_IGNORE();
    antlr4::tree::TerminalNode *COMMA();
    Error_messageContext *error_message();
    antlr4::tree::TerminalNode *K_ROLLBACK();
    antlr4::tree::TerminalNode *K_ABORT();
    antlr4::tree::TerminalNode *K_FAIL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Raise_functionContext* raise_function();

  class  Indexed_columnContext : public antlr4::ParserRuleContext {
  public:
    Indexed_columnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Column_nameContext *column_name();
    antlr4::tree::TerminalNode *K_COLLATE();
    Collation_nameContext *collation_name();
    antlr4::tree::TerminalNode *K_ASC();
    antlr4::tree::TerminalNode *K_DESC();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Indexed_columnContext* indexed_column();

  class  With_clauseContext : public antlr4::ParserRuleContext {
  public:
    With_clauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *K_WITH();
    std::vector<Cte_table_nameContext *> cte_table_name();
    Cte_table_nameContext* cte_table_name(size_t i);
    std::vector<antlr4::tree::TerminalNode *> K_AS();
    antlr4::tree::TerminalNode* K_AS(size_t i);
    std::vector<antlr4::tree::TerminalNode *> OPEN_PAR();
    antlr4::tree::TerminalNode* OPEN_PAR(size_t i);
    std::vector<Select_stmtContext *> select_stmt();
    Select_stmtContext* select_stmt(size_t i);
    std::vector<antlr4::tree::TerminalNode *> CLOSE_PAR();
    antlr4::tree::TerminalNode* CLOSE_PAR(size_t i);
    antlr4::tree::TerminalNode *K_RECURSIVE();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  With_clauseContext* with_clause();

  class  Qualified_table_nameContext : public antlr4::ParserRuleContext {
  public:
    Qualified_table_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Table_nameContext *table_name();
    Database_nameContext *database_name();
    antlr4::tree::TerminalNode *DOT();
    antlr4::tree::TerminalNode *K_INDEXED();
    antlr4::tree::TerminalNode *K_BY();
    Index_nameContext *index_name();
    antlr4::tree::TerminalNode *K_NOT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Qualified_table_nameContext* qualified_table_name();

  class  Ordering_termContext : public antlr4::ParserRuleContext {
  public:
    Ordering_termContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExprContext *expr();
    antlr4::tree::TerminalNode *K_COLLATE();
    Collation_nameContext *collation_name();
    antlr4::tree::TerminalNode *K_ASC();
    antlr4::tree::TerminalNode *K_DESC();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Ordering_termContext* ordering_term();

  class  Pragma_valueContext : public antlr4::ParserRuleContext {
  public:
    Pragma_valueContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Signed_numberContext *signed_number();
    NameContext *name();
    antlr4::tree::TerminalNode *STRING_LITERAL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Pragma_valueContext* pragma_value();

  class  Common_table_expressionContext : public antlr4::ParserRuleContext {
  public:
    Common_table_expressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Table_nameContext *table_name();
    antlr4::tree::TerminalNode *K_AS();
    std::vector<antlr4::tree::TerminalNode *> OPEN_PAR();
    antlr4::tree::TerminalNode* OPEN_PAR(size_t i);
    Select_stmtContext *select_stmt();
    std::vector<antlr4::tree::TerminalNode *> CLOSE_PAR();
    antlr4::tree::TerminalNode* CLOSE_PAR(size_t i);
    std::vector<Column_nameContext *> column_name();
    Column_nameContext* column_name(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Common_table_expressionContext* common_table_expression();

  class  Result_columnContext : public antlr4::ParserRuleContext {
  public:
    Result_columnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STAR();
    Table_nameContext *table_name();
    antlr4::tree::TerminalNode *DOT();
    ExprContext *expr();
    Column_aliasContext *column_alias();
    antlr4::tree::TerminalNode *K_AS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Result_columnContext* result_column();

  class  Table_or_subqueryContext : public antlr4::ParserRuleContext {
  public:
    Table_or_subqueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Table_nameContext *table_name();
    Database_nameContext *database_name();
    antlr4::tree::TerminalNode *DOT();
    Table_aliasContext *table_alias();
    antlr4::tree::TerminalNode *K_INDEXED();
    antlr4::tree::TerminalNode *K_BY();
    Index_nameContext *index_name();
    antlr4::tree::TerminalNode *K_NOT();
    antlr4::tree::TerminalNode *K_AS();
    antlr4::tree::TerminalNode *OPEN_PAR();
    antlr4::tree::TerminalNode *CLOSE_PAR();
    std::vector<Table_or_subqueryContext *> table_or_subquery();
    Table_or_subqueryContext* table_or_subquery(size_t i);
    Join_clauseContext *join_clause();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    Select_stmtContext *select_stmt();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Table_or_subqueryContext* table_or_subquery();

  class  Join_clauseContext : public antlr4::ParserRuleContext {
  public:
    Join_clauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Table_or_subqueryContext *> table_or_subquery();
    Table_or_subqueryContext* table_or_subquery(size_t i);
    std::vector<Join_operatorContext *> join_operator();
    Join_operatorContext* join_operator(size_t i);
    std::vector<Join_constraintContext *> join_constraint();
    Join_constraintContext* join_constraint(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Join_clauseContext* join_clause();

  class  Join_operatorContext : public antlr4::ParserRuleContext {
  public:
    Join_operatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COMMA();
    antlr4::tree::TerminalNode *K_JOIN();
    antlr4::tree::TerminalNode *K_NATURAL();
    antlr4::tree::TerminalNode *K_LEFT();
    antlr4::tree::TerminalNode *K_INNER();
    antlr4::tree::TerminalNode *K_CROSS();
    antlr4::tree::TerminalNode *K_OUTER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Join_operatorContext* join_operator();

  class  Join_constraintContext : public antlr4::ParserRuleContext {
  public:
    Join_constraintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *K_ON();
    ExprContext *expr();
    antlr4::tree::TerminalNode *K_USING();
    antlr4::tree::TerminalNode *OPEN_PAR();
    std::vector<Column_nameContext *> column_name();
    Column_nameContext* column_name(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Join_constraintContext* join_constraint();

  class  Select_coreContext : public antlr4::ParserRuleContext {
  public:
    Select_coreContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *K_SELECT();
    std::vector<Result_columnContext *> result_column();
    Result_columnContext* result_column(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *K_FROM();
    antlr4::tree::TerminalNode *K_WHERE();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *K_GROUP();
    antlr4::tree::TerminalNode *K_BY();
    antlr4::tree::TerminalNode *K_DISTINCT();
    antlr4::tree::TerminalNode *K_ALL();
    std::vector<Table_or_subqueryContext *> table_or_subquery();
    Table_or_subqueryContext* table_or_subquery(size_t i);
    Join_clauseContext *join_clause();
    antlr4::tree::TerminalNode *K_HAVING();
    antlr4::tree::TerminalNode *K_VALUES();
    std::vector<antlr4::tree::TerminalNode *> OPEN_PAR();
    antlr4::tree::TerminalNode* OPEN_PAR(size_t i);
    std::vector<antlr4::tree::TerminalNode *> CLOSE_PAR();
    antlr4::tree::TerminalNode* CLOSE_PAR(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Select_coreContext* select_core();

  class  Compound_operatorContext : public antlr4::ParserRuleContext {
  public:
    Compound_operatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *K_UNION();
    antlr4::tree::TerminalNode *K_ALL();
    antlr4::tree::TerminalNode *K_INTERSECT();
    antlr4::tree::TerminalNode *K_EXCEPT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Compound_operatorContext* compound_operator();

  class  Cte_table_nameContext : public antlr4::ParserRuleContext {
  public:
    Cte_table_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Table_nameContext *table_name();
    antlr4::tree::TerminalNode *OPEN_PAR();
    std::vector<Column_nameContext *> column_name();
    Column_nameContext* column_name(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Cte_table_nameContext* cte_table_name();

  class  Signed_numberContext : public antlr4::ParserRuleContext {
  public:
    Signed_numberContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NUMERIC_LITERAL();
    antlr4::tree::TerminalNode *PLUS();
    antlr4::tree::TerminalNode *MINUS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Signed_numberContext* signed_number();

  class  Literal_valueContext : public antlr4::ParserRuleContext {
  public:
    Literal_valueContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NUMERIC_LITERAL();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *BLOB_LITERAL();
    antlr4::tree::TerminalNode *K_NULL();
    antlr4::tree::TerminalNode *K_CURRENT_TIME();
    antlr4::tree::TerminalNode *K_CURRENT_DATE();
    antlr4::tree::TerminalNode *K_CURRENT_TIMESTAMP();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Literal_valueContext* literal_value();

  class  Unary_operatorContext : public antlr4::ParserRuleContext {
  public:
    Unary_operatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MINUS();
    antlr4::tree::TerminalNode *PLUS();
    antlr4::tree::TerminalNode *TILDE();
    antlr4::tree::TerminalNode *K_NOT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Unary_operatorContext* unary_operator();

  class  Error_messageContext : public antlr4::ParserRuleContext {
  public:
    Error_messageContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STRING_LITERAL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Error_messageContext* error_message();

  class  Column_aliasContext : public antlr4::ParserRuleContext {
  public:
    Column_aliasContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();
    antlr4::tree::TerminalNode *STRING_LITERAL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Column_aliasContext* column_alias();

  class  KeywordContext : public antlr4::ParserRuleContext {
  public:
    KeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *K_ABORT();
    antlr4::tree::TerminalNode *K_ACTION();
    antlr4::tree::TerminalNode *K_ADD();
    antlr4::tree::TerminalNode *K_AFTER();
    antlr4::tree::TerminalNode *K_ALL();
    antlr4::tree::TerminalNode *K_ALTER();
    antlr4::tree::TerminalNode *K_ANALYZE();
    antlr4::tree::TerminalNode *K_AND();
    antlr4::tree::TerminalNode *K_AS();
    antlr4::tree::TerminalNode *K_ASC();
    antlr4::tree::TerminalNode *K_ATTACH();
    antlr4::tree::TerminalNode *K_AUTOINCREMENT();
    antlr4::tree::TerminalNode *K_BEFORE();
    antlr4::tree::TerminalNode *K_BEGIN();
    antlr4::tree::TerminalNode *K_BETWEEN();
    antlr4::tree::TerminalNode *K_BY();
    antlr4::tree::TerminalNode *K_CASCADE();
    antlr4::tree::TerminalNode *K_CASE();
    antlr4::tree::TerminalNode *K_CAST();
    antlr4::tree::TerminalNode *K_CHECK();
    antlr4::tree::TerminalNode *K_COLLATE();
    antlr4::tree::TerminalNode *K_COLUMN();
    antlr4::tree::TerminalNode *K_COMMIT();
    antlr4::tree::TerminalNode *K_CONFLICT();
    antlr4::tree::TerminalNode *K_CONSTRAINT();
    antlr4::tree::TerminalNode *K_CREATE();
    antlr4::tree::TerminalNode *K_CROSS();
    antlr4::tree::TerminalNode *K_CURRENT_DATE();
    antlr4::tree::TerminalNode *K_CURRENT_TIME();
    antlr4::tree::TerminalNode *K_CURRENT_TIMESTAMP();
    antlr4::tree::TerminalNode *K_DATABASE();
    antlr4::tree::TerminalNode *K_DEFAULT();
    antlr4::tree::TerminalNode *K_DEFERRABLE();
    antlr4::tree::TerminalNode *K_DEFERRED();
    antlr4::tree::TerminalNode *K_DELETE();
    antlr4::tree::TerminalNode *K_DESC();
    antlr4::tree::TerminalNode *K_DETACH();
    antlr4::tree::TerminalNode *K_DISTINCT();
    antlr4::tree::TerminalNode *K_DROP();
    antlr4::tree::TerminalNode *K_EACH();
    antlr4::tree::TerminalNode *K_ELSE();
    antlr4::tree::TerminalNode *K_END();
    antlr4::tree::TerminalNode *K_ESCAPE();
    antlr4::tree::TerminalNode *K_EXCEPT();
    antlr4::tree::TerminalNode *K_EXCLUSIVE();
    antlr4::tree::TerminalNode *K_EXISTS();
    antlr4::tree::TerminalNode *K_EXPLAIN();
    antlr4::tree::TerminalNode *K_FAIL();
    antlr4::tree::TerminalNode *K_FOR();
    antlr4::tree::TerminalNode *K_FOREIGN();
    antlr4::tree::TerminalNode *K_FROM();
    antlr4::tree::TerminalNode *K_FULL();
    antlr4::tree::TerminalNode *K_GLOB();
    antlr4::tree::TerminalNode *K_GROUP();
    antlr4::tree::TerminalNode *K_HAVING();
    antlr4::tree::TerminalNode *K_IF();
    antlr4::tree::TerminalNode *K_IGNORE();
    antlr4::tree::TerminalNode *K_IMMEDIATE();
    antlr4::tree::TerminalNode *K_IN();
    antlr4::tree::TerminalNode *K_INDEX();
    antlr4::tree::TerminalNode *K_INDEXED();
    antlr4::tree::TerminalNode *K_INITIALLY();
    antlr4::tree::TerminalNode *K_INNER();
    antlr4::tree::TerminalNode *K_INSERT();
    antlr4::tree::TerminalNode *K_INSTEAD();
    antlr4::tree::TerminalNode *K_INTERSECT();
    antlr4::tree::TerminalNode *K_INTO();
    antlr4::tree::TerminalNode *K_IS();
    antlr4::tree::TerminalNode *K_ISNULL();
    antlr4::tree::TerminalNode *K_JOIN();
    antlr4::tree::TerminalNode *K_KEY();
    antlr4::tree::TerminalNode *K_LEFT();
    antlr4::tree::TerminalNode *K_LIKE();
    antlr4::tree::TerminalNode *K_LIMIT();
    antlr4::tree::TerminalNode *K_MATCH();
    antlr4::tree::TerminalNode *K_NATURAL();
    antlr4::tree::TerminalNode *K_NO();
    antlr4::tree::TerminalNode *K_NOT();
    antlr4::tree::TerminalNode *K_NOTNULL();
    antlr4::tree::TerminalNode *K_NULL();
    antlr4::tree::TerminalNode *K_OF();
    antlr4::tree::TerminalNode *K_OFFSET();
    antlr4::tree::TerminalNode *K_ON();
    antlr4::tree::TerminalNode *K_OR();
    antlr4::tree::TerminalNode *K_ORDER();
    antlr4::tree::TerminalNode *K_OUTER();
    antlr4::tree::TerminalNode *K_PLAN();
    antlr4::tree::TerminalNode *K_PRAGMA();
    antlr4::tree::TerminalNode *K_PRIMARY();
    antlr4::tree::TerminalNode *K_QUERY();
    antlr4::tree::TerminalNode *K_RAISE();
    antlr4::tree::TerminalNode *K_RECURSIVE();
    antlr4::tree::TerminalNode *K_REFERENCES();
    antlr4::tree::TerminalNode *K_REGEXP();
    antlr4::tree::TerminalNode *K_REINDEX();
    antlr4::tree::TerminalNode *K_RELEASE();
    antlr4::tree::TerminalNode *K_RENAME();
    antlr4::tree::TerminalNode *K_REPLACE();
    antlr4::tree::TerminalNode *K_RESTRICT();
    antlr4::tree::TerminalNode *K_RIGHT();
    antlr4::tree::TerminalNode *K_ROLLBACK();
    antlr4::tree::TerminalNode *K_ROW();
    antlr4::tree::TerminalNode *K_SAVEPOINT();
    antlr4::tree::TerminalNode *K_SELECT();
    antlr4::tree::TerminalNode *K_SET();
    antlr4::tree::TerminalNode *K_TABLE();
    antlr4::tree::TerminalNode *K_TEMP();
    antlr4::tree::TerminalNode *K_TEMPORARY();
    antlr4::tree::TerminalNode *K_THEN();
    antlr4::tree::TerminalNode *K_TO();
    antlr4::tree::TerminalNode *K_TRANSACTION();
    antlr4::tree::TerminalNode *K_TRIGGER();
    antlr4::tree::TerminalNode *K_UNION();
    antlr4::tree::TerminalNode *K_UNIQUE();
    antlr4::tree::TerminalNode *K_UPDATE();
    antlr4::tree::TerminalNode *K_USING();
    antlr4::tree::TerminalNode *K_VACUUM();
    antlr4::tree::TerminalNode *K_VALUES();
    antlr4::tree::TerminalNode *K_VIEW();
    antlr4::tree::TerminalNode *K_VIRTUAL();
    antlr4::tree::TerminalNode *K_WHEN();
    antlr4::tree::TerminalNode *K_WHERE();
    antlr4::tree::TerminalNode *K_WITH();
    antlr4::tree::TerminalNode *K_WITHOUT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  KeywordContext* keyword();

  class  NameContext : public antlr4::ParserRuleContext {
  public:
    NameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NameContext* name();

  class  Function_nameContext : public antlr4::ParserRuleContext {
  public:
    Function_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Function_nameContext* function_name();

  class  Database_nameContext : public antlr4::ParserRuleContext {
  public:
    Database_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Database_nameContext* database_name();

  class  Table_nameContext : public antlr4::ParserRuleContext {
  public:
    Table_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Table_nameContext* table_name();

  class  Table_or_index_nameContext : public antlr4::ParserRuleContext {
  public:
    Table_or_index_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Table_or_index_nameContext* table_or_index_name();

  class  New_table_nameContext : public antlr4::ParserRuleContext {
  public:
    New_table_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  New_table_nameContext* new_table_name();

  class  Column_nameContext : public antlr4::ParserRuleContext {
  public:
    Column_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Column_nameContext* column_name();

  class  Collation_nameContext : public antlr4::ParserRuleContext {
  public:
    Collation_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Collation_nameContext* collation_name();

  class  Foreign_tableContext : public antlr4::ParserRuleContext {
  public:
    Foreign_tableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Foreign_tableContext* foreign_table();

  class  Index_nameContext : public antlr4::ParserRuleContext {
  public:
    Index_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Index_nameContext* index_name();

  class  Trigger_nameContext : public antlr4::ParserRuleContext {
  public:
    Trigger_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Trigger_nameContext* trigger_name();

  class  View_nameContext : public antlr4::ParserRuleContext {
  public:
    View_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  View_nameContext* view_name();

  class  Module_nameContext : public antlr4::ParserRuleContext {
  public:
    Module_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Module_nameContext* module_name();

  class  Pragma_nameContext : public antlr4::ParserRuleContext {
  public:
    Pragma_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Pragma_nameContext* pragma_name();

  class  Savepoint_nameContext : public antlr4::ParserRuleContext {
  public:
    Savepoint_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Savepoint_nameContext* savepoint_name();

  class  Table_aliasContext : public antlr4::ParserRuleContext {
  public:
    Table_aliasContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Table_aliasContext* table_alias();

  class  Transaction_nameContext : public antlr4::ParserRuleContext {
  public:
    Transaction_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Any_nameContext *any_name();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Transaction_nameContext* transaction_name();

  class  Any_nameContext : public antlr4::ParserRuleContext {
  public:
    Any_nameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();
    KeywordContext *keyword();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *OPEN_PAR();
    Any_nameContext *any_name();
    antlr4::tree::TerminalNode *CLOSE_PAR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Any_nameContext* any_name();


  virtual bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;
  bool exprSempred(ExprContext */*_localctx*/, size_t predicateIndex);

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

}  // namespace normal::sql
