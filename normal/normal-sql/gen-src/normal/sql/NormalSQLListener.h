
// Generated from /home/matt/Work/pushdownDB/normal/normal-sql/grammar/NormalSQL.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"
#include "NormalSQLParser.h"


namespace normal::sql {

/**
 * This interface defines an abstract listener for a parse tree produced by NormalSQLParser.
 */
class  NormalSQLListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterParse(NormalSQLParser::ParseContext *ctx) = 0;
  virtual void exitParse(NormalSQLParser::ParseContext *ctx) = 0;

  virtual void enterError(NormalSQLParser::ErrorContext *ctx) = 0;
  virtual void exitError(NormalSQLParser::ErrorContext *ctx) = 0;

  virtual void enterSql_stmt_list(NormalSQLParser::Sql_stmt_listContext *ctx) = 0;
  virtual void exitSql_stmt_list(NormalSQLParser::Sql_stmt_listContext *ctx) = 0;

  virtual void enterSql_stmt(NormalSQLParser::Sql_stmtContext *ctx) = 0;
  virtual void exitSql_stmt(NormalSQLParser::Sql_stmtContext *ctx) = 0;

  virtual void enterCompound_select_stmt(NormalSQLParser::Compound_select_stmtContext *ctx) = 0;
  virtual void exitCompound_select_stmt(NormalSQLParser::Compound_select_stmtContext *ctx) = 0;

  virtual void enterFactored_select_stmt(NormalSQLParser::Factored_select_stmtContext *ctx) = 0;
  virtual void exitFactored_select_stmt(NormalSQLParser::Factored_select_stmtContext *ctx) = 0;

  virtual void enterSimple_select_stmt(NormalSQLParser::Simple_select_stmtContext *ctx) = 0;
  virtual void exitSimple_select_stmt(NormalSQLParser::Simple_select_stmtContext *ctx) = 0;

  virtual void enterSelect_stmt(NormalSQLParser::Select_stmtContext *ctx) = 0;
  virtual void exitSelect_stmt(NormalSQLParser::Select_stmtContext *ctx) = 0;

  virtual void enterSelect_or_values(NormalSQLParser::Select_or_valuesContext *ctx) = 0;
  virtual void exitSelect_or_values(NormalSQLParser::Select_or_valuesContext *ctx) = 0;

  virtual void enterType_name(NormalSQLParser::Type_nameContext *ctx) = 0;
  virtual void exitType_name(NormalSQLParser::Type_nameContext *ctx) = 0;

  virtual void enterExpr(NormalSQLParser::ExprContext *ctx) = 0;
  virtual void exitExpr(NormalSQLParser::ExprContext *ctx) = 0;

  virtual void enterRaise_function(NormalSQLParser::Raise_functionContext *ctx) = 0;
  virtual void exitRaise_function(NormalSQLParser::Raise_functionContext *ctx) = 0;

  virtual void enterIndexed_column(NormalSQLParser::Indexed_columnContext *ctx) = 0;
  virtual void exitIndexed_column(NormalSQLParser::Indexed_columnContext *ctx) = 0;

  virtual void enterWith_clause(NormalSQLParser::With_clauseContext *ctx) = 0;
  virtual void exitWith_clause(NormalSQLParser::With_clauseContext *ctx) = 0;

  virtual void enterQualified_table_name(NormalSQLParser::Qualified_table_nameContext *ctx) = 0;
  virtual void exitQualified_table_name(NormalSQLParser::Qualified_table_nameContext *ctx) = 0;

  virtual void enterOrdering_term(NormalSQLParser::Ordering_termContext *ctx) = 0;
  virtual void exitOrdering_term(NormalSQLParser::Ordering_termContext *ctx) = 0;

  virtual void enterPragma_value(NormalSQLParser::Pragma_valueContext *ctx) = 0;
  virtual void exitPragma_value(NormalSQLParser::Pragma_valueContext *ctx) = 0;

  virtual void enterCommon_table_expression(NormalSQLParser::Common_table_expressionContext *ctx) = 0;
  virtual void exitCommon_table_expression(NormalSQLParser::Common_table_expressionContext *ctx) = 0;

  virtual void enterResult_column(NormalSQLParser::Result_columnContext *ctx) = 0;
  virtual void exitResult_column(NormalSQLParser::Result_columnContext *ctx) = 0;

  virtual void enterTable_or_subquery(NormalSQLParser::Table_or_subqueryContext *ctx) = 0;
  virtual void exitTable_or_subquery(NormalSQLParser::Table_or_subqueryContext *ctx) = 0;

  virtual void enterJoin_clause(NormalSQLParser::Join_clauseContext *ctx) = 0;
  virtual void exitJoin_clause(NormalSQLParser::Join_clauseContext *ctx) = 0;

  virtual void enterJoin_operator(NormalSQLParser::Join_operatorContext *ctx) = 0;
  virtual void exitJoin_operator(NormalSQLParser::Join_operatorContext *ctx) = 0;

  virtual void enterJoin_constraint(NormalSQLParser::Join_constraintContext *ctx) = 0;
  virtual void exitJoin_constraint(NormalSQLParser::Join_constraintContext *ctx) = 0;

  virtual void enterSelect_core(NormalSQLParser::Select_coreContext *ctx) = 0;
  virtual void exitSelect_core(NormalSQLParser::Select_coreContext *ctx) = 0;

  virtual void enterCompound_operator(NormalSQLParser::Compound_operatorContext *ctx) = 0;
  virtual void exitCompound_operator(NormalSQLParser::Compound_operatorContext *ctx) = 0;

  virtual void enterCte_table_name(NormalSQLParser::Cte_table_nameContext *ctx) = 0;
  virtual void exitCte_table_name(NormalSQLParser::Cte_table_nameContext *ctx) = 0;

  virtual void enterSigned_number(NormalSQLParser::Signed_numberContext *ctx) = 0;
  virtual void exitSigned_number(NormalSQLParser::Signed_numberContext *ctx) = 0;

  virtual void enterLiteral_value(NormalSQLParser::Literal_valueContext *ctx) = 0;
  virtual void exitLiteral_value(NormalSQLParser::Literal_valueContext *ctx) = 0;

  virtual void enterUnary_operator(NormalSQLParser::Unary_operatorContext *ctx) = 0;
  virtual void exitUnary_operator(NormalSQLParser::Unary_operatorContext *ctx) = 0;

  virtual void enterError_message(NormalSQLParser::Error_messageContext *ctx) = 0;
  virtual void exitError_message(NormalSQLParser::Error_messageContext *ctx) = 0;

  virtual void enterColumn_alias(NormalSQLParser::Column_aliasContext *ctx) = 0;
  virtual void exitColumn_alias(NormalSQLParser::Column_aliasContext *ctx) = 0;

  virtual void enterKeyword(NormalSQLParser::KeywordContext *ctx) = 0;
  virtual void exitKeyword(NormalSQLParser::KeywordContext *ctx) = 0;

  virtual void enterName(NormalSQLParser::NameContext *ctx) = 0;
  virtual void exitName(NormalSQLParser::NameContext *ctx) = 0;

  virtual void enterFunction_name(NormalSQLParser::Function_nameContext *ctx) = 0;
  virtual void exitFunction_name(NormalSQLParser::Function_nameContext *ctx) = 0;

  virtual void enterDatabase_name(NormalSQLParser::Database_nameContext *ctx) = 0;
  virtual void exitDatabase_name(NormalSQLParser::Database_nameContext *ctx) = 0;

  virtual void enterTable_name(NormalSQLParser::Table_nameContext *ctx) = 0;
  virtual void exitTable_name(NormalSQLParser::Table_nameContext *ctx) = 0;

  virtual void enterTable_or_index_name(NormalSQLParser::Table_or_index_nameContext *ctx) = 0;
  virtual void exitTable_or_index_name(NormalSQLParser::Table_or_index_nameContext *ctx) = 0;

  virtual void enterNew_table_name(NormalSQLParser::New_table_nameContext *ctx) = 0;
  virtual void exitNew_table_name(NormalSQLParser::New_table_nameContext *ctx) = 0;

  virtual void enterColumn_name(NormalSQLParser::Column_nameContext *ctx) = 0;
  virtual void exitColumn_name(NormalSQLParser::Column_nameContext *ctx) = 0;

  virtual void enterCollation_name(NormalSQLParser::Collation_nameContext *ctx) = 0;
  virtual void exitCollation_name(NormalSQLParser::Collation_nameContext *ctx) = 0;

  virtual void enterForeign_table(NormalSQLParser::Foreign_tableContext *ctx) = 0;
  virtual void exitForeign_table(NormalSQLParser::Foreign_tableContext *ctx) = 0;

  virtual void enterIndex_name(NormalSQLParser::Index_nameContext *ctx) = 0;
  virtual void exitIndex_name(NormalSQLParser::Index_nameContext *ctx) = 0;

  virtual void enterTrigger_name(NormalSQLParser::Trigger_nameContext *ctx) = 0;
  virtual void exitTrigger_name(NormalSQLParser::Trigger_nameContext *ctx) = 0;

  virtual void enterView_name(NormalSQLParser::View_nameContext *ctx) = 0;
  virtual void exitView_name(NormalSQLParser::View_nameContext *ctx) = 0;

  virtual void enterModule_name(NormalSQLParser::Module_nameContext *ctx) = 0;
  virtual void exitModule_name(NormalSQLParser::Module_nameContext *ctx) = 0;

  virtual void enterPragma_name(NormalSQLParser::Pragma_nameContext *ctx) = 0;
  virtual void exitPragma_name(NormalSQLParser::Pragma_nameContext *ctx) = 0;

  virtual void enterSavepoint_name(NormalSQLParser::Savepoint_nameContext *ctx) = 0;
  virtual void exitSavepoint_name(NormalSQLParser::Savepoint_nameContext *ctx) = 0;

  virtual void enterTable_alias(NormalSQLParser::Table_aliasContext *ctx) = 0;
  virtual void exitTable_alias(NormalSQLParser::Table_aliasContext *ctx) = 0;

  virtual void enterTransaction_name(NormalSQLParser::Transaction_nameContext *ctx) = 0;
  virtual void exitTransaction_name(NormalSQLParser::Transaction_nameContext *ctx) = 0;

  virtual void enterAny_name(NormalSQLParser::Any_nameContext *ctx) = 0;
  virtual void exitAny_name(NormalSQLParser::Any_nameContext *ctx) = 0;


};

}  // namespace normal::sql
