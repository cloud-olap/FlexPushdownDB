
// Generated from /home/matt/Work/pushdownDB/normal/normal-sql/grammar/NormalSQL.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"
#include "NormalSQLListener.h"


namespace normal::sql {

/**
 * This class provides an empty implementation of NormalSQLListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  NormalSQLBaseListener : public NormalSQLListener {
public:

  virtual void enterParse(NormalSQLParser::ParseContext * /*ctx*/) override { }
  virtual void exitParse(NormalSQLParser::ParseContext * /*ctx*/) override { }

  virtual void enterError(NormalSQLParser::ErrorContext * /*ctx*/) override { }
  virtual void exitError(NormalSQLParser::ErrorContext * /*ctx*/) override { }

  virtual void enterSql_stmt_list(NormalSQLParser::Sql_stmt_listContext * /*ctx*/) override { }
  virtual void exitSql_stmt_list(NormalSQLParser::Sql_stmt_listContext * /*ctx*/) override { }

  virtual void enterSql_stmt(NormalSQLParser::Sql_stmtContext * /*ctx*/) override { }
  virtual void exitSql_stmt(NormalSQLParser::Sql_stmtContext * /*ctx*/) override { }

  virtual void enterCompound_select_stmt(NormalSQLParser::Compound_select_stmtContext * /*ctx*/) override { }
  virtual void exitCompound_select_stmt(NormalSQLParser::Compound_select_stmtContext * /*ctx*/) override { }

  virtual void enterFactored_select_stmt(NormalSQLParser::Factored_select_stmtContext * /*ctx*/) override { }
  virtual void exitFactored_select_stmt(NormalSQLParser::Factored_select_stmtContext * /*ctx*/) override { }

  virtual void enterSimple_select_stmt(NormalSQLParser::Simple_select_stmtContext * /*ctx*/) override { }
  virtual void exitSimple_select_stmt(NormalSQLParser::Simple_select_stmtContext * /*ctx*/) override { }

  virtual void enterSelect_stmt(NormalSQLParser::Select_stmtContext * /*ctx*/) override { }
  virtual void exitSelect_stmt(NormalSQLParser::Select_stmtContext * /*ctx*/) override { }

  virtual void enterSelect_or_values(NormalSQLParser::Select_or_valuesContext * /*ctx*/) override { }
  virtual void exitSelect_or_values(NormalSQLParser::Select_or_valuesContext * /*ctx*/) override { }

  virtual void enterType_name(NormalSQLParser::Type_nameContext * /*ctx*/) override { }
  virtual void exitType_name(NormalSQLParser::Type_nameContext * /*ctx*/) override { }

  virtual void enterExpr(NormalSQLParser::ExprContext * /*ctx*/) override { }
  virtual void exitExpr(NormalSQLParser::ExprContext * /*ctx*/) override { }

  virtual void enterRaise_function(NormalSQLParser::Raise_functionContext * /*ctx*/) override { }
  virtual void exitRaise_function(NormalSQLParser::Raise_functionContext * /*ctx*/) override { }

  virtual void enterIndexed_column(NormalSQLParser::Indexed_columnContext * /*ctx*/) override { }
  virtual void exitIndexed_column(NormalSQLParser::Indexed_columnContext * /*ctx*/) override { }

  virtual void enterWith_clause(NormalSQLParser::With_clauseContext * /*ctx*/) override { }
  virtual void exitWith_clause(NormalSQLParser::With_clauseContext * /*ctx*/) override { }

  virtual void enterQualified_table_name(NormalSQLParser::Qualified_table_nameContext * /*ctx*/) override { }
  virtual void exitQualified_table_name(NormalSQLParser::Qualified_table_nameContext * /*ctx*/) override { }

  virtual void enterOrdering_term(NormalSQLParser::Ordering_termContext * /*ctx*/) override { }
  virtual void exitOrdering_term(NormalSQLParser::Ordering_termContext * /*ctx*/) override { }

  virtual void enterPragma_value(NormalSQLParser::Pragma_valueContext * /*ctx*/) override { }
  virtual void exitPragma_value(NormalSQLParser::Pragma_valueContext * /*ctx*/) override { }

  virtual void enterCommon_table_expression(NormalSQLParser::Common_table_expressionContext * /*ctx*/) override { }
  virtual void exitCommon_table_expression(NormalSQLParser::Common_table_expressionContext * /*ctx*/) override { }

  virtual void enterResult_column(NormalSQLParser::Result_columnContext * /*ctx*/) override { }
  virtual void exitResult_column(NormalSQLParser::Result_columnContext * /*ctx*/) override { }

  virtual void enterTable_or_subquery(NormalSQLParser::Table_or_subqueryContext * /*ctx*/) override { }
  virtual void exitTable_or_subquery(NormalSQLParser::Table_or_subqueryContext * /*ctx*/) override { }

  virtual void enterJoin_clause(NormalSQLParser::Join_clauseContext * /*ctx*/) override { }
  virtual void exitJoin_clause(NormalSQLParser::Join_clauseContext * /*ctx*/) override { }

  virtual void enterJoin_operator(NormalSQLParser::Join_operatorContext * /*ctx*/) override { }
  virtual void exitJoin_operator(NormalSQLParser::Join_operatorContext * /*ctx*/) override { }

  virtual void enterJoin_constraint(NormalSQLParser::Join_constraintContext * /*ctx*/) override { }
  virtual void exitJoin_constraint(NormalSQLParser::Join_constraintContext * /*ctx*/) override { }

  virtual void enterSelect_core(NormalSQLParser::Select_coreContext * /*ctx*/) override { }
  virtual void exitSelect_core(NormalSQLParser::Select_coreContext * /*ctx*/) override { }

  virtual void enterCompound_operator(NormalSQLParser::Compound_operatorContext * /*ctx*/) override { }
  virtual void exitCompound_operator(NormalSQLParser::Compound_operatorContext * /*ctx*/) override { }

  virtual void enterCte_table_name(NormalSQLParser::Cte_table_nameContext * /*ctx*/) override { }
  virtual void exitCte_table_name(NormalSQLParser::Cte_table_nameContext * /*ctx*/) override { }

  virtual void enterSigned_number(NormalSQLParser::Signed_numberContext * /*ctx*/) override { }
  virtual void exitSigned_number(NormalSQLParser::Signed_numberContext * /*ctx*/) override { }

  virtual void enterLiteral_value(NormalSQLParser::Literal_valueContext * /*ctx*/) override { }
  virtual void exitLiteral_value(NormalSQLParser::Literal_valueContext * /*ctx*/) override { }

  virtual void enterUnary_operator(NormalSQLParser::Unary_operatorContext * /*ctx*/) override { }
  virtual void exitUnary_operator(NormalSQLParser::Unary_operatorContext * /*ctx*/) override { }

  virtual void enterError_message(NormalSQLParser::Error_messageContext * /*ctx*/) override { }
  virtual void exitError_message(NormalSQLParser::Error_messageContext * /*ctx*/) override { }

  virtual void enterColumn_alias(NormalSQLParser::Column_aliasContext * /*ctx*/) override { }
  virtual void exitColumn_alias(NormalSQLParser::Column_aliasContext * /*ctx*/) override { }

  virtual void enterKeyword(NormalSQLParser::KeywordContext * /*ctx*/) override { }
  virtual void exitKeyword(NormalSQLParser::KeywordContext * /*ctx*/) override { }

  virtual void enterName(NormalSQLParser::NameContext * /*ctx*/) override { }
  virtual void exitName(NormalSQLParser::NameContext * /*ctx*/) override { }

  virtual void enterFunction_name(NormalSQLParser::Function_nameContext * /*ctx*/) override { }
  virtual void exitFunction_name(NormalSQLParser::Function_nameContext * /*ctx*/) override { }

  virtual void enterDatabase_name(NormalSQLParser::Database_nameContext * /*ctx*/) override { }
  virtual void exitDatabase_name(NormalSQLParser::Database_nameContext * /*ctx*/) override { }

  virtual void enterTable_name(NormalSQLParser::Table_nameContext * /*ctx*/) override { }
  virtual void exitTable_name(NormalSQLParser::Table_nameContext * /*ctx*/) override { }

  virtual void enterTable_or_index_name(NormalSQLParser::Table_or_index_nameContext * /*ctx*/) override { }
  virtual void exitTable_or_index_name(NormalSQLParser::Table_or_index_nameContext * /*ctx*/) override { }

  virtual void enterNew_table_name(NormalSQLParser::New_table_nameContext * /*ctx*/) override { }
  virtual void exitNew_table_name(NormalSQLParser::New_table_nameContext * /*ctx*/) override { }

  virtual void enterColumn_name(NormalSQLParser::Column_nameContext * /*ctx*/) override { }
  virtual void exitColumn_name(NormalSQLParser::Column_nameContext * /*ctx*/) override { }

  virtual void enterCollation_name(NormalSQLParser::Collation_nameContext * /*ctx*/) override { }
  virtual void exitCollation_name(NormalSQLParser::Collation_nameContext * /*ctx*/) override { }

  virtual void enterForeign_table(NormalSQLParser::Foreign_tableContext * /*ctx*/) override { }
  virtual void exitForeign_table(NormalSQLParser::Foreign_tableContext * /*ctx*/) override { }

  virtual void enterIndex_name(NormalSQLParser::Index_nameContext * /*ctx*/) override { }
  virtual void exitIndex_name(NormalSQLParser::Index_nameContext * /*ctx*/) override { }

  virtual void enterTrigger_name(NormalSQLParser::Trigger_nameContext * /*ctx*/) override { }
  virtual void exitTrigger_name(NormalSQLParser::Trigger_nameContext * /*ctx*/) override { }

  virtual void enterView_name(NormalSQLParser::View_nameContext * /*ctx*/) override { }
  virtual void exitView_name(NormalSQLParser::View_nameContext * /*ctx*/) override { }

  virtual void enterModule_name(NormalSQLParser::Module_nameContext * /*ctx*/) override { }
  virtual void exitModule_name(NormalSQLParser::Module_nameContext * /*ctx*/) override { }

  virtual void enterPragma_name(NormalSQLParser::Pragma_nameContext * /*ctx*/) override { }
  virtual void exitPragma_name(NormalSQLParser::Pragma_nameContext * /*ctx*/) override { }

  virtual void enterSavepoint_name(NormalSQLParser::Savepoint_nameContext * /*ctx*/) override { }
  virtual void exitSavepoint_name(NormalSQLParser::Savepoint_nameContext * /*ctx*/) override { }

  virtual void enterTable_alias(NormalSQLParser::Table_aliasContext * /*ctx*/) override { }
  virtual void exitTable_alias(NormalSQLParser::Table_aliasContext * /*ctx*/) override { }

  virtual void enterTransaction_name(NormalSQLParser::Transaction_nameContext * /*ctx*/) override { }
  virtual void exitTransaction_name(NormalSQLParser::Transaction_nameContext * /*ctx*/) override { }

  virtual void enterAny_name(NormalSQLParser::Any_nameContext * /*ctx*/) override { }
  virtual void exitAny_name(NormalSQLParser::Any_nameContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

}  // namespace normal::sql
