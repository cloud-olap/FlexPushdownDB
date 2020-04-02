//
// Created by matt on 26/3/20.
//

#include "Listener.h"

#include <utility>
#include <normal/pushdown/FileScan.h>
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/pushdown/Collate.h>
#include "Globals.h"
#include "ast/ScanNode.h"
#include "ast/CollateNode.h"
#include "ast/AggregateNode.h"
#include "ast/AggregateExpression.h"

Listener::Listener(
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues,
    std::shared_ptr<normal::core::OperatorManager> OperatorManager) :
    catalogues_(std::move(catalogues)),
    operatorManager_(std::move(OperatorManager)) {}

void Listener::exitSelect_core(normal::sql::NormalSQLParser::Select_coreContext *Context) {

  auto scan = std::make_shared<ScanNode>();
  auto collate = std::make_shared<CollateNode>();

  collate->name = "collate";



  symbolTable.table.insert(std::pair(0, scan));
  symbolTable.table.insert(std::pair(1, collate));

  // Scan table source
  auto catalogueName = Context->table_or_subquery(0)->database_name()->any_name()->IDENTIFIER()->getText();
  auto tableName = Context->table_or_subquery(0)->table_name()->any_name()->IDENTIFIER()->getText();

  auto catalogue = this->catalogues_->find(catalogueName);
  if (catalogue == this->catalogues_->end())
    throw std::runtime_error("Catalogue '" + catalogueName + "' not found.");

  auto localFS = std::static_pointer_cast<LocalFileSystemCatalogueEntry>(catalogue->second->getEntry(tableName));

  scan->name = catalogueName + "." + tableName;
  scan->tableName = localFS;

  // Aggregate functions
  auto functionNameContext = Context->result_column(0)->expr()->function_name();

  if(functionNameContext != nullptr){
    auto functionName = functionNameContext->any_name()->IDENTIFIER()->toString();
    std::transform(functionName.begin(), functionName.end(), functionName.begin(), ::tolower);
    if(functionName =="sum"){

      auto aggregate = std::make_shared<AggregateNode>();

      auto aggregateFunction = std::make_shared<AggregateFunction>("sum");

      auto columnNameExpressionContext = Context->result_column(0)->expr()->expr(0)->column_name();
      auto columnName = columnNameExpressionContext->any_name()->IDENTIFIER()->getText();

      auto aggregateExpression = std::make_shared<AggregateExpression>(columnName);
      aggregateFunction->expression = aggregateExpression;

      aggregate->functions.emplace_back(aggregateFunction);

      aggregate->name = "sum(" + columnName + ")";

      symbolTable.table.insert(std::pair(2, aggregate));

      scan->consumer = aggregate;
      aggregate->consumer = collate;
    }
  }
  else{
    scan->consumer = collate;
  }
}


