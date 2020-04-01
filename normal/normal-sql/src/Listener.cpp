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

Listener::Listener(
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues,
    std::shared_ptr<normal::core::OperatorManager> OperatorManager) :
    catalogues_(std::move(catalogues)),
    operatorManager_(std::move(OperatorManager)) {}

void Listener::enterSelect_core(normal::sql::NormalSQLParser::Select_coreContext *Context) {

  auto scan = std::make_shared<ScanNode>();
  auto collate = std::make_shared<CollateNode>();

  auto catalogueName = Context->table_or_subquery(0)->database_name()->any_name()->IDENTIFIER()->getText();
  auto tableName = Context->table_or_subquery(0)->table_name()->any_name()->IDENTIFIER()->getText();

  auto catalogue = this->catalogues_->find(catalogueName);
  if (catalogue == this->catalogues_->end())
    throw std::runtime_error("Catalogue '" + catalogueName + "' not found.");

  auto localFS = std::static_pointer_cast<LocalFileSystemCatalogueEntry>(catalogue->second->getEntry(tableName));

  scan->name = catalogueName + "." + tableName;
  scan->tableName = localFS;

  collate->name = "collate";

  scan->consumer = collate;

  symbolTable.table.insert(std::pair(0, scan));
  symbolTable.table.insert(std::pair(1, collate));
}


