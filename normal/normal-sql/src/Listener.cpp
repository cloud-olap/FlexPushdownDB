//
// Created by matt on 26/3/20.
//

#include "Listener.h"

#include <utility>
#include <normal/pushdown/FileScan.h>
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include "Globals.h"

Listener::Listener(
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues,
    std::shared_ptr<normal::core::OperatorManager> OperatorManager) :
    catalogues_(std::move(catalogues)),
    operatorManager_(std::move(OperatorManager)) {}

void Listener::enterSelect_core(normal::sql::NormalSQLParser::Select_coreContext *Context) {
  SPDLOG_DEBUG(Context->toString());

  std::string databaseName = Context->table_or_subquery()[0]->database_name()->any_name()->IDENTIFIER()->toString();
  std::string tableName = Context->table_or_subquery()[0]->table_name()->any_name()->IDENTIFIER()->toString();

  auto catalogue = this->catalogues_->find(databaseName);

  if (catalogue == this->catalogues_->end())
    throw std::runtime_error("Catalogue '" + databaseName + "' not found.");

  std::shared_ptr<CatalogueEntry> catalogueEntry = catalogue->second->getEntry(tableName);

  std::shared_ptr<LocalFileSystemCatalogueEntry>
      entry = std::dynamic_pointer_cast<LocalFileSystemCatalogueEntry>(catalogueEntry);

  auto fileScan =
      std::make_shared<normal::pushdown::FileScan>("FileScan." + databaseName + "." + tableName, entry->getPath());

  NormalSQLBaseListener::enterSelect_core(Context);
}


