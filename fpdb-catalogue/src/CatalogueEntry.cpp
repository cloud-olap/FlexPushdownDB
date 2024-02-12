//
// Created by matt on 27/3/20.
//

#include <fpdb/catalogue/CatalogueEntry.h>

using namespace fpdb::catalogue;

CatalogueEntry::CatalogueEntry(CatalogueEntryType type,
                               string schemaName,
                               const shared_ptr<Catalogue>& Catalogue) :
  type_(type),
  schemaName_(move(schemaName)),
  catalogue_(Catalogue) {}

const string &CatalogueEntry::getSchemaName() const {
  return schemaName_;
}

const weak_ptr<Catalogue> &CatalogueEntry::getCatalogue() const {
  return catalogue_;
}

CatalogueEntryType CatalogueEntry::getType() const {
  return type_;
}
