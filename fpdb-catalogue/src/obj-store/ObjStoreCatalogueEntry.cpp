//
// Created by matt on 27/3/20.
//

#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntry.h>
#include <fpdb/catalogue/CatalogueEntryType.h>
#include <fmt/format.h>
#include <utility>

using namespace fpdb::catalogue;

namespace fpdb::catalogue::obj_store {

ObjStoreCatalogueEntry::ObjStoreCatalogueEntry(ObjStoreType storeType,
                                               string schemaName,
                                               string bucket,
                                               shared_ptr<Catalogue> catalogue) :
  CatalogueEntry(OBJ_STORE,
                 move(schemaName),
                 move(catalogue)),
  storeType_(storeType),
  bucket_(move(bucket)) {}

ObjStoreType ObjStoreCatalogueEntry::getStoreType() const {
  return storeType_;
}

const string &ObjStoreCatalogueEntry::getBucket() const {
  return bucket_;
}

vector<shared_ptr<ObjStoreTable>> ObjStoreCatalogueEntry::getTables() const {
  vector<shared_ptr<ObjStoreTable>> tables;
  for (const auto &it: tableMap_) {
    tables.emplace_back(it.second);
  }
  return tables;
}

const shared_ptr<ObjStoreTable> &ObjStoreCatalogueEntry::getTable(const string& tableName) const {
  const auto &it = tableMap_.find(tableName);
  if (it == tableMap_.end()) {
    throw runtime_error(fmt::format("ObjStoreTable not found: {}", tableName));
  }
  return it->second;
}

string ObjStoreCatalogueEntry::getTypeName() const {
  return "ObjStoreCatalogueEntry";
}

void ObjStoreCatalogueEntry::addTable(const shared_ptr<ObjStoreTable> &objStoreTable) {
  tableMap_.emplace(objStoreTable->getName(), objStoreTable);
}

string ObjStoreCatalogueEntry::getName() const {
  auto storeName = storeType_ == ObjStoreType::S3 ? "s3" : "fpdb-store";
  return fmt::format("{}://{}/{}", storeName, bucket_, getSchemaName());
}

string ObjStoreCatalogueEntry::getStoreTypeName() const {
  switch (storeType_) {
    case ObjStoreType::S3: return "S3";
    case ObjStoreType::FPDB_STORE: return "FPDB-Store";
    default: return "Unknown";
  }
}

}
