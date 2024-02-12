//
// Created by matt on 27/3/20.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECATALOGUEENTRY_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECATALOGUEENTRY_H

#include <fpdb/catalogue/CatalogueEntry.h>
#include <fpdb/catalogue/Catalogue.h>
#include <fpdb/catalogue/obj-store/ObjStoreType.h>
#include <fpdb/catalogue/obj-store/ObjStoreTable.h>
#include <string>

using namespace fpdb::catalogue;
using namespace std;

namespace fpdb::catalogue::obj_store {

class ObjStoreCatalogueEntry : public CatalogueEntry {

public:
  ObjStoreCatalogueEntry(ObjStoreType storeType,
                         string schemaName,
                         string bucket,
                         shared_ptr<Catalogue> catalogue);
  ~ObjStoreCatalogueEntry() override = default;

  ObjStoreType getStoreType() const;
  const string &getBucket() const;
  vector<shared_ptr<ObjStoreTable>> getTables() const;
  const shared_ptr<ObjStoreTable> &getTable(const string& tableName) const;
  string getTypeName() const override;
  string getName() const override;
  string getStoreTypeName() const;

  void addTable(const shared_ptr<ObjStoreTable> &objStoreTable);

private:
  ObjStoreType storeType_;
  string bucket_;
  unordered_map<string, shared_ptr<ObjStoreTable>> tableMap_;
};

}

#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECATALOGUEENTRY_H
