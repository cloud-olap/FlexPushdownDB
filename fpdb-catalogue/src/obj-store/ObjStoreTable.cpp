//
// Created by Yifei Yang on 11/8/21.
//

#include <fpdb/catalogue/obj-store/ObjStoreTable.h>

namespace fpdb::catalogue::obj_store {

ObjStoreTable::ObjStoreTable(const string &name,
                             const shared_ptr<arrow::Schema> &schema,
                             const shared_ptr<fpdb::tuple::FileFormat> &format,
                             const unordered_map<string, int> &apxColumnLengthMap,
                             int apxRowLength,
                             const unordered_set<string> &zonemapColumnNames,
                             const vector<shared_ptr<ObjStorePartition>> &ObjStorePartitions) :
  Table(name, schema, format, apxColumnLengthMap, apxRowLength, zonemapColumnNames),
  ObjStorePartitions_(ObjStorePartitions) {}

const vector<shared_ptr<ObjStorePartition>> &ObjStoreTable::getObjStorePartitions() const {
  return ObjStorePartitions_;
}

CatalogueEntryType ObjStoreTable::getCatalogueEntryType() {
  return OBJ_STORE;
}

}
