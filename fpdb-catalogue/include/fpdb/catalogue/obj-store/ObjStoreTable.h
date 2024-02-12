//
// Created by Yifei Yang on 11/8/21.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORETABLE_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORETABLE_H

#include <fpdb/catalogue/Table.h>
#include <fpdb/catalogue/obj-store/ObjStorePartition.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <vector>

using namespace std;

namespace fpdb::catalogue::obj_store {

class ObjStoreTable: public Table {
public:
  ObjStoreTable(const string &name,
          const shared_ptr<arrow::Schema> &schema,
          const shared_ptr<fpdb::tuple::FileFormat> &format,
          const unordered_map<string, int> &apxColumnLengthMap,
          int apxRowLength,
          const unordered_set<string> &zonemapColumnNames,
          const vector<shared_ptr<ObjStorePartition>> &ObjStorePartitions);
  ObjStoreTable() = default;
  ObjStoreTable(const ObjStoreTable&) = default;
  ObjStoreTable& operator=(const ObjStoreTable&) = default;

  const vector<shared_ptr<ObjStorePartition>> &getObjStorePartitions() const;

  CatalogueEntryType getCatalogueEntryType() override;

private:
  vector<shared_ptr<ObjStorePartition>> ObjStorePartitions_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ObjStoreTable& table) {
    auto schemaToBytes = [&table]() -> decltype(auto) {
      return fpdb::tuple::ArrowSerializer::schema_to_bytes(table.schema_);
    };
    auto schemaFromBytes = [&table](const std::vector<std::uint8_t> &bytes) {
      table.schema_ = ArrowSerializer::bytes_to_schema(bytes);
      return true;
    };
    return f.object(table).fields(f.field("name", table.name_),
                                  f.field("schema", schemaToBytes, schemaFromBytes),
                                  f.field("format", table.format_),
                                  f.field("apxColumnLengthMap", table.apxColumnLengthMap_),
                                  f.field("apxRowLength", table.apxRowLength_),
                                  f.field("zonemapColumnNames", table.zonemapColumnNames_),
                                  f.field("ObjStorePartitions", table.ObjStorePartitions_));
  }
};

}


#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORETABLE_H
