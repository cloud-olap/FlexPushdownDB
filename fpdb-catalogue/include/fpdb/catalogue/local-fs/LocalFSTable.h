//
// Created by Yifei Yang on 11/8/21.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_LOCAL_FS_LOCALFSTABLE_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_LOCAL_FS_LOCALFSTABLE_H

#include <fpdb/catalogue/Table.h>
#include <fpdb/catalogue/local-fs/LocalFSPartition.h>
#include <vector>

using namespace std;

namespace fpdb::catalogue::local_fs {

class LocalFSTable: public Table {

public:
  LocalFSTable(const string &name,
               const shared_ptr<arrow::Schema> &schema,
               const shared_ptr<fpdb::tuple::FileFormat> &format,
               const unordered_map<string, int> &apxColumnLengthMap,
               int apxRowLength,
               const unordered_set<string> &zonemapColumnNames,
               const vector<shared_ptr<LocalFSPartition>> &localFsPartitions);
  LocalFSTable() = default;
  LocalFSTable(const LocalFSTable&) = default;
  LocalFSTable& operator=(const LocalFSTable&) = default;

  CatalogueEntryType getCatalogueEntryType() override;

private:
  vector<shared_ptr<LocalFSPartition>> localFSPartitions_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LocalFSTable& table) {
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
                                  f.field("localFSPartitions", table.localFSPartitions_));
  }
};

}


#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_LOCAL_FS_LOCALFSTABLE_H
