//
// Created by Yifei Yang on 11/8/21.
//

#include <fpdb/catalogue/local-fs/LocalFSTable.h>

using namespace std;

namespace fpdb::catalogue::local_fs {

LocalFSTable::LocalFSTable(const string &name,
                           const shared_ptr<arrow::Schema> &schema,
                           const shared_ptr<fpdb::tuple::FileFormat> &format,
                           const unordered_map<string, int> &apxColumnLengthMap,
                           int apxRowLength,
                           const unordered_set<string> &zonemapColumnNames,
                           const vector<shared_ptr<LocalFSPartition>> &localFsPartitions) :
  Table(name, schema, format, apxColumnLengthMap, apxRowLength, zonemapColumnNames),
  localFSPartitions_(localFsPartitions) {}

CatalogueEntryType LocalFSTable::getCatalogueEntryType() {
  return LOCAL_FS;
}

}
