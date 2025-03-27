//
// Created by Yifei Yang on 11/8/21.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_TABLE_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_TABLE_H

#include <fpdb/catalogue/CatalogueEntryType.h>
#include <fpdb/tuple/FileFormat.h>
#include <arrow/type.h>
#include <unordered_set>
#include <map>

using namespace std;

namespace fpdb::catalogue {

// To represent foreign keys
struct ColRef {
  unordered_map<string, uint> fKey_;    // columns in the foreign table, use position map for ease of lookup
  string pTable_;                       // primary table
  vector<string> pKey_;                 // columns in the primary table
  bool allRefReversed_;                 // whether this ColRef is reversely derived since all values of
                                        // original (unreversed) pk are referenced by original fk

  ColRef(const vector<string> &fKey, const string &pTable, const vector<string> &pKey, bool allRefReversed):
    pTable_(pTable), pKey_(pKey), allRefReversed_(allRefReversed) {
    if (fKey.size() != pKey.size()) {
      throw std::runtime_error("size of pk and fk mismatch when making 'ColRef'.");
    }
    for (uint i = 0; i < fKey.size(); ++i) {
      fKey_[fKey[i]] = i;
    }
  }
};

class Table {
public:
  Table(string name,
        const shared_ptr<arrow::Schema>& schema,
        const shared_ptr<fpdb::tuple::FileFormat>& format,
        const vector<ColRef>& colRefs,
        const unordered_map<string, int> &apxColumnLengthMap,
        int apxRowLength,
        const unordered_set<string> &zonemapColumnNames);
  Table() = default;
  Table(const Table&) = default;
  Table& operator=(const Table&) = default;
  virtual ~Table() = default;

  const string &getName() const;
  const shared_ptr<arrow::Schema> &getSchema() const;
  const shared_ptr<fpdb::tuple::FileFormat> &getFormat() const;
  const vector<ColRef> &getColRefs() const;
  vector<string> getColumnNames() const;
  int getApxColumnLength(const string &columnName) const;
  int getApxRowLength() const;

  virtual CatalogueEntryType getCatalogueEntryType() = 0;

protected:
  string name_;
  shared_ptr<arrow::Schema> schema_;
  shared_ptr<fpdb::tuple::FileFormat> format_;
  vector<ColRef> colRefs_;
  unordered_map<string, int> apxColumnLengthMap_;   // apx: approximate
  int apxRowLength_;
  unordered_set<string> zonemapColumnNames_;
};

}


#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_TABLE_H
