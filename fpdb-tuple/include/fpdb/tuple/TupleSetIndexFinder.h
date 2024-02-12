//
// Created by matt on 3/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETINDEXFINDER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETINDEXFINDER_H

#include <fpdb/tuple/TupleSetIndex.h>
#include <fpdb/tuple/TupleKey.h>
#include <vector>
#include <cstdint>

using namespace std;

/**
 * Provides a find method allowing the caller to look up tupleKeys in the tupleSetIndex.
 */
namespace fpdb::tuple {

class TupleSetIndexFinder {

public:
  TupleSetIndexFinder(shared_ptr<TupleSetIndex> tupleSetIndex,
                      vector<int> probeColumnIndexes,
                      shared_ptr<arrow::RecordBatch> recordBatch);
  virtual ~TupleSetIndexFinder() = default;

  static tl::expected<shared_ptr<TupleSetIndexFinder>, string> make(const shared_ptr<TupleSetIndex> &tupleSetIndex,
                                                                    const vector<string> &probeColumnNames,
                                                                    const shared_ptr<arrow::RecordBatch> &recordBatch);

  tl::expected<vector<int64_t>, string> find(int64_t rowIndex);

private:
  shared_ptr<TupleSetIndex> tupleSetIndex_;
  vector<int> probeColumnIndexes_;
  shared_ptr<arrow::RecordBatch> recordBatch_;

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETINDEXFINDER_H
