//
// Created by matt on 3/8/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H

#include <fpdb/tuple/TupleSetIndex.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/ArrayAppender.h>
#include <fpdb/tuple/ColumnName.h>
#include <unordered_set>
#include <memory>
#include <utility>

using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::join {

class RecordBatchHashJoiner {
public:
  RecordBatchHashJoiner(shared_ptr<TupleSetIndex> buildTupleSetIndex,
                        vector<string> probeJoinColumnNames,
                        shared_ptr<::arrow::Schema> outputSchema,
                        vector<shared_ptr<pair<bool, int>>> neededColumnIndice,
                        int64_t buildRowOffset);

  static tl::expected<shared_ptr<RecordBatchHashJoiner>, string> make(
          const shared_ptr<TupleSetIndex> &buildTupleSetIndex,
          const vector<string> &probeJoinColumnNames,
          const shared_ptr<::arrow::Schema> &outputSchema,
          const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice,
          int64_t buildRowOffset);

  tl::expected<void, string> join(const shared_ptr<::arrow::RecordBatch> &probeRecordBatch,
                                  int64_t probeRowOffset);

  tl::expected<shared_ptr<TupleSet>, string> toTupleSet();

  const unordered_set<int64_t> &getBuildRowMatchIndexes() const;
  const unordered_set<int64_t> &getProbeRowMatchIndexes() const;

private:
  shared_ptr<TupleSetIndex> buildTupleSetIndex_;
  vector<string> probeJoinColumnNames_;
  shared_ptr<::arrow::Schema> outputSchema_;
  vector<shared_ptr<pair<bool, int>>> neededColumnIndice_;

  vector<::arrow::ArrayVector> joinedArrayVectors_;
  int64_t buildRowOffset_;
  unordered_set<int64_t> buildRowMatchIndexes_;
  unordered_set<int64_t> probeRowMatchIndexes_;

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H
