//
// Created by Yifei Yang on 3/26/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER_H

#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/ArrayAppender.h>
#include <fpdb/tuple/ArrayHasher.h>
#include <arrow/api.h>
#include <tl/expected.hpp>

using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::shuffle {

/**
* Class to shuffle a record batch into N tuple sets.
*/
class RecordBatchShuffler {

public:
  RecordBatchShuffler(vector<int> shuffleColumnIndices,
                      size_t numSlots,
                      shared_ptr<::arrow::Schema> schema,
                      vector<vector<shared_ptr<ArrayAppender>>> shuffledAppendersVector);

  static tl::expected<shared_ptr<RecordBatchShuffler>, string> make(const vector<string> &columnNames,
                                                                    size_t numSlots,
                                                                    const shared_ptr<::arrow::Schema> &schema,
                                                                    size_t numRows);

  tl::expected<void, string> shuffle(const shared_ptr<::arrow::RecordBatch> &recordBatch);

  tl::expected<vector<shared_ptr<TupleSet>>, string> toTupleSets();

private:
  vector<int> shuffleColumnIndices_;
  size_t numSlots_;
  shared_ptr<::arrow::Schema> schema_;
  vector<vector<shared_ptr<ArrayAppender>>> shuffledAppendersVector_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER_H
