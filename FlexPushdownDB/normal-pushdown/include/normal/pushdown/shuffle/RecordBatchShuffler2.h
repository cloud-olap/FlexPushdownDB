//
// Created by Yifei Yang on 3/26/21.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_RecordBatchShuffler2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_RecordBatchShuffler2_H

#include <arrow/api.h>
#include <tl/expected.hpp>

#include <normal/tuple/TupleSet2.h>
#include <normal/tuple/ArrayAppender.h>

using namespace normal::tuple;

namespace normal::pushdown::shuffle {

/**
* Class to shuffle a record batch into N tuple sets.
*/
class RecordBatchShuffler2 {

public:
  RecordBatchShuffler2(int shuffleColumnIndex, size_t numSlots, std::shared_ptr<::arrow::Schema> schema, size_t numRows);

  static tl::expected<std::shared_ptr<RecordBatchShuffler2>, std::string>
  make(const std::string &columnName, size_t numSlots, const std::shared_ptr<::arrow::Schema> &schema, size_t numRows);

  [[nodiscard]] tl::expected<void, std::string> shuffle(const std::shared_ptr<::arrow::RecordBatch> &recordBatch);

  tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string> toTupleSets();

protected:
  int shuffleColumnIndex_;
  size_t numSlots_;
  std::shared_ptr<::arrow::Schema> schema_;
  std::vector<std::vector<std::shared_ptr<ArrayAppender>>> shuffledAppendersVector_;
};

}


#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_RecordBatchShuffler2_H
