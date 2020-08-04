//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H

#include <memory>
#include <utility>

#include <normal/pushdown/join/TupleSetIndexWrapper.h>
#include <normal/pushdown/join/TupleSetIndexFinder.h>
#include <normal/pushdown/shuffle/ArrayAppender.h>
#include <normal/tuple/ColumnName.h>
#include <normal/tuple/TupleSet2.h>
#include "TupleSetIndexFinderWrapper.h"

using namespace normal::pushdown::shuffle;

namespace normal::pushdown::join {

class RecordBatchJoiner {
public:

  RecordBatchJoiner(std::shared_ptr<TupleSetIndex> buildTupleSetIndex,
					std::string probeJoinColumnName,
					std::shared_ptr<::arrow::Schema> outputSchema);

  static tl::expected<std::shared_ptr<RecordBatchJoiner>, std::string>
  make(const std::shared_ptr<TupleSetIndex> &buildTupleSetIndex,
	   const std::string &probeJoinColumnName,
	   const std::shared_ptr<::arrow::Schema> &outputSchema);

  tl::expected<void, std::string> join(const std::shared_ptr<::arrow::RecordBatch> &recordBatch);

  tl::expected<std::shared_ptr<TupleSet2>, std::string> toTupleSet();

private:
  std::shared_ptr<TupleSetIndex> buildTupleSetIndex_;
  std::string probeJoinColumnName_;
  std::shared_ptr<::arrow::Schema> outputSchema_;
  std::vector<std::shared_ptr<::arrow::Array>> joinedArrayVector_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H
