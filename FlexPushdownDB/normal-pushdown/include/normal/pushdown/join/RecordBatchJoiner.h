//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H

#include <memory>
#include <utility>

#include <normal/tuple/TupleSetIndexWrapper.h>
#include <normal/tuple/TupleSetIndexFinder.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ColumnName.h>
#include <normal/tuple/TupleSet2.h>
#include <set>
#include "normal/tuple/TupleSetIndexFinderWrapper.h"

using namespace normal::tuple;

namespace normal::pushdown::join {

class RecordBatchJoiner {
public:

  RecordBatchJoiner(std::shared_ptr<TupleSetIndex> buildTupleSetIndex,
					std::string probeJoinColumnName,
					std::shared_ptr<::arrow::Schema> outputSchema,
          std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice);

  static tl::expected<std::shared_ptr<RecordBatchJoiner>, std::string>
  make(const std::shared_ptr<TupleSetIndex> &buildTupleSetIndex,
	   const std::string &probeJoinColumnName,
	   const std::shared_ptr<::arrow::Schema> &outputSchema,
	   const std::vector<std::shared_ptr<std::pair<bool, int>>> &neededColumnIndice);

  tl::expected<void, std::string> join(const std::shared_ptr<::arrow::RecordBatch> &recordBatch);

  tl::expected<std::shared_ptr<TupleSet2>, std::string> toTupleSet();

private:
  std::shared_ptr<TupleSetIndex> buildTupleSetIndex_;
  std::string probeJoinColumnName_;
  std::shared_ptr<::arrow::Schema> outputSchema_;
  std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice_;
  std::vector<::arrow::ArrayVector> joinedArrayVectors_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H
