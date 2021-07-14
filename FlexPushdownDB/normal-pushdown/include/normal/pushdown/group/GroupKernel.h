//
// Created by matt on 20/10/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKERNEL_H

#include <vector>
#include <string>
#include <memory>

#include <normal/tuple/TupleSet2.h>

#include <normal/pushdown/aggregate/AggregationFunction.h>
#include <normal/pushdown/group/GroupKey.h>

using namespace normal::pushdown::aggregate;
using namespace normal::tuple;
using namespace normal::expression;

namespace normal::pushdown::group {

class GroupKernel {

public:
  GroupKernel(std::vector<std::string> columnNames,
			  std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions);

  void onTuple(const TupleSet2 &tupleSet);

  std::shared_ptr<TupleSet2> group();

private:
  std::vector<std::string> columnNames_;
  std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions_;

  std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;
  std::optional<std::shared_ptr<Projector>> projector_;

  std::unordered_map<std::shared_ptr<GroupKey>,
					 std::shared_ptr<TupleSet2>,
					 GroupKeyPointerHash,
					 GroupKeyPointerPredicate> groupedTuples_;
  std::unordered_map<std::shared_ptr<GroupKey>,
					 std::vector<std::shared_ptr<AggregationResult>>,
					 GroupKeyPointerHash,
					 GroupKeyPointerPredicate> aggregateResults_;

  void cacheInputSchema(const TupleSet2 &tupleSet);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKERNEL_H
