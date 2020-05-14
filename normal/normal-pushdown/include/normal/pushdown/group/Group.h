//
// Created by matt on 13/5/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H

#include <normal/core/Operator.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>
#include <normal/tuple/Scalar.h>
#include <normal/tuple/TupleSet2.h>
#include "GroupKey.h"

namespace normal::pushdown::group {

/**
 * Group with aggregate operator
 *
 * This operator takes a list of column names to group by and a list of aggregate expressions to
 * compute for each group.
 *
 * A map is created from grouped values to computed aggregates. On the receipt of each tuple set
 * the aggregates are computed for each group and stored in the map.
 *
 * Upon completion of all upstream operators the final aggregates for each group are sent to downstream operators.
 */
class Group : public normal::core::Operator {

public:
  Group(const std::string &Name,
		std::vector<std::string> ColumnNames,
		std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> AggregateFunctions);

  static std::shared_ptr<Group> make(const std::string& Name,
									 const std::vector<std::string>& columnNames,
									 const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>>& AggregateFunctions);

  void onReceive(const core::message::Envelope &msg) override;

private:
  std::vector<std::string> columnNames_;
  std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> aggregateFunctions_;

  std::unordered_map<std::shared_ptr<GroupKey>, std::shared_ptr<normal::tuple::TupleSet2>, GroupKeyPointerHash, GroupKeyPointerPredicate> groupedTuples_;
  std::unordered_map<std::shared_ptr<GroupKey>, std::shared_ptr<aggregate::AggregationResult>, GroupKeyPointerHash, GroupKeyPointerPredicate> aggregateResults_;

  std::optional<std::shared_ptr<normal::expression::Projector>> projector_;
  std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;

  void onStart();
  void onTuple(const core::message::TupleMessage &msg);
  void onComplete(const core::message::CompleteMessage &msg);

  void cacheInputSchema(const normal::core::TupleSet &tuples);
  void buildAndCacheProjector();

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H
