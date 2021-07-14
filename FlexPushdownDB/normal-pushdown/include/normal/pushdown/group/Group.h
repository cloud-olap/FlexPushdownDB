//
// Created by matt on 13/5/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H

#include <string>
#include <vector>
#include <memory>

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>

#include <normal/core/message/TupleMessage.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>
#include <normal/pushdown/group/GroupKernel.h>
#include <normal/pushdown/group/GroupKernel2.h>

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
		const std::vector<std::string> &ColumnNames,
    const std::vector<std::string> &AggregateColumnNames,
		const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
		long queryId = 0);

  static std::shared_ptr<Group> make(const std::string &Name,
									 const std::vector<std::string> &groupColumnNames,
                   const std::vector<std::string> &aggregateColumnNames,
									 const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
									 long queryId);

  void onReceive(const core::message::Envelope &msg) override;

private:

  std::unique_ptr<GroupKernel2> kernel2_;

  void onStart();
  void onTuple(const core::message::TupleMessage &msg);
  void onComplete(const core::message::CompleteMessage &msg);

  size_t bytesGrouped_ = 0;
  long groupTime_ = 0;
  long numRows_ = 0;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H
