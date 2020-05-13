//
// Created by matt on 13/5/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H

#include <normal/core/Operator.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>

namespace normal::pushdown::group {

class Group : public normal::core::Operator {

public:
  Group(const std::string &Name,
		const std::vector<std::string> &ColumnNames,
		const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions);

  static std::shared_ptr<Group> make(std::string Name,
									 std::vector<std::string> columnNames,
									 std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> AggregateFunctions);

  void onReceive(const core::message::Envelope &msg) override;

private:
  std::vector<std::string> columnNames_;
  std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> aggregateFunctions_;

  void onStart();
  void onTuple(const core::message::TupleMessage &msg);
  void onComplete(const core::message::CompleteMessage &msg);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUP_H
