//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H

#include <memory>                                 // for unique_ptr
#include <string>                                 // for string
#include <vector>                                 // for vector

#include <normal/core/Operator.h>

#include "AggregateExpression.h"

class Message;

class Aggregate : public Operator {
private:
  std::vector<std::unique_ptr<AggregateExpression>> m_expressions;
public:
  Aggregate(std::string name, std::vector<std::unique_ptr<AggregateExpression>> m_expressions);
  ~Aggregate() override = default;
  void onStart() override;
  void onStop() override;
  void onReceive(std::unique_ptr<Message> msg) override;
};

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H
