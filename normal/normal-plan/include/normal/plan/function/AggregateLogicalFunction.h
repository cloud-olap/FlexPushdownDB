//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALFUNCTION_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALFUNCTION_H

#include <string>
#include <memory>

#include <normal/expression/Expression.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>

namespace normal::plan::function {

class AggregateLogicalFunction {

public:
  explicit AggregateLogicalFunction(std::string type);
  virtual ~AggregateLogicalFunction() = default;

  std::shared_ptr<expression::gandiva::Expression> expression();
  void expression(const std::shared_ptr<expression::gandiva::Expression> &expression);

  virtual std::shared_ptr<pushdown::aggregate::AggregationFunction> toExecutorFunction() = 0;
  virtual std::shared_ptr<pushdown::aggregate::AggregationFunction> toExecutorReduceFunction() = 0;

  void setName(const std::string &name);

protected:
  std::string name_;

private:
  std::string type_;
  std::shared_ptr<expression::gandiva::Expression> expression_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALFUNCTION_H
