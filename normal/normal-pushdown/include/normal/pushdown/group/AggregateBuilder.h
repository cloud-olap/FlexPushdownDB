//
// Created by matt on 27/10/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_AGGREGATEBUILDER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_AGGREGATEBUILDER_H

#include <memory>

#include <normal/pushdown/aggregate/AggregationResult.h>

namespace normal::pushdown::aggregate {

class AggregateBuilder {
public:
  virtual ~AggregateBuilder() = default;

  virtual tl::expected<void, std::string> append(const std::shared_ptr<AggregationResult> &result) = 0;
  virtual tl::expected<std::shared_ptr<arrow::Array>, std::string> finalise() = 0;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_AGGREGATEBUILDER_H
