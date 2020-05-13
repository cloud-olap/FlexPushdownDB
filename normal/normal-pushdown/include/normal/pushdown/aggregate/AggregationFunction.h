//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_AGGREGATEEXPRESSION2_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_AGGREGATEEXPRESSION2_H

#include <memory>

#include <normal/core/TupleSet.h>

#include <normal/pushdown/aggregate/AggregationResult.h>

namespace normal::pushdown::aggregate {

/**
 * Base class for aggregation functions
 */
class AggregationFunction {

protected:

  /**
   * The expression projector, created and cached when input schema is extracted from first tuple received
   */
  std::optional<std::shared_ptr<normal::expression::Projector>> projector_;

  /**
   * The schema of received tuples, sometimes cannot be known up front (e.g. when input source is a CSV file, the
   * columns aren't known until the file is read) so needs to be extracted from the first batch of tuples received
   */
  std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;

public:
  explicit AggregationFunction(std::string alias);
  virtual ~AggregationFunction() = default;

  /**
   * Alias is the symbolic name of the attribute, it's not guaranteed to be unique so shouldn't be used for anything
   * important. Ostensibly just for labelling attributes in query output.
   *
   * FIXME: Support this being undefined, perhaps it should be an Optional?
   *
   * @return
   */
  [[nodiscard]] const std::string &alias() const;

  virtual void apply(std::shared_ptr<aggregate::AggregationResult> result, std::shared_ptr<normal::core::TupleSet> tuples) = 0;
  virtual std::shared_ptr<arrow::DataType> returnType() = 0;

  /**
   * Invoked when an aggregate function should expect no more data to give it an opportunity to
   * compute its final result.
   */
  virtual void finalize(std::shared_ptr<aggregate::AggregationResult> result) = 0;

private:
  std::string alias_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_AGGREGATEEXPRESSION2_H
