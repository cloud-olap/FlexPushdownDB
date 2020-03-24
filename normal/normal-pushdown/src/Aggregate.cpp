//
// Created by matt on 11/12/19.
//

#include "normal/pushdown/Aggregate.h"

#include <string>
#include <utility>
#include <memory>

#include <normal/core/CompleteMessage.h>
#include "normal/core/Operator.h"
#include "normal/core/TupleMessage.h"
#include "normal/core/Message.h"
#include <normal/pushdown/aggregate/AggregationResult.h>
#include "normal/pushdown/Globals.h"

namespace normal::pushdown {

Aggregate::Aggregate(std::string name,
                     std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> functions)
    : Operator(std::move(name)),
      functions_(std::move(functions)),
      result_(std::make_shared<aggregate::AggregationResult>()) {}

void Aggregate::onStart() {
  SPDLOG_DEBUG("Starting");

  for (const auto& expression: *functions_) {
    expression->init(this->result_);
  }
}

void Aggregate::onReceive(const normal::core::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::core::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::core::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    throw;
  }
}

void Aggregate::onComplete(const normal::core::CompleteMessage &message) {

  SPDLOG_DEBUG("Completing");

  std::shared_ptr<arrow::Schema> schema;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto& expression: *functions_) {
    std::shared_ptr<arrow::Field> field = arrow::field(expression->columnName(), arrow::utf8());
    fields.emplace_back(field);
  }
  schema = arrow::schema(fields);

  SPDLOG_DEBUG("Aggregation output schema: {}\n", schema->ToString());

  arrow::MemoryPool *pool = arrow::default_memory_pool();

  std::vector<std::shared_ptr<arrow::Array>> columns;
  for (const auto& expression: *functions_) {
    arrow::StringBuilder colBuilder(pool);
    colBuilder.Append(this->result_->get(expression->columnName()));
    std::shared_ptr<arrow::StringArray> col;
    colBuilder.Finish(&col);
    columns.emplace_back(col);
  }

  std::shared_ptr<arrow::Table> table;
  table = arrow::Table::Make(schema, columns);

  const std::shared_ptr<core::TupleSet> &aggregatedTuples = core::TupleSet::make(table);

  SPDLOG_DEBUG("Completing  |  Aggregation result: \n{}",  aggregatedTuples->toString());

  std::shared_ptr<normal::core::Message> tupleMessage = std::make_shared<normal::core::TupleMessage>(aggregatedTuples, this->name());
  ctx()->tell(tupleMessage);

  ctx()->notifyComplete();

//  std::shared_ptr<normal::core::Message> cm = std::make_shared<normal::core::CompleteMessage>();
//  ctx()->tell(cm);
//
//  ctx()->operatorActor()->quit();
}

void Aggregate::onTuple(const core::TupleMessage& message) {
  SPDLOG_DEBUG("Received tuple message");
  compute(message.tuples());
}

void Aggregate::compute(const std::shared_ptr<normal::core::TupleSet>& tuples) {
  for (const auto& expression: *functions_) {
    expression->apply(tuples);
  }
}

}