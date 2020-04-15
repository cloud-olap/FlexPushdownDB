//
// Created by matt on 11/12/19.
//

#include "normal/pushdown/Aggregate.h"

#include <string>
#include <utility>
#include <memory>

#include <normal/core/message/CompleteMessage.h>
#include "normal/core/Operator.h"
#include "normal/core/message/TupleMessage.h"
#include "normal/core/message/Message.h"
#include <normal/pushdown/aggregate/AggregationResult.h>
#include "normal/pushdown/Globals.h"
#include "arrow/scalar.h"

namespace normal::pushdown {

Aggregate::Aggregate(std::string name,
                     std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> functions)
    : Operator(std::move(name), "Aggregate"),
      functions_(std::move(functions)),
      results_(std::make_shared<std::vector<std::shared_ptr<aggregate::AggregationResult>>>()) {}

void Aggregate::onStart() {
  SPDLOG_DEBUG("Starting");

  for (const auto &function: *functions_) {
    if(function->buffer_ == nullptr){
      auto result = std::make_shared<aggregate::AggregationResult>();
	  results_->emplace_back(result);
	  function->init(result);
    }
    else{
	  function->buffer_->reset();
    }
  }
}

void Aggregate::onReceive(const normal::core::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    throw;
  }
}

void Aggregate::onComplete(const normal::core::message::CompleteMessage &) {

  SPDLOG_DEBUG("Producer complete");

  if (this->ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer)) {

    SPDLOG_DEBUG("All producers complete, completing");

    // Create output schema
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto &function: *functions_) {
      std::shared_ptr<arrow::Field> field = arrow::field(function->alias(), function->returnType());
      fields.emplace_back(field);
    }
    schema = arrow::schema(fields);

    SPDLOG_DEBUG("Aggregation output schema: {}\n", schema->ToString());

//    arrow::MemoryPool *pool = arrow::default_memory_pool();

    // Create output tuples
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (const auto &function: *functions_) {

	  function->finalize();

      if(function->returnType() == arrow::float64()){
        auto scalar = std::static_pointer_cast<arrow::DoubleScalar>(function->buffer_->evaluate());
        auto colArgh = makeArgh<arrow::DoubleType>(scalar);
        columns.emplace_back(colArgh.value());
      }
	  else if(function->returnType() == arrow::int32()){
		auto scalar = std::static_pointer_cast<arrow::Int32Scalar>(function->buffer_->evaluate());
		auto colArgh = makeArgh<arrow::Int32Type>(scalar);
		columns.emplace_back(colArgh.value());
	  }
	  else{
	    throw std::runtime_error("Unrecognized type " + function->returnType()->name());
	  }
    }

    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, columns);

    const std::shared_ptr<core::TupleSet> &aggregatedTuples = core::TupleSet::make(table);

    SPDLOG_DEBUG("Completing  |  Aggregation result: \n{}", aggregatedTuples->toString());

    std::shared_ptr<normal::core::message::Message>
        tupleMessage = std::make_shared<normal::core::message::TupleMessage>(aggregatedTuples, this->name());
    ctx()->tell(tupleMessage);

    ctx()->notifyComplete();

    //  std::shared_ptr<normal::core::Message> cm = std::make_shared<normal::core::CompleteMessage>();
    //  ctx()->tell(cm);
    //
    //  ctx()->operatorActor()->quit();
  }
}

void Aggregate::onTuple(const core::message::TupleMessage &message) {
  SPDLOG_DEBUG("Received tuple message");
  compute(message.tuples());
}

void Aggregate::compute(const std::shared_ptr<normal::core::TupleSet> &tuples) {
  for (const auto &function: *functions_) {
	function->apply(tuples);
  }
}

}