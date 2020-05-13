//
// Created by matt on 13/5/20.
//

#include <normal/pushdown/group/Group.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/expression/gandiva/Projector.h>

#include <utility>

using namespace normal::pushdown::group;

Group::Group(const std::string &Name,
			 std::vector<std::string> ColumnNames,
			 std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> AggregateFunctions) :
	Operator(Name, "Group"),
	columnNames_(std::move(ColumnNames)),
	aggregateFunctions_(std::move(AggregateFunctions)),
	aggregateResults_(){
}

std::shared_ptr<Group> Group::make(const std::string& Name,
								   const std::vector<std::string>& columnNames,
								   const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>>& AggregateFunctions) {

  std::vector<std::string> canonicalColumnNames;
  canonicalColumnNames.reserve(columnNames.size());
  for(const auto &columnName: columnNames){
	canonicalColumnNames.push_back(tuple::ColumnName::canonicalize(columnName));
  }

  return std::make_shared<Group>(Name, canonicalColumnNames, AggregateFunctions);
}

void Group::onReceive(const normal::core::message::Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void Group::onStart() {
  SPDLOG_DEBUG("Starting");

//  for (const auto &function: *aggregateFunctions_) {
//	if (function->buffer_ == nullptr) {
//	  auto result = std::make_shared<aggregate::AggregationResult>();
//	  aggregateResults_->emplace_back(result);
//	  function->init(result);
//	} else {
//	  function->buffer_->reset();
//	}
//  }
}

void Group::onTuple(const normal::core::message::TupleMessage &message) {
  auto tupleSet = normal::tuple::TupleSet2::create(message.tuples());

  // Set the input schema if not yet set
//  cacheInputSchema(*message.tuples());

  ::arrow::TableBatchReader batchReader(*tupleSet->getArrowTable().value());
  auto batch = batchReader.Next().ValueOrDie();
  while (batch->num_rows() > 0) {
    for(int r = 0; r < batch->num_rows();++r){

      // Build the group values tuple
      std::vector<std::shared_ptr<normal::tuple::Scalar>> groupValues;
	  for (const auto &columnName: columnNames_) {
	    auto column = batch->GetColumnByName(columnName);
		if(column->type()->id() == ::arrow::Int64Type::type_id) {
		  auto typedColumn = std::static_pointer_cast<::arrow::Int32Array>(column);
		  auto value = ::arrow::MakeScalar(typedColumn->Value(r));
		  auto scalar = std::make_shared<normal::tuple::Scalar>(value);
		  groupValues.push_back(scalar);
		}
		else{
		  throw std::runtime_error("Group for column of type '" + column->type()->ToString() + "' not implemented yet");
		}
	  }

	  // Get or initialise the aggregate result for the current group
	  std::shared_ptr<aggregate::AggregationResult> currentAggregate;
	  auto currentAggregateIt = aggregateResults_.find(groupValues);
	  if(currentAggregateIt == aggregateResults_.end()){
		auto y = ::arrow::MakeScalar(0);
		currentAggregate = std::make_shared<aggregate::AggregationResult>();
		currentAggregate->put("SUM", y);
		aggregateResults_.emplace(groupValues, currentAggregate);
	  }
	  else{
		currentAggregate = currentAggregateIt->second;
	  }

	  SPDLOG_DEBUG("Group");
	}
	batch = batchReader.Next().ValueOrDie();
  }
}

void Group::onComplete(const normal::core::message::CompleteMessage &msg) {

//  SPDLOG_DEBUG("Producer complete");
//
//  if (this->ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer)) {
//
//	SPDLOG_DEBUG("All producers complete, completing");
//
//	// Create output schema
//	std::shared_ptr<arrow::Schema> schema;
//	std::vector<std::shared_ptr<arrow::Field>> fields;
//	for (const auto &function: *aggregateFunctions_) {
//	  std::shared_ptr<arrow::Field> field = arrow::field(function->alias(), function->returnType());
//	  fields.emplace_back(field);
//	}
//	schema = arrow::schema(fields);
//
//	SPDLOG_DEBUG("Aggregation output schema: {}\n", schema->ToString());
//
////    arrow::MemoryPool *pool = arrow::default_memory_pool();
//
//	// Create output tuples
//	std::vector<std::shared_ptr<arrow::Array>> columns;
//	for (const auto &function: *aggregateFunctions_) {
//
//	  function->finalize();
//
//	  if (function->returnType() == arrow::float64()) {
//		auto scalar = std::static_pointer_cast<arrow::DoubleScalar>(function->buffer_->evaluate());
//		auto colArgh = makeArgh<arrow::DoubleType>(scalar);
//		columns.emplace_back(colArgh.value());
//	  } else if (function->returnType() == arrow::int32()) {
//		auto scalar = std::static_pointer_cast<arrow::Int32Scalar>(function->buffer_->evaluate());
//		auto colArgh = makeArgh<arrow::Int32Type>(scalar);
//		columns.emplace_back(colArgh.value());
//	  } else {
//		throw std::runtime_error("Unrecognized type " + function->returnType()->name());
//	  }
//	}
//
//	std::shared_ptr<arrow::Table> table;
//	table = arrow::Table::Make(schema, columns);
//
//	const std::shared_ptr<core::TupleSet> &aggregatedTuples = core::TupleSet::make(table);
//
//	SPDLOG_DEBUG("Completing  |  Aggregation result: \n{}", aggregatedTuples->toString());
//
//	std::shared_ptr<normal::core::message::Message>
//		tupleMessage = std::make_shared<normal::core::message::TupleMessage>(aggregatedTuples, this->name());
//	ctx()->tell(tupleMessage);
//
//	ctx()->notifyComplete();
//}

}
