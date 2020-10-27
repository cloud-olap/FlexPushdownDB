//
// Created by matt on 13/5/20.
//

#include <normal/pushdown/group/Group.h>

using namespace normal::tuple;

namespace normal::pushdown::group {

Group::Group(const std::string &Name,
			 const std::vector<std::string> &ColumnNames,
			 const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
			 long queryId) :
	Operator(Name, "Group", queryId),
//	kernel_(std::make_unique<GroupKernel>(ColumnNames, *AggregateFunctions)),
			kernel2_(std::make_unique<GroupKernel2>(ColumnNames, *AggregateFunctions)) {
}

std::shared_ptr<Group> Group::make(const std::string &Name,
								   const std::vector<std::string> &columnNames,
								   const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions) {

  std::vector<std::string> canonicalColumnNames;
  canonicalColumnNames.reserve(columnNames.size());
  for (const auto &columnName: columnNames) {
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
}

void Group::onTuple(const normal::core::message::TupleMessage &message) {
  auto tupleSet = normal::tuple::TupleSet2::create(message.tuples());
//  kernel_->onTuple(*tupleSet);
  auto expectedGroupResult = kernel2_->group(*tupleSet);
  if(!expectedGroupResult)
    throw std::runtime_error(expectedGroupResult.error());
}

void Group::onComplete(const normal::core::message::CompleteMessage &) {
  if (!ctx()->isComplete() && this->ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer)) {
//	auto groupedTupleSet = kernel_->group();
	auto expectedGroupedTupleSet = kernel2_->finalise();
	if(!expectedGroupedTupleSet)
	  throw std::runtime_error(expectedGroupedTupleSet.error());
	std::shared_ptr<normal::core::message::Message>
		tupleMessage =
		std::make_shared<normal::core::message::TupleMessage>(expectedGroupedTupleSet.value()->toTupleSetV1(), this->name());
	ctx()->tell(tupleMessage);

	ctx()->notifyComplete();
  }
}

}
