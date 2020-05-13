//
// Created by matt on 13/5/20.
//

#include <normal/pushdown/group/Group.h>

using namespace normal::pushdown::group;

Group::Group(const std::string &Name,
			 const std::vector<std::string> &ColumnNames,
			 const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions) :
	Operator(Name, "Group"),
	columnNames_(ColumnNames),
	aggregateFunctions_(AggregateFunctions) {}

std::shared_ptr<Group> Group::make(std::string Name,
								   std::vector<std::string> columnNames,
								   std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> AggregateFunctions) {
  return std::make_shared<Group>(Name, columnNames, AggregateFunctions);
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

}

void Group::onTuple(const normal::core::message::TupleMessage &msg) {

}

void Group::onComplete(const normal::core::message::CompleteMessage &msg) {

}

