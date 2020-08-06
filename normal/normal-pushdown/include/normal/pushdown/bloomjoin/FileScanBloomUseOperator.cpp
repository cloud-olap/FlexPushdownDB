//
// Created by matt on 5/8/20.
//

#include "FileScanBloomUseOperator.h"

#include <utility>

using namespace normal::pushdown::bloomjoin;

FileScanBloomUseOperator::FileScanBloomUseOperator(const std::string &name, std::string columnName) :
	Operator(name, "BloomJoinUseOperator"),
	columnName_(std::move(columnName)), kernel_(std::__cxx11::string(),
												std::vector<std::string>(),
												0,
												0,
												std::__cxx11::string()) {
}

void FileScanBloomUseOperator::onReceive(const Envelope &msg) {
  // FIXME: Really need to get rid of these if type == string tests... Urgh
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void FileScanBloomUseOperator::onStart() {

}

void FileScanBloomUseOperator::onTuple(const TupleMessage &msg) {

}

void FileScanBloomUseOperator::onComplete(const CompleteMessage &msg) {

}
