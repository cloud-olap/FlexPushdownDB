//
// Created by Yifei Yang on 4/17/24.
//

#include <fpdb/executor/physical/project/TupleSetSizeProjectPOp.h>

namespace fpdb::executor::physical::project {

TupleSetSizeProjectPOp::TupleSetSizeProjectPOp(const string &name,
                                               const vector<string> &projectColumnNames,
                                               int nodeId):
  PhysicalOp(name, TUPLESET_SIZE_PROJECT, projectColumnNames, nodeId) {}

std::string TupleSetSizeProjectPOp::getTypeString() const {
  return "TupleSetSizeProjectPOp";
}

void TupleSetSizeProjectPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleSetMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void TupleSetSizeProjectPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void TupleSetSizeProjectPOp::onTupleSet(const TupleSetMessage &msg) {
  numRows_ += msg.tuples()->numRows();
}

void TupleSetSizeProjectPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    auto tupleSetSizeMessage = std::make_shared<TupleSetSizeMessage>(numRows_, name_);
    ctx()->tell(tupleSetSizeMessage);
    ctx()->notifyComplete();
  }
}

void TupleSetSizeProjectPOp::clear() {
  // noop
}

}
