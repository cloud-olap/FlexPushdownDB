//
// Created by matt on 14/4/20.
//

#include "normal/pushdown/Project.h"

#include <utility>

#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/expression/Expressions.h>

#include "normal/pushdown/Globals.h"

namespace normal::pushdown {

Project::Project(const std::string &Name,
                 std::vector<std::shared_ptr<normal::core::expression::Expression>> Expressions)
    : Operator(Name, "Project"),
      expressions_(std::move(Expressions)) {}

void Project::onStart() {
  // NOOP
}

void Project::onReceive(const normal::core::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    // FIXME: Propagate error properly
    throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void Project::projectAndSendTuples() {
  auto projectedTuples = tuples_->evaluate(expressions_);
  if(projectedTuples) {
    sendTuples(projectedTuples.value());
  }
  else{
    // FIXME: Propagate error properly
    throw std::runtime_error(projectedTuples.error());
  }
}

void Project::onTuple(const core::message::TupleMessage &message) {

  // Add the tuples to the internal buffer
  bufferTuples(message);

  // Project and send if the buffer is full enough
  if (tuples_->numRows() > DEFAULT_BUFFER_SIZE) {
    projectAndSendTuples();
  } else {
    // NOOP
  }
}

void Project::sendTuples(std::shared_ptr<normal::core::TupleSet> &projected) {

  std::shared_ptr<core::message::Message>
      tupleMessage = std::make_shared<core::message::TupleMessage>(projected, name());
  ctx()->tell(tupleMessage);

  // FIXME: Either set tuples to size 0 or use an optional
  tuples_ = nullptr;
}

void Project::bufferTuples(const core::message::TupleMessage &message) {
  if (!tuples_) {
    // Initialise tuples buffer with message contents
    tuples_ = message.tuples();
  } else {
    // Append message contents to tuples buffer
    auto tables = {tuples_->table(), message.tuples()->table()};
    auto res = arrow::ConcatenateTables(tables);
    if (!res.ok()) {
      tuples_->table(*res);
    } else {
      // FIXME: Propagate error properly
      throw std::runtime_error(res.status().message());
    }
  }
}

void Project::onComplete(const normal::core::message::CompleteMessage &) {

  // Project and send any remaining tuples
  projectAndSendTuples();

  ctx()->notifyComplete();
}

}
