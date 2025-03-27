//
// Created by Yifei Yang on 4/5/24.
//

#include <fpdb/executor/physical/adaptive/AdaptSinkPOp.h>

namespace fpdb::executor::physical::adaptive {

AdaptSinkPOp::AdaptSinkPOp(const std::string &name,
                           const std::vector<std::string> &projectColumnNames,
                           int nodeId):
  PhysicalOp(name, ADAPT_SINK, projectColumnNames, nodeId) {}

std::string AdaptSinkPOp::getTypeString() const {
  return "AdaptSinkPOp";
}

void AdaptSinkPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleSetMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else if (msg.message().type() == MessageType::ADAPT_RESUME) {
    auto adaptResumeMessage = dynamic_cast<const AdaptResumeMessage &>(msg.message());
    this->onAdaptResume(adaptResumeMessage);
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void AdaptSinkPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void AdaptSinkPOp::onTupleSet(const TupleSetMessage &msg) {
  if (result_ == nullptr) {
    result_ = msg.tuples();
  } else {
    auto expConcatTupleSet = TupleSet::concatenate({result_, msg.tuples()});
    if (!expConcatTupleSet.has_value()) {
      ctx()->notifyError(expConcatTupleSet.error());
      return;
    }
    result_ = *expConcatTupleSet;
  }
}

void AdaptSinkPOp::onAdaptResume(const AdaptResumeMessage &msg) {
  // simply forward buffered results to consumers and then complete
  if (result_ == nullptr) {
    ctx()->notifyError("No input received");
    return;
  }
  auto tupleSetMessage = std::make_shared<TupleSetMessage>(result_, name_);
  ctx()->tell(tupleSetMessage);

  // if to preserve, then this sink will still be used in later stages
  // but still need to send complete msg to consumers to let them proceed
  if (msg.preserve()) {
    auto completeMessage = std::make_shared<CompleteMessage>(name_);
    ctx()->tell(completeMessage);
  } else {
    ctx()->notifyComplete();
  }
}

void AdaptSinkPOp::onComplete(const CompleteMessage &) {
  // noop since this will be the starting op of the next exec stage
}

void AdaptSinkPOp::clear() {
  result_.reset();
}
  
}
