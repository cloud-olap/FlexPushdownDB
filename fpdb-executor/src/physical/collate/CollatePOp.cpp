//
// Created by matt on 5/12/19.
//

#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/store/server/flight/Util.hpp>
#include <arrow/table.h>               // for ConcatenateTables, Table (ptr ...
#include <arrow/pretty_print.h>
#include <vector>                      // for vector

namespace fpdb::executor::physical::collate {

CollatePOp::CollatePOp(std::string name,
                       std::vector<std::string> projectColumnNames,
                       int nodeId) :
  PhysicalOp(std::move(name), COLLATE, std::move(projectColumnNames), nodeId) {
}

std::string CollatePOp::getTypeString() const {
  return "CollatePOp";
}

void CollatePOp::onReceive(const fpdb::executor::message::Envelope &message) {
  if (message.message().type() == MessageType::START) {
    this->onStart();
  } else if (message.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const fpdb::executor::message::TupleSetMessage &>(message.message());
    this->onTupleSet(tupleSetMessage);
  } else if (message.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const fpdb::executor::message::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
  }
}

bool CollatePOp::isForward() const {
  return forward_;
}

const std::unordered_map<std::string, std::string> &CollatePOp::getForwardConsumers() const {
  return forwardConsumers_;
}

const std::vector<std::string> &CollatePOp::getEndConsumers() const {
  return endConsumers_;
}

void CollatePOp::setForward(bool forward) {
  forward_ = forward;
}

void CollatePOp::setForwardConsumers(const std::unordered_map<std::string, std::string> &forwardConsumers) {
  forwardConsumers_ = forwardConsumers;
}

void CollatePOp::setEndConsumers(const std::vector<std::string> &endConsumers) {
  endConsumers_ = endConsumers;
}

void CollatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void CollatePOp::onComplete(const fpdb::executor::message::CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    if (!tables_.empty()) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok()) {
        ctx()->notifyError(res.status().message());
      }
      tuples_->table(*res);
      tables_.clear();
    }

    // if "forward_" is enabled, then send "end table" for "forwardConsumers_" to the root actor,
    // denoting the pipeline of tables is ended
    if (forward_) {
      auto expEndTable = fpdb::store::server::flight::Util::getEndTable();
      if (!expEndTable.has_value()) {
        ctx()->notifyError(expEndTable.error());
        return;
      }
      for (const auto &endConsumer: endConsumers_) {
        std::shared_ptr<Message> tupleSetBufferMessage = std::make_shared<TupleSetBufferMessage>(
                TupleSet::make(*expEndTable), endConsumer, name_);
        ctx()->notifyRoot(tupleSetBufferMessage);
      }
    }

    // make the order of output columns the same as the query specifies
    if (tuples_ && tuples_->valid()) {
      const auto &expTupleSet = tuples_->projectExist(getProjectColumnNames());
      if (!expTupleSet.has_value()) {
        ctx()->notifyError(expTupleSet.error());
      }
      tuples_ = expTupleSet.value();
    } else {
      tuples_ = TupleSet::makeWithEmptyTable();
    }

	  ctx()->notifyComplete();
  }
}

void CollatePOp::onTupleSet(const fpdb::executor::message::TupleSetMessage &message) {
  if (forward_) {
    onTupleSetForward(message);
  } else {
    onTupleSetRegular(message);
  }
}

void CollatePOp::onTupleSetForward(const fpdb::executor::message::TupleSetMessage &message) {
  auto consumerIt = forwardConsumers_.find(message.sender());
  if (consumerIt == forwardConsumers_.end()) {
    ctx()->notifyError(fmt::format("Sender '{}' not found in forward consumers", message.sender()));
    return;
  }
  std::shared_ptr<Message> tupleSetBufferMessage = std::make_shared<TupleSetBufferMessage>(
          message.tuples(), consumerIt->second, name_);
  ctx()->notifyRoot(tupleSetBufferMessage);
}

void CollatePOp::onTupleSetRegular(const fpdb::executor::message::TupleSetMessage &message) {
  if (!tuples_) {
    assert(message.tuples());
    tuples_ = message.tuples();
  } else {
    tables_.push_back(message.tuples()->table());
    if (tables_.size() > tablesCutoff_) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok()) {
        ctx()->notifyError(res.status().message());
      }
      tuples_->table(*res);
      tables_.clear();
    }
  }
}

std::shared_ptr<TupleSet> CollatePOp::tuples() {
  return tuples_;
}

void CollatePOp::clear() {
  tuples_.reset();
  tables_.clear();
}

}