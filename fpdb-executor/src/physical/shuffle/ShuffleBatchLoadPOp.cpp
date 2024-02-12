//
// Created by Yifei Yang on 11/29/22.
//

#include <fpdb/executor/physical/shuffle/ShuffleBatchLoadPOp.h>
#include <fpdb/executor/flight/FlightClients.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/store/server/flight/GetBatchLoadInfoTicket.hpp>
#include <fpdb/store/server/flight/GetTableTicket.hpp>
#include <fpdb/store/server/flight/Util.hpp>
#include <fpdb/tuple/util/Util.h>

namespace fpdb::executor::physical::shuffle {

ShuffleBatchLoadPOp::ShuffleBatchLoadPOp(const std::string &name,
                                         const std::vector<std::string> &projectColumnNames,
                                         int nodeId):
  PhysicalOp(name, POpType::SHUFFLE_BATCH_LOAD, projectColumnNames, nodeId) {}

void ShuffleBatchLoadPOp::onReceive(const Envelope &envelope) {
  const auto &message = envelope.message();

  if (message.type() == MessageType::START) {
    this->onStart();
  } else if (message.type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message);
    this->onComplete(completeMessage);
  } else if (message.type() == MessageType::TUPLESET_READY_REMOTE) {
    auto tupleSetReadyRemoteMessage = dynamic_cast<const TupleSetReadyRemoteMessage &>(message);
    this->onTupleSetReadyRemote(tupleSetReadyRemoteMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.getTypeString());
  }
}

std::string ShuffleBatchLoadPOp::getTypeString() const {
  return "ShuffleBatchLoadPOp";
}

void ShuffleBatchLoadPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void ShuffleBatchLoadPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    ctx()->notifyComplete();
  }
}

void ShuffleBatchLoadPOp::onTupleSetReadyRemote(const TupleSetReadyRemoteMessage &msg) {
  // make flight client and connect
  auto client = flight::GlobalFlightClients.getFlightClient(msg.getHost(), msg.getPort());

  // first request to get lengths
  std::vector<std::string> consumers{consumers_.begin(), consumers_.end()};
  std::shared_ptr<fpdb::store::server::flight::TicketObject> ticketObj =
          fpdb::store::server::flight::GetBatchLoadInfoTicket::make(queryId_, msg.sender(), consumers, name_);
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    ctx()->notifyError(expTicket.error());
    return;
  }

  std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
  auto status = client->DoGet(*expTicket, &reader);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
    return;
  }

  arrow::RecordBatchVector recordBatches;
  status = reader->ReadAll(&recordBatches);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
    return;
  }

  // read into lengths
  if (recordBatches.size() != 1) {
    ctx()->notifyError(fmt::format("GetBatchLoadInfoTicket should only return one batch, but got '{}'",
                                   recordBatches.size()));
  }
  size_t numConsumers = consumers.size();
  std::vector<int64_t> lengths;
  lengths.resize(numConsumers);
  auto res = fpdb::store::server::flight::Util::readTableLengthBatch(recordBatches[0], &lengths);
  if (!res) {
    ctx()->notifyError(res.error());
    return;
  }

  // second request to get shuffled data
  ticketObj = fpdb::store::server::flight::GetTableTicket::make(queryId_, name_, "");
  expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    ctx()->notifyError(expTicket.error());
    return;
  }

  status = client->DoGet(*expTicket, &reader);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
    return;
  }

  std::shared_ptr<::arrow::Table> table;
  status = reader->ReadAll(&table);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }
  if (table == nullptr) {
    ctx()->notifyError(fmt::format("Received null table from remote node: {}", msg.getHost()));
  }
  // FIXME: arrow flight may produce unaligned buffers after transferring, which may crash if the table is used
  //  in GroupArrowKernel (at arrow::util::CheckAlignment).
  //  Here is a temp fix that recreates the table by a round of serialization and deserialization.
  if (consumers[0].substr(0, 5) == "Group") {
    table = ArrowSerializer::align_table_by_copy(table);
  }

  // metrics
#if SHOW_DEBUG_METRICS == true
  std::shared_ptr<Message> execMetricsMsg;
  if (msg.isFromStore()) {
    execMetricsMsg = std::make_shared<TransferMetricsMessage>(
            metrics::TransferMetrics(TupleSet::make(table)->size(), 0, 0), name_);
  } else {
    execMetricsMsg = std::make_shared<TransferMetricsMessage>(
            metrics::TransferMetrics(0, 0, TupleSet::make(table)->size()), name_);
  }
  ctx()->notifyRoot(execMetricsMsg);
#endif

  // split into shuffled pieces
  std::vector<arrow::ChunkedArrayVector> shuffledColumns{numConsumers};
  for (size_t i = 0; i < numConsumers; ++i) {
    shuffledColumns[i].resize(table->num_columns());
  }
  for (int c = 0; c < table->num_columns(); ++c) {
    const auto &col = table->column(c);
    int64_t offset = 0;
    for (size_t i = 0; i < numConsumers; ++i) {
      shuffledColumns[i][c] = col->Slice(offset, lengths[i]);
      offset += lengths[i];
    }
  }

  // send shuffled pieces to the corresponding consumer
  for (size_t i = 0; i < numConsumers; ++i) {
    std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(
            TupleSet::make(table->schema(), shuffledColumns[i]), name_);
    ctx()->send(tupleSetMessage, consumers[i]);
  }
}

void ShuffleBatchLoadPOp::clear() {
  // Noop
}

}
