//
// Created by Yifei Yang on 10/6/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreTableCacheLoadPOp.h>
#include <fpdb/executor/flight/FlightClients.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/store/server/flight/GetTableTicket.hpp>
#include <fpdb/store/server/flight/Util.hpp>
#include <arrow/flight/api.h>

namespace fpdb::executor::physical::fpdb_store {

FPDBStoreTableCacheLoadPOp::FPDBStoreTableCacheLoadPOp(const std::string &name,
                                                       const std::vector<std::string> &projectColumnNames,
                                                       int nodeId):
  PhysicalOp(name, POpType::FPDB_STORE_TABLE_CACHE_LOAD, projectColumnNames, nodeId) {}

void FPDBStoreTableCacheLoadPOp::onReceive(const Envelope &envelope) {
  const auto &message = envelope.message();

  if (message.type() == MessageType::START) {
    this->onStart();
  } else if (message.type() == MessageType::TUPLESET_WAIT_REMOTE) {
    auto tupleSetWaitRemoteMessage = dynamic_cast<const TupleSetWaitRemoteMessage &>(message);
    this->onTupleSetWaitRemote(tupleSetWaitRemoteMessage);
  } else if (message.type() == MessageType::COMPLETE) {
    // noop
  } else {
    ctx()->notifyError("Unrecognized message type " + message.getTypeString());
  }
}

std::string FPDBStoreTableCacheLoadPOp::getTypeString() const {
  return "FPDBStoreTableCacheLoadPOp";
}

void FPDBStoreTableCacheLoadPOp::consume(const std::shared_ptr<PhysicalOp> &op) {
  if (!producers_.empty()) {
    throw std::runtime_error("FPDBStoreTableCacheLoadPOp should only have one producer");
  }
  PhysicalOp::consume(op);
}

const std::string &FPDBStoreTableCacheLoadPOp::getProducer() const {
  if (producers_.empty()) {
    throw std::runtime_error("FPDBStoreTableCacheLoadPOp has no producer");
  }
  return *producers_.begin();
}

void FPDBStoreTableCacheLoadPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void FPDBStoreTableCacheLoadPOp::onTupleSetWaitRemote(const TupleSetWaitRemoteMessage &msg) {
  // make flight client and connect
  auto client = flight::GlobalFlightClients.getFlightClient(msg.getHost(), msg.getPort());

  // make request
  auto ticketObj = fpdb::store::server::flight::GetTableTicket::make(queryId_, msg.sender(), name_, true);
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    ctx()->notifyError(expTicket.error());
    return;
  }
  auto ticket = *expTicket;

  // load table until receive the "end table"
  while (true) {
    std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
    auto status = client->DoGet(ticket, &reader);
    if (!status.ok()) {
      ctx()->notifyError(status.message());
      return;
    }
    std::shared_ptr<::arrow::Table> table;
    status = reader->ReadAll(&table);
    if (!status.ok()) {
      ctx()->notifyError(status.message());
      return;
    }

    // check table
    if (table == nullptr) {
      ctx()->notifyError(fmt::format("Received null table from remote node: {}", msg.getHost()));
      return;
    }
    if (fpdb::store::server::flight::Util::isEndTable(table)) {
      ctx()->notifyComplete();
      return;
    } else {
      auto tupleSet = TupleSet::make(table);

      // metrics
#if SHOW_DEBUG_METRICS == true
      std::shared_ptr<Message> execMetricsMsg = std::make_shared<TransferMetricsMessage>(
              metrics::TransferMetrics(tupleSet->size(), 0, 0), name_);
      ctx()->notifyRoot(execMetricsMsg);
#endif

      std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSet, name_);
      ctx()->tell(tupleSetMessage);
    }
  }
}

void FPDBStoreTableCacheLoadPOp::clear() {
  // Noop
}

}
