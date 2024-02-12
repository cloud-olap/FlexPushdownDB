//
// Created by Yifei Yang on 6/26/22.
//

#include <fpdb/executor/FPDBStoreExecution.h>

namespace fpdb::executor {

FPDBStoreExecution::FPDBStoreExecution(long queryId,
                                       const std::shared_ptr<::caf::actor_system> &actorSystem,
                                       const std::shared_ptr<PhysicalPlan> &physicalPlan,
                                       TableCallBack tableCallBack,
                                       BitmapCallBack bitmapCallBack):
  Execution(queryId, actorSystem, {}, nullptr, {}, physicalPlan, false),
  tableCallBack_(std::move(tableCallBack)),
  bitmapCallBack_(std::move(bitmapCallBack)) {}

void FPDBStoreExecution::join() {
  SPDLOG_DEBUG("Waiting for all operators to complete");

  auto handle_err = [&](const ::caf::error &err) {
    throw runtime_error(to_string(err));
  };

  bool allComplete = false;
  (*rootActor_)->receive_while([&] { return !allComplete; })(
          [&](const Envelope &e) {
            const auto &msg = e.message();
            SPDLOG_DEBUG("Query root actor received message  |  query: '{}', messageKind: '{}', from: '{}'",
                         queryId_, msg.getTypeString(), msg.sender());

            auto errAct = [&](const std::string &errMsg) {
              allComplete = true;
              close();
              throw runtime_error(errMsg);
            };

            switch (msg.type()) {
              case MessageType::COMPLETE: {
                this->opDirectory_.setComplete(msg.sender())
                        .map_error(errAct);
                allComplete = this->opDirectory_.allComplete();
                break;
              }

              case MessageType::BITMAP: {
                auto bitmapMessage = ((BitmapMessage &) msg);
                bitmapCallBack_(bitmapMessage.sender(), bitmapMessage.getBitmap());
                break;
              }

              case MessageType::TUPLESET_BUFFER: {
                auto tupleSetBufferMessage = ((TupleSetBufferMessage &) msg);
                tableCallBack_(tupleSetBufferMessage.getConsumer(), tupleSetBufferMessage.tuples()->table());
                break;
              }

#if SHOW_DEBUG_METRICS == true
              case MessageType::TRANSFER_METRICS: {
                auto transferMetricsMsg = ((TransferMetricsMessage &) msg);
                debugMetrics_.add(transferMetricsMsg.getTransferMetrics());
                break;
              }

              case MessageType::DISK_METRICS: {
                auto diskMetricsMsg = ((DiskMetricsMessage &) msg);
                debugMetrics_.add(diskMetricsMsg.getDiskMetrics());
                break;
              }
#endif

              case MessageType::ERROR: {
                errAct(fmt::format("ERROR: {}, from {}", ((ErrorMessage &) msg).getContent(), msg.sender()));
              }
              default: {
                errAct(fmt::format("Invalid message type sent to the root actor: {}, from {}", msg.getTypeString(), msg.sender()));
              }
            }

          },
          handle_err);

  stopTime_ = chrono::steady_clock::now();
}

}
