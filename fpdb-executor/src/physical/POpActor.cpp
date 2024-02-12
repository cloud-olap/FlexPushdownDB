//
// Created by Matt Youill on 31/12/19.
//

#include <fpdb/executor/physical/POpActor.h>
#include <fpdb/executor/message/StartMessage.h>
#include <fpdb/executor/message/ConnectMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/executor/flight/FlightClients.h>
#include <fpdb/store/server/flight/GetTableTicket.hpp>
#include <arrow/flight/api.h>
#include <spdlog/spdlog.h>
#include <utility>

namespace fpdb::executor::physical {

POpActor::POpActor(::caf::actor_config &cfg, std::shared_ptr<PhysicalOp> opBehaviour) :
	::caf::event_based_actor(cfg),
	opBehaviour_(std::move(opBehaviour)) {

  name_ = opBehaviour_->name();
}

::caf::behavior behaviour(POpActor *self) {

  auto ctx = self->operator_()->ctx();
  ctx->operatorActor(self);

  return {
	  [=](GetProcessingTimeAtom) {
		auto start = std::chrono::steady_clock::now();
		auto finish = std::chrono::steady_clock::now();
		auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
		self->incrementProcessingTime(elapsedTime);
		return self->getProcessingTime();
	  },
	  [=](const fpdb::executor::message::Envelope &msg) {

		auto start = std::chrono::steady_clock::now();

    SPDLOG_DEBUG("Message received  |  recipient: '{}', sender: '{}', type: '{}'",
                 self->operator_()->name(),
                 msg.message().sender(),
                 msg.message().type());

		if (msg.message().type() == MessageType::CONNECT) {
		  auto connectMessage = dynamic_cast<const message::ConnectMessage &>(msg.message());

		  for (const auto &element: connectMessage.connections()) {
        auto localEntry = LocalPOpDirectoryEntry(element.getName(),
                                element.getActorHandle(),
                                element.getConnectionType(),
                                element.getNodeId(),
                                false);

        auto result = self->operator_()->ctx()->operatorMap().insert(localEntry);
        if (!result.has_value()) {
          self->operator_()->ctx()->notifyError(result.error());
        }
		  }
		}
		else if (msg.message().type() == MessageType::START) {
		  auto startMessage = dynamic_cast<const message::StartMessage &>(msg.message());

		  self->running_ = true;

		  self->operator_()->onReceive(msg);

      while (!self->messageBuffer_.empty()){
        const auto &bufferedMsg = self->messageBuffer_.front();
        self->on_regular_message(bufferedMsg);
        self->messageBuffer_.pop();

        // if running_ turns to false, we should not continue processing rest messages in buffer
        if (!self->running_) {
          break;
        }
		  }

		} else if (msg.message().type() == MessageType::STOP) {
		  self->running_ = false;

		  self->operator_()->onReceive(msg);
		}
		else{
		  if (!self->running_){
			  self->messageBuffer_.emplace(msg);
		  }
		  else {
        self->on_regular_message(msg);
		  }
		}

		auto finish = std::chrono::steady_clock::now();
		auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
		self->incrementProcessingTime(elapsedTime);
	  }
  };
}

void POpActor::on_regular_message(const fpdb::executor::message::Envelope &msg) {
  if (msg.message().type() == MessageType::TUPLESET_READY_REMOTE
      && opBehaviour_->getType() != POpType::SHUFFLE_BATCH_LOAD) {    // ShuffleBatchLoadPOp handles this itself
    const auto &typedMessage = dynamic_cast<const TupleSetReadyRemoteMessage &>(msg.message());
    auto tupleSet = read_remote_table(typedMessage.getHost(), typedMessage.getPort(), typedMessage.sender());

    // metrics
#if SHOW_DEBUG_METRICS == true
    std::shared_ptr<Message> execMetricsMsg;
    if (typedMessage.isFromStore()) {
      execMetricsMsg = std::make_shared<TransferMetricsMessage>(metrics::TransferMetrics(tupleSet->size(), 0, 0),
                                                                opBehaviour_->name());
    } else {
      execMetricsMsg = std::make_shared<TransferMetricsMessage>(metrics::TransferMetrics(0, 0, tupleSet->size()),
                                                                opBehaviour_->name());
    }
    opBehaviour_->ctx()->notifyRoot(execMetricsMsg);
#endif

    std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSet, typedMessage.sender());
    on_regular_message(Envelope(tupleSetMessage));
    return;
  }

  if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const message::CompleteMessage &>(msg.message());
    auto result = opBehaviour_->ctx()->operatorMap().setComplete(msg.message().sender());
    if (!result.has_value()) {
      opBehaviour_->ctx()->notifyError(result.error());
    }
  }

  opBehaviour_->onReceive(msg);
}

std::shared_ptr<TupleSet>
POpActor::read_remote_table(const std::string &host, int port, const std::string &sender) {
  // make flight client and connect
  auto client = flight::GlobalFlightClients.getFlightClient(host, port);

  // send request to store
  auto ticketObj = fpdb::store::server::flight::GetTableTicket::make(opBehaviour_->getQueryId(),
                                                                     sender,
                                                                     opBehaviour_->name());
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    opBehaviour_->ctx()->notifyError(expTicket.error());
  }

  std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
  auto status = client->DoGet(*expTicket, &reader);
  if (!status.ok()) {
    opBehaviour_->ctx()->notifyError(status.message());
  }

  std::shared_ptr<::arrow::Table> table;
  status = reader->ReadAll(&table);
  if (!status.ok()) {
    opBehaviour_->ctx()->notifyError(status.message());
  }

  // return
  if (table == nullptr) {
    opBehaviour_->ctx()->notifyError(fmt::format("Received null table from remote node: {}", host));
  }

  // FIXME: arrow flight may produce unaligned buffers after transferring, which may crash if the table is used
  //  in GroupArrowKernel (at arrow::util::CheckAlignment).
  //  Here is a temp fix that recreates the table by a round of serialization and deserialization.
  if (opBehaviour_->name().substr(0, 5) == "Group") {
    table = ArrowSerializer::align_table_by_copy(table);
  }

  return TupleSet::make(table);
}

::caf::behavior POpActor::make_behavior() {
  return behaviour(this);
}

std::shared_ptr<fpdb::executor::physical::PhysicalOp> POpActor::operator_() const {
  return opBehaviour_;
}

long POpActor::getProcessingTime() const {
  return processingTime_;
}

void POpActor::incrementProcessingTime(long time) {
  processingTime_ += time;
}

void POpActor::on_exit() {
  SPDLOG_DEBUG("Stopping operator  |  name: '{}'", this->opBehaviour_->name());

  /*
   * Need to delete the actor handle in operator otherwise CAF will never release the actor
   */
  this->opBehaviour_->destroyActor();
}

}
