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
      // exclude network time here
      return self->getProcessingTime() - self->getNetworkTime();
    },
    [=](GetNetworkTimeAtom) {
      return self->getNetworkTime();
    },
	  [=](const fpdb::executor::message::Envelope &msg) {

		auto start = std::chrono::steady_clock::now();

    SPDLOG_DEBUG("Message received  |  recipient: '{}', sender: '{}', type: '{}'",
                 self->operator_()->name(),
                 msg.message().sender(),
                 msg.message().type());

		if (msg.message().type() == MessageType::CONNECT) {
		  auto connectMessage = dynamic_cast<const message::ConnectMessage &>(msg.message());

                  // clear old connections if needed
                  if (connectMessage.clear()) {
                    self->operator_()->clearConnections();
                    self->operator_()->ctx()->operatorMap().clear();
                    // need to also set "producers_" and "consumers_" since by default only localOpEntry is added
                    std::set<std::string> producers, consumers;
                    for (const auto &element: connectMessage.connections()) {
                      if (element.getConnectionType() == POpRelationshipType::Producer) {
                        producers.emplace(element.getName());
                      } else {
                        consumers.emplace(element.getName());
                      }
                    }
                    self->operator_()->setProducers(producers);
                    self->operator_()->setConsumers(consumers);
                  }

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
  if (msg.message().type() == MessageType::TUPLESET_READY_REMOTE &&
      opBehaviour_->getType() != POpType::FPDB_STORE_SHUFFLE_BATCH_LOAD &&  // ShuffleBatchLoadPOp handles this itself
      opBehaviour_->getType() != POpType::BATCH_EXCHANGE_RR_FORWARD) {      // BatchExchangeRRForwardPOp handles this itself
    auto start = std::chrono::steady_clock::now();

    const auto &typedMessage = dynamic_cast<const TupleSetReadyRemoteMessage &>(msg.message());
    auto tupleSet = read_remote_table(typedMessage.getHost(), typedMessage.getPort(),
                                      typedMessage.sender(), typedMessage.getOriginalConsumer());

    // metrics
#if SHOW_DEBUG_METRICS == true
    std::shared_ptr<Message> execMetricsMsg;
    int64_t tupleSetSize = tupleSet->size();
    if (typedMessage.isFromStore()) {
      execMetricsMsg = std::make_shared<NetworkMetricsMessage>(metrics::NetworkMetrics(tupleSetSize, 0, 0),
                                                               opBehaviour_->name());
    } else {
      execMetricsMsg = std::make_shared<NetworkMetricsMessage>(metrics::NetworkMetrics(0, 0, tupleSetSize),
                                                               opBehaviour_->name());
    }
    opBehaviour_->ctx()->notifyRoot(execMetricsMsg);
#endif

    auto finish = std::chrono::steady_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
    networkTime_ += elapsedTime;

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
POpActor::read_remote_table(const std::string &host, int port, const std::string &sender,
                            const std::optional<std::string> &originalConsumer) {
  // make flight client and connect
  auto client = flight::GlobalFlightClients.getFlightClient(host, port);

  // use originalConsumer if it has value
  auto consumer = originalConsumer.has_value() ? *originalConsumer : opBehaviour_->name();

  // send request to store
  auto ticketObj = fpdb::store::server::flight::GetTableTicket::make(opBehaviour_->getQueryId(), sender, consumer);
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    opBehaviour_->ctx()->notifyError(expTicket.error());
  }

  auto expReader = client->DoGet(*expTicket);
  if (!expReader.ok()) {
    opBehaviour_->ctx()->notifyError(expReader.status().message());
  }

  auto expTable = (*expReader)->ToTable();
  if (!expTable.ok()) {
    opBehaviour_->ctx()->notifyError(expTable.status().message());
  }
  auto table = *expTable;

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

long POpActor::getNetworkTime() const {
  return networkTime_;
}

void POpActor::incrementProcessingTime(long time) {
  processingTime_ += time;
}

void POpActor::incrementNetworkTime(long time) {
  networkTime_ += time;
}

void POpActor::on_exit() {
  SPDLOG_DEBUG("Stopping operator  |  name: '{}'", this->opBehaviour_->name());

  /*
   * Need to delete the actor handle in operator otherwise CAF will never release the actor
   */
  this->opBehaviour_->destroyActor();
}

}
