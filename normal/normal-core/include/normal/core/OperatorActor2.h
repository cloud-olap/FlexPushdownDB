//
// Created by matt on 18/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORACTOR2_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORACTOR2_H

#include <caf/all.hpp>
#include <utility>

#include <normal/core/Globals.h>
#include <normal/core/LocalOperatorDirectory.h>
#include <normal/core/message/Envelope.h>
#include <normal/core/message/StartMessage.h>
#include <normal/core/OperatorConnection.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/OperatorActor.h>
#include <queue>
#include <normal/core/message/ConnectMessage.h>

using namespace normal::core::message;

namespace normal::core {

using ConnectAtom = ::caf::atom_constant<caf::atom("connect")>;
using StartAtom = ::caf::atom_constant<caf::atom("start")>;
using StopAtom = ::caf::atom_constant<caf::atom("stop")>;
using CompleteAtom = ::caf::atom_constant<caf::atom("complete")>;

using OperatorActor2 = ::caf::typed_actor<::caf::reacts_to<ConnectAtom, std::vector<OperatorConnection>>,
										  ::caf::reacts_to<StartAtom>,
										  ::caf::reacts_to<StopAtom>,
										  ::caf::reacts_to<CompleteAtom>,
										  ::caf::replies_to<GetProcessingTimeAtom>::with<long>,
										  ::caf::reacts_to<Envelope>>;

template<class Actor>
class OperatorActorState {

public:
  virtual ~OperatorActorState() = default;

  const char *name = "<operator>";

protected:

  void setBaseState(Actor /* actor */,
					const char *name_,
					::caf::actor rootActor,
					::caf::actor segmentCacheActorHandle) {
	name = name_;
	rootActor_ = std::move(rootActor);
	segmentCacheActorHandle_ = std::move(segmentCacheActorHandle);
  }


  ///
  /// Process wrappers
  ///

  template<typename R>
  caf::result<R> processResult(Actor actor, const std::function<R(caf::strong_actor_ptr)> &f) {
	auto processingStartTime_ = std::chrono::steady_clock::now();

	caf::strong_actor_ptr messageSender;
	if (overriddenMessageSender_)
	  messageSender = overriddenMessageSender_.value();
	else
	  messageSender = actor->current_sender();

	R r = f(messageSender);
	auto processingStopTime_ = std::chrono::steady_clock::now();
	processingTime_ +=
		std::chrono::duration_cast<std::chrono::nanoseconds>(processingStopTime_ - processingStartTime_).count();
	return r;
  }

  void process(Actor actor, const std::function<void(caf::strong_actor_ptr)> &f) {
	auto processingStartTime_ = std::chrono::steady_clock::now();

	caf::strong_actor_ptr messageSender;
	if (overriddenMessageSender_)
	  messageSender = overriddenMessageSender_.value();
	else
	  messageSender = actor->current_sender();

	f(messageSender);
	auto processingStopTime_ = std::chrono::steady_clock::now();
	processingTime_ +=
		std::chrono::duration_cast<std::chrono::nanoseconds>(processingStopTime_ - processingStartTime_).count();
  }


  ///
  /// Behaviour factory
  ///

  template<class... Handlers>
  auto makeBaseBehavior(Actor actor, Handlers... handlers) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Spawning Actor", actor->id(),
				 actor->name());

	actor->set_error_handler([=](const ::caf::error &error) {
	  SPDLOG_ERROR("[Actor {} ('{}')]  Actor Error  |  error: {}", actor->id(),
				   actor->name(), to_string(error));
	  onError(actor, error);
	});

	actor->set_default_handler([=](::caf::scheduled_actor * /*actor*/, ::caf::message_view &message) {
	  SPDLOG_WARN("[Actor {} ('{}')]  Unexpected Message  |  message: {}", actor->id(),
				  actor->name(), to_string(message.copy_content_to_message()));
	  onUnexpectedMessage(actor, message);
	  // Default behavior is to quit
	  actor->quit(caf::sec::unexpected_message);
	  return message.move_content_to_message();
	});

	actor->set_exception_handler([=](const std::exception_ptr &exceptionPointer) -> ::caf::error {
	  onException(actor, exceptionPointer);
	  std::string msg;
	  try {
		if (exceptionPointer) {
		  std::rethrow_exception(exceptionPointer);
		}
	  } catch (const std::exception &exception) {
		msg = fmt::format("[Actor {} ('{}')]  Unhandled Exception  |  cause: '{}'",
						  actor->id(),
						  actor->name(),
						  exception.what());
		SPDLOG_ERROR(msg);
		spdlog::dump_backtrace();
	  }

	  return make_error(::caf::sec::runtime_error, msg);
	});

	actor->attach_functor([=](const caf::error &reason) {
	  onExit(actor, reason);
	  SPDLOG_DEBUG("[Actor {} ('{}')]  Actor Exit  |  reason: {}", actor->id(),
				   actor->name(), to_string(reason));
	});

	actor->set_down_handler([=](const ::caf::down_msg &downMessage) {
	  onDown(actor, downMessage);
	  SPDLOG_DEBUG("[Actor {} ('{}')]  Monitored Actor Down  |  source: {}, reason: {}", actor->id(),
				   actor->name(), to_string(downMessage.source), to_string(downMessage.reason));

	});

	actor->set_exit_handler([=](const ::caf::exit_msg &exitMessage) {
	  onLinkedExit(actor, exitMessage);
	  if (exitMessage.reason == ::caf::exit_reason::normal) {
		SPDLOG_DEBUG("[Actor {} ('{}')]  Linked Actor Exit  |  source: {}, reason: {}", actor->id(),
					 actor->name(), to_string(exitMessage.source), to_string(exitMessage.reason));
	  } else {
		SPDLOG_WARN("[Actor {} ('{}')]  Linked Actor Exit  |  source: {}, reason: {}", actor->id(),
					actor->name(), to_string(exitMessage.source), to_string(exitMessage.reason));
		actor->quit(exitMessage.reason);
	  }
	});

	return caf::make_typed_behavior(
		[=](StartAtom) {
		  process(actor, [=](auto messageSender) { handleStart(actor, messageSender); });
		},
		[=](StopAtom) {
		  process(actor, [=](auto messageSender) { handleStop(actor, messageSender); });
		},
		[=](ConnectAtom, const std::vector<OperatorConnection> &connections) {
		  process(actor, [=](auto messageSender) { handleConnect(actor, messageSender, connections); });
		},
		[=](CompleteAtom) {
		  process(actor, [=](auto messageSender) { handleComplete(actor, messageSender); });
		},
		[=](GetProcessingTimeAtom) -> caf::result<long> {
		  return processResult<long>(actor,
									 [=](auto messageSender) { return handleGetProcessingTime(actor, messageSender); });
		},
		// Legacy handler
		[=](const Envelope &envelope) {
		  process(actor, [=](auto messageSender) { handleEnvelope(actor, messageSender, envelope); });
		},
		std::move(handlers)...
	);
  }


  ///
  /// Message handlers
  ///

  void handleStart(Actor actor, const caf::strong_actor_ptr &messageSender) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Start  |  source: {}", actor->id(),
				 actor->name(), to_string(messageSender));

	running_ = true;

	while (!actor->state.buffer_.empty()) {
	  auto item = buffer_.front();
	  overriddenMessageSender_ = item.second;
	  actor->call_handler(actor->current_behavior(), item.first);
	  buffer_.pop();
	}

	overriddenMessageSender_ = std::nullopt;

	onStart(actor, messageSender);
  };

  void handleStop(Actor actor, const caf::strong_actor_ptr &messageSender) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Stop  |  source: {}", actor->id(),
				 actor->name(), to_string(actor->current_sender()));

	running_ = false;

	onStop(actor, messageSender);
  };

  void handleConnect(Actor actor,
					 const caf::strong_actor_ptr &messageSender,
					 const std::vector<OperatorConnection> &connections) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Connect  |  source: {}", actor->id(),
				 actor->name(), to_string(messageSender));

	for (const auto &connection: connections) {
	  actor->state.operatorDirectory_.emplace(connection.getActorHandle().id(),
											  std::tuple(connection.getActorHandle(),
														 connection.getName(),
														 connection.getConnectionType(),
														 false));

	  if (connection.getConnectionType() == OperatorRelationshipType::Producer)
		++numProducers_;
	  if (connection.getConnectionType() == OperatorRelationshipType::Consumer)
		++numConsumers_;
	}

	onConnect(actor, messageSender);
  };

  void handleComplete(Actor actor, const caf::strong_actor_ptr &messageSender) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Complete  |  source: {}", actor->id(),
				 actor->name(), to_string(messageSender));

	auto maybeEntry = operatorDirectory_.find(actor->current_sender()->id());
	if (maybeEntry == operatorDirectory_.end()) {
	  throw std::runtime_error(fmt::format("Complete message received from unexpected sender  |  sender: {}",
										   to_string(messageSender)));
	}
	std::get<3>(maybeEntry->second) = true;

	if (std::get<2>(maybeEntry->second) == OperatorRelationshipType::Producer)
	  ++numCompleteProducers_;
	if (std::get<2>(maybeEntry->second) == OperatorRelationshipType::Consumer)
	  ++numCompleteConsumers_;

	onComplete(actor, messageSender);
  };

  long handleGetProcessingTime(Actor actor, const caf::strong_actor_ptr &messageSender) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  GetProcessingTime  |  source: {}", actor->id(),
				 actor->name(), to_string(messageSender));

	return actor->state.processingTime_;
  };

  /**
   * Legacy message handler for messages of type Envelope
   *
   * @param actor
   * @param envelope
   */
  void handleEnvelope(Actor actor, const caf::strong_actor_ptr &messageSender, const Envelope &envelope) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Envelope  |  source: {}, message: {}", actor->id(),
				 actor->name(), to_string(messageSender), envelope.getMessage()->type());

	auto message = envelope.getMessage();
	if (message->type() == "StartMessage") {
	  handleStart(actor, messageSender);
	} else if (message->type() == "StopMessage") {
	  handleStop(actor, messageSender);
	} else if (message->type() == "ConnectMessage") {
	  handleConnect(actor, messageSender, std::static_pointer_cast<ConnectMessage>(envelope.getMessage())->connections());
	} else {
	  if (!actor->state.running_) {
		actor->state.buffer_.emplace(actor->current_mailbox_element()->move_content_to_message(),
									 messageSender);
	  } else {
		if (message->type() == "CompleteMessage") {
		  handleComplete(actor, messageSender);
		} else {
		  onEnvelope(actor, messageSender, envelope);
		}
	  }
	}
  }


  ///
  /// Extension points
  ///

  virtual void onExit(Actor /*actor*/, const ::caf::error &/*reason*/) { /*NOOP*/ };
  virtual void onError(Actor /*actor*/, const ::caf::error &/*error*/) { /*NOOP*/ };
  virtual void onDown(Actor /*actor*/, const ::caf::down_msg &/*downMessage*/) { /*NOOP*/ };
  virtual void onLinkedExit(Actor /*actor*/, const ::caf::exit_msg &/*exitMessage*/) { /*NOOP*/ };
  virtual void onException(Actor /*actor*/, const std::exception_ptr &/*exceptionPointer*/) { /*NOOP*/ };
  virtual void onUnexpectedMessage(Actor /*actor*/, const ::caf::message_view &/*message*/) { /*NOOP*/ };

  virtual void onStart(Actor /*actor*/, const caf::strong_actor_ptr & /*messageSender*/) { /*NOOP*/ };
  virtual void onStop(Actor /*actor*/, const caf::strong_actor_ptr & /*messageSender*/) { /*NOOP*/ };
  virtual void onConnect(Actor /*actor*/, const caf::strong_actor_ptr & /*messageSender*/) { /*NOOP*/ };
  virtual void onComplete(Actor /*actor*/, const caf::strong_actor_ptr & /*messageSender*/) { /*NOOP*/ };
  virtual void onEnvelope(Actor /*actor*/,
						  const caf::strong_actor_ptr & /*messageSender*/,
						  const Envelope &/*envelope*/) { /*NOOP*/ };

  template<typename... Parameters>
  void anonymousSend(Actor actor, caf::actor destination, Parameters...parameters) {
	actor->anon_send(destination, parameters...);
  }

  template<typename... Parameters>
  void tell(Actor actor, Parameters...parameters) {
	for (const auto &operatorEntry: actor->state.operatorDirectory_) {
	  if (std::get<2>(operatorEntry.second) == OperatorRelationshipType::Consumer)
		anonymousSend(actor, std::get<0>(operatorEntry.second), parameters...);
	}
  }

  void notifyComplete(Actor actor) {
//	tell(actor, CompleteAtom::value);
	tell(actor, Envelope(std::make_shared<CompleteMessage>(name)));
//	anonymousSend(actor, rootActor_, CompleteAtom::value);
	anonymousSend(actor, rootActor_, Envelope(std::make_shared<CompleteMessage>(name)));
  }

  bool isAllProducersComplete() {
	assert(numCompleteProducers_ <= numProducers_);
	return numCompleteProducers_ == numProducers_;
  }

  bool isAllConsumersComplete() {
	assert(numCompleteConsumers_ <= numConsumers_);
	return numCompleteConsumers_ == numConsumers_;
  }

  [[nodiscard]] const caf::actor &getSegmentCacheActorHandle() const {
	return segmentCacheActorHandle_;
  }

private:
  caf::actor rootActor_;
  caf::actor segmentCacheActorHandle_;

  bool running_ = false;

  std::optional<caf::strong_actor_ptr> overriddenMessageSender_;
  std::queue<std::pair<caf::message, caf::strong_actor_ptr>> buffer_;

  std::map<caf::actor_id, std::tuple<caf::actor, std::string, OperatorRelationshipType, bool>> operatorDirectory_;
  int numProducers_ = 0;
  int numConsumers_ = 0;
  int numCompleteProducers_ = 0;
  int numCompleteConsumers_ = 0;

  long processingTime_ = 0;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::vector<normal::core::OperatorConnection>);

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORACTOR2_H
