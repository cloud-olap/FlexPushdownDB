//
// Created by matt on 18/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORACTOR2_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORACTOR2_H

#include <utility>
#include <queue>

#include <caf/all.hpp>
#include <boost/callable_traits.hpp>

#include <normal/core/Globals.h>
#include <normal/core/LocalOperatorDirectory.h>
#include <normal/core/message/Envelope.h>
#include <normal/core/message/StartMessage.h>
#include <normal/core/OperatorConnection.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/OperatorActor.h>
#include <normal/core/message/ConnectMessage.h>

using namespace normal::core::message;
using namespace boost::callable_traits;

using ExpectedVoidString = tl::expected<void, std::string>;
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::vector<normal::core::OperatorConnection>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(ExpectedVoidString);

namespace normal::core {

using ConnectAtom = ::caf::atom_constant<caf::atom("connect")>;
using StartAtom = ::caf::atom_constant<caf::atom("start")>;
using StopAtom = ::caf::atom_constant<caf::atom("stop")>;
using CompleteAtom = ::caf::atom_constant<caf::atom("complete")>;

/**
 * Base operator actor definition
 */
using OperatorActor2 = ::caf::typed_actor<::caf::reacts_to<ConnectAtom, std::vector<OperatorConnection>>,
										  ::caf::reacts_to<StartAtom>,
										  ::caf::reacts_to<StopAtom>,
										  ::caf::reacts_to<CompleteAtom>,
										  ::caf::replies_to<GetProcessingTimeAtom>::with<long>,
										  ::caf::reacts_to<Envelope>>;

/**
 * Base operator actor state
 *
 * @tparam Actor
 */
template<class Actor>
class OperatorActorState {

public:
  virtual ~OperatorActorState() = default;

  std::string name = "<operator>";

protected:

  void setBaseState(Actor /* actor */,
					std::string name_,
					unsigned long queryId,
					::caf::actor rootActor,
					::caf::actor segmentCacheActorHandle) {
	name = std::move(name_);
	queryId_ = queryId;
	rootActorHandle_ = std::move(rootActor);
	segmentCacheActorHandle_ = std::move(segmentCacheActorHandle);
  }


  ///
  /// Process wrappers
  ///
  /// Capture processing time, and allow message sender override
  ///

  /**
   * Non void process wrapper
   *
   * @tparam F
   * @tparam R
   * @param actor
   * @param f
   * @return
   */
  template<typename F,
	  typename R = boost::callable_traits::return_type_t<F>,
	  typename = std::enable_if_t<!std::is_void_v<R>>>
  caf::result<R> process(Actor actor, const F &f) {

	auto processingStartTime_ = std::chrono::steady_clock::now();

	caf::strong_actor_ptr messageSender;
	if (overriddenMessageSender_)
	  messageSender = overriddenMessageSender_.value();
	else
	  messageSender = actor->current_sender();

	R r = f(messageSender);

	/*
	 * If return type is an (un)expected, raise an error
	 */
	if constexpr (tl::detail::is_expected<R>::value) {
	  if (!r) {
		auto error = caf::make_error(caf::sec::runtime_error, r.error());
		actor->call_error_handler(error);
	  }
	}

	auto processingStopTime_ = std::chrono::steady_clock::now();
	processingTime_ +=
		std::chrono::duration_cast<std::chrono::nanoseconds>(processingStopTime_ - processingStartTime_).count();

	return caf::make_result(r);
  }

  /**
   * Void process wrapper
   *
   * @tparam F
   * @tparam R
   * @param actor
   * @param f
   */
  template<typename F,
	  typename R = boost::callable_traits::return_type_t<F>,
	  typename = std::enable_if_t<std::is_void_v<R>>>
  void process(Actor actor, const F &f) {

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

  /**
   * Base hehaviour factory
   *
   * @tparam Handlers
   * @param actor
   * @param handlers
   * @return
   */
  template<class... Handlers>
  auto makeBaseBehavior(Actor actor, Handlers... handlers) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Spawning operator  |  queryId: {}", actor->id(),
				 actor->name(), queryId_.value());

	/**
	 * Handler for errors received from other actors
	 *
	 * Default behavior is to quit. See caf::scheduled_actor::default_error_handler
	 */
	actor->set_error_handler([=](const ::caf::error &error) {

	  SPDLOG_ERROR("[Actor {} ('{}')]  Operator error  |  queryId: {}, error: {}", actor->id(),
				   actor->name(), queryId_.value(), to_string(error));

	  onError(actor, error);

	  actor->quit(error);
	});

	/**
	 * Handler for unexpected messages received from other actors
	 *
	 * Default behavior is print_and_drop, which simply returns an unexpected_message
	 * error. See caf::scheduled_actor::print_and_drop
	 */
	actor->set_default_handler([=](caf::scheduled_actor * /*actor*/, ::caf::message_view &message) {

	  SPDLOG_ERROR("[Actor {} ('{}')]  Operator received unexpected message  |  queryId: {}, message: {}", actor->id(),
				   actor->name(), queryId_.value(), message.content().stringify());

	  onUnexpectedMessage(actor, message);

	  return caf::sec::unexpected_message;
	});

	/**
	 * Handler for unexpected exceptions thrown by actor
	 *
	 * Default behavior is to print the exception and return a runtime error.
	 * See error scheduled_actor::default_exception_handler
	 */
	actor->set_exception_handler([=](const std::exception_ptr &exceptionPointer) -> ::caf::error {

	  onException(actor, exceptionPointer);

	  std::string message;
	  try {
		if (exceptionPointer) {
		  std::rethrow_exception(exceptionPointer);
		}
	  } catch (const std::exception &exception) {
		message = fmt::format("[Actor {} ('{}')]  Operator caught unhandled exception  |  queryId: {}, cause: '{}'",
							  actor->id(),
							  actor->name(),
							  queryId_.value(), exception.what());
		SPDLOG_ERROR(message);
		spdlog::dump_backtrace();
	  }

	  return make_error(::caf::sec::runtime_error, message);
	});

	/**
	 * Handler for actor exit event
	 */
	actor->attach_functor([=](const caf::error &reason) {

	  SPDLOG_DEBUG("[Actor {} ('{}')]  Operator exit  |  queryId: {}, reason: {}", actor->id(),
				   actor->name(), queryId_.value(), to_string(reason));

	  onExit(actor, reason);
	});

	/**
	 * Handler for down messages received from monitored actors
	 *
	 * Default behavior is to print the message. See caf::scheduled_actor::default_down_handler
	 */
	actor->set_down_handler([=](const ::caf::down_msg &downMessage) {

	  SPDLOG_WARN("[Actor {} ('{}')]  Operator monitored actor down  |  queryId: {}, source: {}, reason: {}",
				  actor->id(),
				  actor->name(),
				  queryId_.value(),
				  to_string(downMessage.source),
				  to_string(downMessage.reason));

	  onMonitoredDown(actor, downMessage);
	});

	/**
	 * Handler for exit messages received from linked actors
	 *
	 * Default behavior is to print the message and quit if error is non
	 * zero. i.e. none, otherwise ignore. See caf::scheduled_actor::default_exit_handler
	 */
	actor->set_exit_handler([=](const ::caf::exit_msg &exitMessage) {

	  if (exitMessage.reason) {
		SPDLOG_WARN("[Actor {} ('{}')]  Operator linked actor exit  |  queryId: {}, source: {}, reason: {}",
					actor->id(),
					actor->name(),
					queryId_.value(),
					to_string(exitMessage.source),
					to_string(exitMessage.reason));
	  }

	  onLinkedExit(actor, exitMessage);

	  if (exitMessage.reason) {
		actor->quit(exitMessage.reason);
	  }
	});


	///
	/// Message handlers
	///
	return caf::make_typed_behavior(
		[=](StartAtom) {
		  process(actor, [=](const caf::strong_actor_ptr &messageSender) { return handleStart(actor, messageSender); });
		},
		[=](StopAtom) {
		  process(actor, [=](const caf::strong_actor_ptr &messageSender) { return handleStop(actor, messageSender); });
		},
		[=](ConnectAtom, const std::vector<OperatorConnection> &connections) {
		  process(actor,
				  [=](const caf::strong_actor_ptr &messageSender) {
					return handleConnect(actor,
										 messageSender,
										 connections);
				  });
		},
		[=](CompleteAtom) {
		  process(actor,
				  [=](const caf::strong_actor_ptr &messageSender) { return handleComplete(actor, messageSender); });
		},
		[=](GetProcessingTimeAtom) {
		  return process(actor,
						 [=](const caf::strong_actor_ptr &messageSender) {
						   return handleGetProcessingTime(actor,
														  messageSender);
						 });
		},
		// Legacy handler
		[=](const Envelope &envelope) {
		  process(actor,
				  [=](const caf::strong_actor_ptr &messageSender) {
					return handleEnvelope(actor,
										  messageSender,
										  envelope);
				  });
		},
		std::move(handlers)...
	);
  }


  ///
  /// Extension points
  ///

  virtual void onExit(Actor /*actor*/, const ::caf::error &/*reason*/) { /*NOOP*/ };
  virtual void onError(Actor /*actor*/, const ::caf::error &/*error*/) { /*NOOP*/ };
  virtual void onMonitoredDown(Actor /*actor*/, const ::caf::down_msg &/*downMessage*/) { /*NOOP*/ };
  virtual void onLinkedExit(Actor /*actor*/, const ::caf::exit_msg &/*exitMessage*/) { /*NOOP*/ };
  virtual void onException(Actor /*actor*/, const std::exception_ptr &/*exceptionPointer*/) { /*NOOP*/ };
  virtual void onUnexpectedMessage(Actor /*actor*/, const ::caf::message_view &/*message*/) { /*NOOP*/ };

  [[nodiscard]] virtual tl::expected<void, std::string>
  onStart(Actor /*actor*/,
		  const caf::strong_actor_ptr & /*messageSender*/) { return {}; };

  [[nodiscard]] virtual tl::expected<void, std::string>
  onStop(Actor /*actor*/,
		 const caf::strong_actor_ptr & /*messageSender*/) { return {}; };

  [[nodiscard]] virtual tl::expected<void, std::string>
  onConnect(Actor /*actor*/,
			const caf::strong_actor_ptr & /*messageSender*/) { return {}; };

  [[nodiscard]] virtual tl::expected<void, std::string>
  onComplete(Actor /*actor*/,
			 const caf::strong_actor_ptr & /*messageSender*/) { return {}; };

  [[nodiscard]] virtual tl::expected<void, std::string>
  onEnvelope(Actor /*actor*/,
			 const caf::strong_actor_ptr & /*messageSender*/,
			 const Envelope &/*envelope*/) { return {}; };

  template<typename... Parameters>
  void anonymousSend(Actor actor, caf::actor destination, Parameters...parameters) {
	actor->anon_send(destination, parameters...);
  }

  /**
   * Sends message to consumers. Fails if operator is not started, or complete.
   *
   * @tparam Parameters
   * @param actor
   * @param parameters
   * @return
   */
  template<typename... Parameters>
  [[nodiscard]] tl::expected<void, std::string> tell(Actor actor, Parameters...parameters) {

	if (!running_)
	  return tl::make_unexpected(fmt::format("Cannot tell message to consumers, operator {} ('{}') is not running",
											 actor->id(),
											 actor->name()));

	if (complete_)
	  return tl::make_unexpected(fmt::format("Cannot tell message to consumers, operator {} ('{}') is complete",
											 actor->id(),
											 actor->name()));

	for (const auto &operatorEntry: actor->state.operatorDirectory_) {
	  if (std::get<2>(operatorEntry.second) == OperatorRelationshipType::Consumer)
		anonymousSend(actor, std::get<0>(operatorEntry.second), parameters...);
	}

	return {};
  }

  /**
   * Sets the operator to complete. Fails if operator is not running, or complete.
   *
   * @param actor
   * @return
   */
  [[nodiscard]] tl::expected<void, std::string> notifyComplete(Actor actor) {

	if (!running_) {
	  return tl::make_unexpected(fmt::format("Cannot complete operator {} ('{}'), not running",
											 actor->id(),
											 actor->name()));
	}

	if (complete_) {
	  return tl::make_unexpected(fmt::format("Cannot complete operator {} ('{}'), already complete",
											 actor->id(),
											 actor->name()));
	}

	SPDLOG_INFO("[Actor {} ('{}')]  Completing operator  |  queryId: {}",
				actor->id(),
				actor->name(),
				queryId_.value());

//	tell(actor, CompleteAtom::value);
	auto tellResult = tell(actor, Envelope(std::make_shared<CompleteMessage>(name)));
	if (!tellResult)
	  return tellResult;
//	anonymousSend(actor, rootActorHandle_, CompleteAtom::value);
	anonymousSend(actor, rootActorHandle_.value(), Envelope(std::make_shared<CompleteMessage>(name)));

	complete_ = true;

	return {};
  }

  /**
   * @return True if all producers are complete
   */
  bool isAllProducersComplete() {
	assert(numCompleteProducers_ <= numProducers_);
	return numCompleteProducers_ == numProducers_;
  }

  /**
   * @return True if all consumers are complete
   */
  bool isAllConsumersComplete() {
	assert(numCompleteConsumers_ <= numConsumers_);
	return numCompleteConsumers_ == numConsumers_;
  }

  /**
   * @return SegmentCache actor handle
   */
  [[nodiscard]] const std::optional<caf::actor> &getSegmentCacheActorHandle() const {
	return segmentCacheActorHandle_;
  }

  [[nodiscard]] const std::optional<caf::actor> &getRootActorHandle() const {
	return rootActorHandle_;
  }

  [[nodiscard]] bool isRunning() const {
	return running_;
  }

  [[nodiscard]] bool isComplete() const {
	return complete_;
  }

  [[nodiscard]] std::optional<unsigned long> getQueryId() const {
	return queryId_;
  }

private:
  std::optional<unsigned long> queryId_;

  std::optional<caf::actor> rootActorHandle_;
  std::optional<caf::actor> segmentCacheActorHandle_;

  bool running_ = false;
  bool complete_ = false;

  std::optional<caf::strong_actor_ptr> overriddenMessageSender_;
  std::queue<std::pair<caf::message, caf::strong_actor_ptr>> buffer_;

  std::map<caf::actor_id, std::tuple<caf::actor, std::string, OperatorRelationshipType, bool>> operatorDirectory_;
  int numProducers_ = 0;
  int numConsumers_ = 0;
  int numCompleteProducers_ = 0;
  int numCompleteConsumers_ = 0;

  long processingTime_ = 0;

  ///
  /// Message handlers
  ///

  [[nodiscard]] tl::expected<void, std::string> handleStart(Actor actor, const caf::strong_actor_ptr &messageSender) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Starting operator  |  queryId: {}, source: {}", actor->id(),
				 actor->name(), queryId_.value(), to_string(messageSender));

	running_ = true;

	while (!actor->state.buffer_.empty()) {
	  auto item = buffer_.front();
	  overriddenMessageSender_ = item.second;
	  actor->call_handler(actor->current_behavior(), item.first);
	  buffer_.pop();
	}

	overriddenMessageSender_ = std::nullopt;

	return onStart(actor, messageSender);
  };

  [[nodiscard]] tl::expected<void, std::string> handleStop(Actor actor, const caf::strong_actor_ptr &messageSender) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Stopping operator  |  queryId: {}, source: {}", actor->id(),
				 actor->name(), queryId_.value(), to_string(messageSender));

	running_ = false;

	return onStop(actor, messageSender);
  };

  [[nodiscard]] tl::expected<void, std::string> handleConnect(Actor actor,
															  const caf::strong_actor_ptr &messageSender,
															  const std::vector<OperatorConnection> &connections) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Connecting operator  |  queryId: {}, source: {}", actor->id(),
				 actor->name(), queryId_.value(), to_string(messageSender));

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

	return onConnect(actor, messageSender);
  };

  [[nodiscard]] tl::expected<void, std::string> handleComplete(Actor actor,
															   const caf::strong_actor_ptr &messageSender) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Connected operator complete  |  queryId: {}, source: {}", actor->id(),
				 actor->name(), queryId_.value(), to_string(messageSender));

	auto maybeEntry = operatorDirectory_.find(actor->current_sender()->id());
	if (maybeEntry == operatorDirectory_.end()) {
	  return tl::make_unexpected(fmt::format("Complete message received from unexpected sender {}",
											 to_string(messageSender)));
	}
	std::get<3>(maybeEntry->second) = true;

	if (std::get<2>(maybeEntry->second) == OperatorRelationshipType::Producer)
	  ++numCompleteProducers_;
	if (std::get<2>(maybeEntry->second) == OperatorRelationshipType::Consumer)
	  ++numCompleteConsumers_;

	return onComplete(actor, messageSender);
  };

  long handleGetProcessingTime(Actor actor, const caf::strong_actor_ptr &messageSender) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Getting operator processing time  |  queryId: {}, source: {}", actor->id(),
				 actor->name(), queryId_.value(), to_string(messageSender));

	return actor->state.processingTime_;
  };

  /**
   * Legacy message handler for messages of type Envelope
   *
   * @param actor
   * @param envelope
   */
  [[nodiscard]] tl::expected<void, std::string>
  handleEnvelope(Actor actor, const caf::strong_actor_ptr &messageSender, const Envelope &envelope) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Operator received envelope  |  queryId: {}, source: {}, message type: {}",
				 actor->id(),
				 actor->name(),
				 queryId_.value(),
				 to_string(messageSender),
				 envelope.getMessage()->type());

	auto message = envelope.getMessage();
	if (message->type() == "StartMessage") {
	  return handleStart(actor, messageSender);
	} else if (message->type() == "StopMessage") {
	  return handleStop(actor, messageSender);
	} else if (message->type() == "ConnectMessage") {
	  return handleConnect(actor,
						   messageSender,
						   std::static_pointer_cast<ConnectMessage>(envelope.getMessage())->connections());
	} else {
	  if (!actor->state.running_) {
		actor->state.buffer_.emplace(actor->current_mailbox_element()->move_content_to_message(),
									 messageSender);
		return {};
	  } else {
		if (message->type() == "CompleteMessage") {
		  return handleComplete(actor, messageSender);
		} else {
		  return onEnvelope(actor, messageSender, envelope);
		}
	  }
	}
  }

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORACTOR2_H
