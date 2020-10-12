//
// Created by matt on 16/9/20.
//

#ifndef NORMAL_NORMAL_CORE_TEST_SAMPLEOPERATOR_H
#define NORMAL_NORMAL_CORE_TEST_SAMPLEOPERATOR_H

#include <caf/all.hpp>
#include <utility>

#include <normal/core/OperatorActor2.h>

using namespace caf;
using namespace normal::core;

using SampleActor = OperatorActor2::extend_with<::caf::typed_actor<reacts_to<add_atom>>>;

class SampleState : public OperatorActorState<SampleActor::stateful_pointer <SampleState>> {

public:

  void setState(SampleActor::stateful_pointer <SampleState> self, const char* name_){
	OperatorActorState::setBaseState(self, name_, 0, nullptr, nullptr);
  }

  template<class... Handlers>
  auto makeBehavior(SampleActor::stateful_pointer <SampleState> self, Handlers... handlers) {
	return OperatorActorState::makeBaseBehavior(
		self,
		[=](add_atom) {
		  SPDLOG_DEBUG("[Actor {} ('{}')]  Received add  |  sender: {}", self->id(),
					  self->name(), to_string(self->current_sender()));
		  tell(self, StopAtom::value);
		},
		std::move(handlers)...
	);
  }

protected:
tl::expected<void, std::string> onStart(SampleActor::stateful_pointer <SampleState> self, const caf::strong_actor_ptr &messageSender) override {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Start  |  sender: {}", self->id(),
				 self->name(), to_string(messageSender));
	tell(self, CompleteAtom::value);
	self->quit(caf::exit_reason::normal);
  }

tl::expected<void, std::string> onStop(SampleActor::stateful_pointer <SampleState> self, const caf::strong_actor_ptr &messageSender) override {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Stop  |  sender: {}", self->id(),
				 self->name(), to_string(messageSender));
  }

tl::expected<void, std::string> onComplete(SampleActor::stateful_pointer <SampleState> self, const caf::strong_actor_ptr &messageSender) override {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Complete  |  sender: {}", self->id(),
				 self->name(), to_string(messageSender));
	tell(self, CompleteAtom::value);
	self->quit(caf::exit_reason::normal);
  }

};

SampleActor::behavior_type SampleActorFunctor(SampleActor::stateful_pointer <SampleState> self, const char *name);

#endif //NORMAL_NORMAL_CORE_TEST_SAMPLEOPERATOR_H
