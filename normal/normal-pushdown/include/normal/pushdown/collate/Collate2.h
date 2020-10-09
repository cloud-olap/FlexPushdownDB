//
// Created by matt on 9/10/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_COLLATE2_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_COLLATE2_H

#include <caf/all.hpp>

#include <normal/core/Forward.h>
#include <normal/core/OperatorActor2.h>
#include <normal/core/cache/SegmentCacheActor.h>

#include <normal/pushdown/Forward.h>
#include <normal/pushdown/TupleMessage.h>

using namespace normal::core;
using namespace normal::core::message;
using namespace normal::tuple;

using TupleSetPtr = std::shared_ptr<TupleSet>;
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(TupleSetPtr);

using ExpectedTupleSetPtrString = tl::expected<std::shared_ptr<TupleSet>, std::string>;
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(ExpectedTupleSetPtrString);

namespace normal::pushdown {

using TupleSetAtom = caf::atom_constant<caf::atom("tupleset")>;
using GetTupleSetAtom = caf::atom_constant<caf::atom("g-tupleset")>;

using CollateActor = OperatorActor2::extend_with<::caf::typed_actor<
	caf::reacts_to<TupleSetAtom, TupleSetPtr>,
	caf::replies_to<GetTupleSetAtom>::with<ExpectedTupleSetPtrString>>>;

using CollateStatefulActor = CollateActor::stateful_pointer<CollateState>;

class CollateState : public OperatorActorState<CollateStatefulActor> {
public:
  void setState(CollateStatefulActor actor,
				const char *name,
				long queryId,
				const caf::actor &rootActorHandle,
				const caf::actor &segmentCacheActorHandle) {

	OperatorActorState::setBaseState(actor, name, queryId, rootActorHandle, segmentCacheActorHandle);
  }

  template<class... Handlers>
  CollateActor::behavior_type makeBehavior(CollateStatefulActor actor, Handlers... handlers) {
	return OperatorActorState::makeBaseBehavior(
		actor,
		[=](TupleSetAtom, const TupleSetPtr &tupleSet) {
		  process(actor,
				  [=](const caf::strong_actor_ptr &messageSender) {
					return onTupleSet(actor, messageSender, tupleSet);
				  });
		},
		[=](GetTupleSetAtom) {
		  return process(actor,
						 [=](const caf::strong_actor_ptr &messageSender) {
						   return onGetTupleSet(actor, messageSender);
						 });
		},
		std::move(handlers)...
	);
  }

private:
  std::shared_ptr<TupleSet2> tupleSet_;

protected:

  tl::expected<void, std::string> onStart(CollateStatefulActor /*actor*/,
										  const strong_actor_ptr &/*messageSender*/) override {
	if (tupleSet_)
	  tupleSet_.reset();
	return {};
  }

  tl::expected<void, std::string> onComplete(CollateStatefulActor actor,
											 const caf::strong_actor_ptr & /*messageSender*/) override {
	if (!isComplete() && isAllProducersComplete()) {
	  return notifyComplete(actor);
	}
	return {};
  }

  tl::expected<void, std::string> onEnvelope(CollateStatefulActor actor,
											 const caf::strong_actor_ptr &messageSender,
											 const Envelope &envelope) override {
	if (envelope.message().type() == "TupleMessage") {
	  auto tupleMessage = dynamic_cast<const TupleMessage &>(envelope.message());
	  return this->onTupleSet(actor, messageSender, tupleMessage.tuples());
	} else {
	  return tl::make_unexpected(fmt::format("Unrecognized message type {}", envelope.message().type()));
	}
  }

private:

  [[nodiscard]] tl::expected<void, std::string> onTupleSet(CollateStatefulActor actor,
														   const caf::strong_actor_ptr &messageSender,
														   const TupleSetPtr &tupleSet) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Received tupleSet  |  sender: {}", actor->id(),
				 actor->name(), to_string(messageSender));

	if (!tupleSet_) {
	  tupleSet_ = TupleSet2::create(tupleSet);
	  return {};
	} else {
	  return tupleSet_->append(TupleSet2::create(tupleSet));
	}
  }

  [[nodiscard]] ExpectedTupleSetPtrString onGetTupleSet(CollateStatefulActor actor,
														const caf::strong_actor_ptr &messageSender) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Getting tupleSet  |  sender: {}", actor->id(),
				 actor->name(), to_string(messageSender));

	return tupleSet_->toTupleSetV1();
  }

};

CollateActor::behavior_type CollateFunctor(CollateStatefulActor actor,
										   const char *name,
										   long queryId,
										   const caf::actor &rootActorHandle,
										   const caf::actor &segmentCacheActorHandle);

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_COLLATE2_H
