//
// Created by matt on 9/10/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_COLLATE_COLLATEPOP2_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_COLLATE_COLLATEPOP2_H

#include <fpdb/executor/physical/Forward.h>
#include <fpdb/executor/physical/POpActor2.h>
#include <fpdb/executor/cache/SegmentCacheActor.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::executor::physical;
using namespace fpdb::executor::message;
using namespace fpdb::tuple;

using TupleSetPtr = std::shared_ptr<TupleSet>;

using ExpectedTupleSetPtrString = tl::expected<std::shared_ptr<TupleSet>, std::string>;
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(ExpectedTupleSetPtrString)

CAF_BEGIN_TYPE_ID_BLOCK(CollatePOp2, fpdb::caf::CAFUtil::CollatePOp2_first_custom_type_id)
CAF_ADD_ATOM(CollatePOp2, TupleSetAtom)
CAF_ADD_ATOM(CollatePOp2, GetTupleSetAtom)
CAF_ADD_TYPE_ID(CollatePOp2, (TupleSetPtr))
CAF_ADD_TYPE_ID(CollatePOp2, (ExpectedTupleSetPtrString))
CAF_END_TYPE_ID_BLOCK(CollatePOp2)

namespace fpdb::executor::physical::collate {

using CollateActor = POpActor2::extend_with<::caf::typed_actor<
        ::caf::reacts_to<TupleSetAtom, TupleSetPtr>,
        ::caf::replies_to<GetTupleSetAtom>::with<ExpectedTupleSetPtrString>>>;

using CollateStatefulActor = CollateActor::stateful_pointer<CollateState>;

class [[maybe_unused]] CollateState : public POpActorState<CollateStatefulActor> {
public:
  [[maybe_unused]] void setState(CollateStatefulActor actor,
				const std::string &name,
				long queryId,
				const ::caf::actor &rootActorHandle,
				const ::caf::actor &segmentCacheActorHandle) {

	POpActorState::setBaseState(actor, name, queryId, rootActorHandle, segmentCacheActorHandle);
  }

  template<class... Handlers>
  CollateActor::behavior_type makeBehavior(CollateStatefulActor actor, Handlers... handlers) {
	return POpActorState::makeBaseBehavior(
		actor,
		[=](TupleSetAtom, const TupleSetPtr &tupleSet) {
		  process(actor,
				  [=](const ::caf::strong_actor_ptr &messageSender) {
					return onTupleSet(actor, messageSender, tupleSet);
				  });
		},
		[=](GetTupleSetAtom) {
		  return process(actor,
						 [=](const ::caf::strong_actor_ptr &messageSender) {
						   return onGetTupleSet(actor, messageSender);
						 });
		},
		std::move(handlers)...
	);
  }

private:
  std::shared_ptr<TupleSet> tupleSet_;

protected:

  tl::expected<void, std::string> onStart(CollateStatefulActor /*actor*/,
										  const strong_actor_ptr &/*messageSender*/) override {
	if (tupleSet_)
	  tupleSet_.reset();
	return {};
  }

  tl::expected<void, std::string> onComplete(CollateStatefulActor actor,
											 const ::caf::strong_actor_ptr & /*messageSender*/) override {
	if (!isComplete() && isAllProducersComplete()) {
	  return notifyComplete(actor);
	}
	return {};
  }

  tl::expected<void, std::string> onEnvelope(CollateStatefulActor actor,
											 const ::caf::strong_actor_ptr &messageSender,
											 const Envelope &envelope) override {
	if (envelope.message().type() == MessageType::TUPLESET) {
	  auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(envelope.message());
	  return this->onTupleSet(actor, messageSender, tupleSetMessage.tuples());
	} else {
	  return tl::make_unexpected(fmt::format("Unrecognized message type {}", envelope.message().getTypeString()));
	}
  }

private:

  [[nodiscard]] tl::expected<void, std::string> onTupleSet(CollateStatefulActor actor,
														   const ::caf::strong_actor_ptr &messageSender,
														   const TupleSetPtr &tupleSet) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Received tupleSet  |  sender: {}", actor->id(),
				 actor->name(), to_string(messageSender));

	if (!tupleSet_) {
	  tupleSet_ = tupleSet;
	  return {};
	} else {
	  return tupleSet_->append(tupleSet);
	}
  }

  [[nodiscard]] ExpectedTupleSetPtrString onGetTupleSet(CollateStatefulActor actor,
														const ::caf::strong_actor_ptr &messageSender) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Getting tupleSet  |  sender: {}", actor->id(),
				 actor->name(), to_string(messageSender));

	return tupleSet_;
  }

};

[[maybe_unused]] CollateActor::behavior_type CollateFunctor(CollateStatefulActor actor,
										   const std::string &name,
										   long queryId,
										   const ::caf::actor &rootActorHandle,
										   const ::caf::actor &segmentCacheActorHandle);

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_COLLATE_COLLATEPOP2_H
