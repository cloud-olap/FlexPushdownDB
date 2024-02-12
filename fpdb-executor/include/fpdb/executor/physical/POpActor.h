//
// Created by Matt Youill on 31/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPACTOR_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPACTOR_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/Envelope.h>
#include <fpdb/executor/caf-serialization/CAFMessageSerializer.h>
#include <fpdb/executor/physical/Forward.h>
#include <fpdb/caf/CAFUtil.h>
#include <memory>
#include <queue>

CAF_BEGIN_TYPE_ID_BLOCK(POpActor, fpdb::caf::CAFUtil::POpActor_first_custom_type_id)
CAF_ADD_ATOM(POpActor, GetProcessingTimeAtom)
CAF_END_TYPE_ID_BLOCK(POpActor)

namespace fpdb::executor::physical {

/**
 * Physical operator actor implements caf::actor and combines the physical operators' behaviour and state
 */
class POpActor : public ::caf::event_based_actor {

public:
  POpActor(::caf::actor_config &cfg, std::shared_ptr<PhysicalOp> opBehaviour);

  std::shared_ptr<PhysicalOp> operator_() const;
  ::caf::behavior make_behavior() override;
  void on_regular_message(const fpdb::executor::message::Envelope &msg);
  void on_exit() override;
  const char* name() const override {
    return name_.c_str();
  }

  long getProcessingTime() const;
  void incrementProcessingTime(long time);
  bool running_ = false;
  std::string name_;
  std::queue<fpdb::executor::message::Envelope> messageBuffer_;

private:
  std::shared_ptr<TupleSet> read_remote_table(const std::string &host, int port, const std::string &sender);

  std::shared_ptr<PhysicalOp> opBehaviour_;
  long processingTime_ = 0;

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPACTOR_H
