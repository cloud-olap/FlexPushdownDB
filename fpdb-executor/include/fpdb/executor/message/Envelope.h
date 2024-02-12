//
// Created by matt on 4/3/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ENVELOPE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ENVELOPE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/caf/CAFUtil.h>
#include <memory>

namespace fpdb::executor::message {
/**
 * Class encapsulating a message sent between actors
 *
 * CAF seems to want its messages to be declared as temporaries which it then copies. To make sure CAF doesn't copy
 * any big messages the actual message is kept only as a pointer in this Envelope class.
 *
 * FIXME: Is Envelope the best name? Does it imply this class does more than it actually does? This will need to be
 * reworked when moving to tuped actors so probably not super important
 */
class Envelope {

public:
  explicit Envelope(std::shared_ptr<Message> message);
  Envelope() = default;
  Envelope(const Envelope&) = default;
  Envelope& operator=(const Envelope&) = default;

  const Message &message() const;

private:
  std::shared_ptr<Message> message_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Envelope& env) {
    return f.apply(env.message_);
  };
};

}

CAF_BEGIN_TYPE_ID_BLOCK(Envelope, fpdb::caf::CAFUtil::Envelope_first_custom_type_id)
CAF_ADD_TYPE_ID(Envelope, (fpdb::executor::message::Envelope))
CAF_END_TYPE_ID_BLOCK(Envelope)

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ENVELOPE_H
