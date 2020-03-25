//
// Created by matt on 4/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_ENVELOPE_H
#define NORMAL_NORMAL_CORE_SRC_ENVELOPE_H

#include <memory>

#include <caf/all.hpp>

#include "normal/core/message/Message.h"

namespace normal::core::message {
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

private:
  std::shared_ptr<Message> message_;

public:
  explicit Envelope(std::shared_ptr<Message> message);
  [[nodiscard]] const Message &message() const;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::message::Envelope)

#endif //NORMAL_NORMAL_CORE_SRC_ENVELOPE_H
