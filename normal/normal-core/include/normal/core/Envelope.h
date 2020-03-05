//
// Created by matt on 4/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_ENVELOPE_H
#define NORMAL_NORMAL_CORE_SRC_ENVELOPE_H

#include <memory>
#include <caf/all.hpp>

#include "Message.h"

namespace normal::core {

class Envelope {
private:
  std::shared_ptr<Message> message_;

public:
  explicit Envelope(std::shared_ptr<Message> message);
  [[nodiscard]] const Message &message() const;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::Envelope)

#endif //NORMAL_NORMAL_CORE_SRC_ENVELOPE_H
