//
// Created by matt on 4/3/20.
//

#include <fpdb/executor/message/Envelope.h>
#include <memory>
#include <utility>

namespace fpdb::executor::message {

Envelope::Envelope(std::shared_ptr<Message> message) :
    message_(std::move(message)) {}

const Message &Envelope::message() const {
  return *message_;
}

} // namespace