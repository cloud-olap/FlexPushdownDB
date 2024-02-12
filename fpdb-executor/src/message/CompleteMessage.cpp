//
// Created by matt on 5/3/20.
//

#include <fpdb/executor/message/CompleteMessage.h>
#include <utility>

namespace fpdb::executor::message {

CompleteMessage::CompleteMessage(std::string sender) :
  Message(COMPLETE, std::move(sender)) {}

std::string CompleteMessage::getTypeString() const {
  return "CompleteMessage";
}

}