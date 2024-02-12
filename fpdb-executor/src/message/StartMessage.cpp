//
// Created by matt on 5/1/20.
//

#include <fpdb/executor/message/StartMessage.h>
#include <utility>

namespace fpdb::executor::message {

StartMessage::StartMessage(std::string from) :
  Message(START, std::move(from)) {}

std::string StartMessage::getTypeString() const {
  return "StartMessage";
}

}
