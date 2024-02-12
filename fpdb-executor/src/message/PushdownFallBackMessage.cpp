//
// Created by Yifei Yang on 12/15/22.
//

#include <fpdb/executor/message/PushdownFallBackMessage.h>

namespace fpdb::executor::message {

PushdownFallBackMessage::PushdownFallBackMessage(const std::string &sender):
  Message(PUSHDOWN_FALL_BACK, sender) {}

std::string PushdownFallBackMessage::getTypeString() const {
  return "PushdownFallBackMessage";
}

}
