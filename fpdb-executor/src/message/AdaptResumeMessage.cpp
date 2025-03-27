//
// Created by Yifei Yang on 4/5/24.
//

#include <fpdb/executor/message/AdaptResumeMessage.h>
#include <utility>

namespace fpdb::executor::message {

AdaptResumeMessage::AdaptResumeMessage(bool preserve, std::string sender) :
  Message(ADAPT_RESUME, std::move(sender)),
  preserve_(preserve) {}

std::string AdaptResumeMessage::getTypeString() const {
  return "AdaptResumeMessage";
}

bool AdaptResumeMessage::preserve() const {
  return preserve_;
}

}