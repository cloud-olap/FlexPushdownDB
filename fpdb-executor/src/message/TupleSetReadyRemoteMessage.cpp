//
// Created by Yifei Yang on 7/3/22.
//

#include <fpdb/executor/message/TupleSetReadyRemoteMessage.h>

namespace fpdb::executor::message {

TupleSetReadyRemoteMessage::TupleSetReadyRemoteMessage(const std::string &host,
                                                       int port,
                                                       bool isFromStore,
                                                       const std::string &sender,
                                                       const std::optional<std::string> &originalConsumer) :
  Message(TUPLESET_READY_REMOTE, sender),
  host_(host),
  port_(port),
  isFromStore_(isFromStore),
  originalConsumer_(originalConsumer) {}

std::string TupleSetReadyRemoteMessage::getTypeString() const {
  return "TupleSetReadyRemoteMessage";
}

const std::string &TupleSetReadyRemoteMessage::getHost() const {
  return host_;
}

int TupleSetReadyRemoteMessage::getPort() const {
  return port_;
}

bool TupleSetReadyRemoteMessage::isFromStore() const {
  return isFromStore_;
}

const std::optional<std::string> &TupleSetReadyRemoteMessage::getOriginalConsumer() const {
  return originalConsumer_;
}

}
