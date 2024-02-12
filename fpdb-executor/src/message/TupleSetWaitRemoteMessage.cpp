//
// Created by Yifei Yang on 10/10/22.
//

#include <fpdb/executor/message/TupleSetWaitRemoteMessage.h>

namespace fpdb::executor::message {

TupleSetWaitRemoteMessage::TupleSetWaitRemoteMessage(const std::string &host,
                                                     int port,
                                                     const std::string &sender) :
  Message(TUPLESET_WAIT_REMOTE, sender),
  host_(host),
  port_(port) {}

std::string TupleSetWaitRemoteMessage::getTypeString() const {
  return "TupleSetWaitRemoteMessage";
}

const std::string &TupleSetWaitRemoteMessage::getHost() const {
  return host_;
}

int TupleSetWaitRemoteMessage::getPort() const {
  return port_;
}

}
