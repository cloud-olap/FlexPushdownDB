//
// Created by matt on 30/9/20.
//

#include <fpdb/executor/message/ConnectMessage.h>

namespace fpdb::executor::message {

ConnectMessage::ConnectMessage(std::vector<POpConnection> connections,
							   std::string from) :
	Message(CONNECT, std::move(from)),
	connections_(std::move(connections)) {}

std::string ConnectMessage::getTypeString() const {
  return "ConnectMessage";
}

const std::vector<POpConnection> &ConnectMessage::connections() const {
  return connections_;
}

}