//
// Created by matt on 30/9/20.
//

#include "normal/core/message/ConnectMessage.h"

namespace normal::core::message {

ConnectMessage::ConnectMessage(std::vector<OperatorConnection> operatorConnections,
							   std::string from) :
	Message("ConnectMessage", std::move(from)),
	operatorConnections_(std::move(operatorConnections)) {

}

const std::vector<OperatorConnection> &ConnectMessage::connections() const {
  return operatorConnections_;
}

}