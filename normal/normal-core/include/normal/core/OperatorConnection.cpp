//
// Created by matt on 23/9/20.
//

#include "OperatorConnection.h"

#include <utility>

namespace normal::core {

OperatorConnection::OperatorConnection(std::string name,
									   caf::actor actorHandle,
									   OperatorRelationshipType connectionType) :
	name_(std::move(name)),
	actorHandle_(std::move(actorHandle)),
	connectionType_(connectionType) {}

const std::string &OperatorConnection::getName() const {
  return name_;
}

const caf::actor &OperatorConnection::getActorHandle() const {
  return actorHandle_;
}

OperatorRelationshipType OperatorConnection::getConnectionType() const {
  return connectionType_;
}

}
