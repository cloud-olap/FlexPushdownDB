//
// Created by matt on 23/9/20.
//

#include <fpdb/executor/physical/POpConnection.h>
#include <utility>

namespace fpdb::executor::physical {

POpConnection::POpConnection(std::string name,
                             ::caf::actor actorHandle,
                             POpRelationshipType connectionType,
                             int nodeId) :
	name_(std::move(name)),
	actorHandle_(std::move(actorHandle)),
	connectionType_(connectionType),
  nodeId_(nodeId) {}

const std::string &POpConnection::getName() const {
  return name_;
}

const ::caf::actor &POpConnection::getActorHandle() const {
  return actorHandle_;
}

POpRelationshipType POpConnection::getConnectionType() const {
  return connectionType_;
}

int POpConnection::getNodeId() const {
  return nodeId_;
}

}
