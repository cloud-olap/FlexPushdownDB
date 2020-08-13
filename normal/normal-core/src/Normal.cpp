//
// Created by matt on 16/12/19.
//

#include "normal/core/Normal.h"

#include "normal/core/Globals.h"

namespace normal::core {

Normal::Normal() :
	operatorManager_(std::make_shared<OperatorManager>()) {
  operatorManager_->boot();
  operatorManager_->start();
}

std::shared_ptr<Normal> Normal::start() {
  return std::make_shared<Normal>();
}

void Normal::stop() {
  operatorManager_->stop();
}

std::shared_ptr<OperatorGraph> Normal::createQuery() {
  return OperatorGraph::make(operatorManager_);
}

const std::shared_ptr<OperatorManager> &Normal::getOperatorManager() const {
  return operatorManager_;
}

}
