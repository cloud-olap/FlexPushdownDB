//
// Created by matt on 16/12/19.
//

#include "normal/core/ATTIC/Normal.h"

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

  for(const auto &query: queries_){
    query->close();
  }
  queries_.clear();

  operatorManager_->stop();
}

std::shared_ptr<OperatorGraph> Normal::createQuery() {
  auto query = OperatorGraph::make(operatorManager_);
  queries_.push_back(query);
  return query;
}

}
