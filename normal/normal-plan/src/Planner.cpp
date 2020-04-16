//
// Created by matt on 16/4/20.
//

#include "normal/plan/Planner.h"

std::shared_ptr<PhysicalPlan> Planner::generate(std::shared_ptr<LogicalPlan> logicalPlan) {

  auto physicalPlan = std::make_shared<PhysicalPlan>();

  // Convert all the logical operators to physical
  for(const auto& logicalOperator: *logicalPlan->getOperators()){
	physicalPlan->put(logicalOperator->toOperator());
  }

  // Connect physical producers and consumers
  for(const auto& logicalOperator: *logicalPlan->getOperators()){
	auto op = physicalPlan->get(logicalOperator->name);
	if(logicalOperator->consumer != nullptr){
	  auto consumerOp = physicalPlan->get(logicalOperator->consumer->name);
	  op->produce(consumerOp);
	  consumerOp->consume(op);
	}
  }

  return physicalPlan;
}
