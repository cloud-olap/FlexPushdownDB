//
// Created by matt on 16/4/20.
//

#include <normal/plan/OperatorTypes.h>
#include <normal/plan/ScanLogicalOperator.h>
#include "normal/plan/Planner.h"

std::shared_ptr<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>> getRootOperators(std::shared_ptr<LogicalPlan> logicalPlan){
  auto rootOperators = std::make_shared<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>>();
  for (const auto &logicalOperator: *logicalPlan->getOperators()) {
	if (logicalOperator->type()->is(OperatorTypes::scanOperatorType())) {
	  rootOperators->push_back(logicalOperator);
	}
  }
  return rootOperators;
}

void visit(std::shared_ptr<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>> operators,
		   std::vector<std::shared_ptr<normal::core::Operator>> producers,
		   std::shared_ptr<PhysicalPlan> physicalPlan) {

  for (const auto &logicalOperator: *operators) {

	if (logicalOperator->type()->is(OperatorTypes::scanOperatorType())) {

	  // Examine the partitioning scheme of the scan operator
	  auto scanOp = std::static_pointer_cast<normal::plan::ScanLogicalOperator>(logicalOperator);
	  auto partitioningScheme = scanOp->getPartitioningScheme();
	  auto partitions = partitioningScheme->partitions();

	  // Create a physical scan operator for each partition
	  auto physicalOperators = std::vector<std::shared_ptr<normal::core::Operator>>();
	  for (const auto &partition: *partitions) {
		auto physicalOperator = logicalOperator->toOperator();
		physicalOperators.push_back(physicalOperator);

		// Add the physical scan operator to the plan
		physicalPlan->put(physicalOperator);
	  }

	  // Visit the consumers of this operator
	  if (logicalOperator->getConsumer() != nullptr) {
		auto consumers = std::make_shared<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>>();
		consumers->push_back(logicalOperator->getConsumer());
		visit(consumers, physicalOperators, physicalPlan);
	  }
	} else {

	  // Create a physical operator for each producer
	  auto physicalOperators = std::vector<std::shared_ptr<normal::core::Operator>>();
	  for (const auto &producer: producers) {
		auto physicalOperator = logicalOperator->toOperator();
		physicalOperators.push_back(physicalOperator);

		// Connect this operator to its producer
		producer->produce(physicalOperator);
		physicalOperator->consume(producer);

		// Add the physical scan operator to the plan
		physicalPlan->put(physicalOperator);
	  }

	  // Visit the consumers of this operator
	  if (logicalOperator->getConsumer() != nullptr) {
		auto consumers = std::make_shared<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>>();
		consumers->push_back(logicalOperator->getConsumer());
		visit(consumers, physicalOperators, physicalPlan);
	  }
	}
  }

}

std::shared_ptr<PhysicalPlan> Planner::generate(std::shared_ptr<LogicalPlan> logicalPlan) {
  auto physicalPlan = std::make_shared<PhysicalPlan>();
  std::vector<std::shared_ptr<normal::core::Operator>> producers = {};
  auto rootOperators = getRootOperators(logicalPlan);
  visit(rootOperators, producers, physicalPlan);
  return physicalPlan;
}
