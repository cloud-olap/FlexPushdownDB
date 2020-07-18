//
// Created by matt on 16/4/20.
//

#include <normal/plan/Planner.h>

#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/plan/operator_/ScanLogicalOperator.h>

using namespace normal::plan;

//std::shared_ptr<std::vector<std::shared_ptr<operator_::LogicalOperator>>> getRootOperators(const LogicalPlan &logicalPlan){
//  auto rootOperators = std::make_shared<std::vector<std::shared_ptr<operator_::LogicalOperator>>>();
//  for (const auto &logicalOperator: *logicalPlan.getOperators()) {
//	if (logicalOperator->type()->is(operator_::type::OperatorTypes::scanOperatorType())) {
//	  rootOperators->push_back(logicalOperator);
//	}
//  }
//  return rootOperators;
//}

//void visit(const std::shared_ptr<std::vector<std::shared_ptr<operator_::LogicalOperator>>>& operators,
//		   const std::vector<std::shared_ptr<normal::core::Operator>>& producers,
//		   PhysicalPlan &physicalPlan) {
//
//  for (const auto &logicalOperator: *operators) {
//
//	if (logicalOperator->type()->is(operator_::type::OperatorTypes::scanOperatorType())) {
//
//	  // Examine the partitioning scheme of the scan operator
//	  auto scanOp = std::static_pointer_cast<operator_::ScanLogicalOperator>(logicalOperator);
//	  auto partitioningScheme = scanOp->getPartitioningScheme();
//	  auto partitions = partitioningScheme->partitions();
//
//	  // Create a physical scan operator for each partition
//	  auto physicalOperators = logicalOperator->toOperators();
//	  for (const auto &physicalOperator: *physicalOperators) {
//		// Add the physical scan operator to the plan
//		physicalPlan.put(physicalOperator);
//	  }
//
//	  // Visit the consumers of this operator
//	  if (logicalOperator->getConsumer() != nullptr) {
//		auto consumers = std::make_shared<std::vector<std::shared_ptr<operator_::LogicalOperator>>>();
//		consumers->push_back(logicalOperator->getConsumer());
//		visit(consumers, *physicalOperators, physicalPlan);
//	  }
//	} else if (logicalOperator->type()->is(operator_::type::OperatorTypes::collateOperatorType())) {
//
//	  // Create a single collate operator, collate operators are never multi-partition
//	  auto physicalCollateOperator = logicalOperator->toOperator();
//
//	  // Connect all producers to the single collate
//	  for (const auto &producer: producers) {
//		producer->produce(physicalCollateOperator);
//		physicalCollateOperator->consume(producer);
//	  }
//
//	  // Add the collate operator to the plan
//	  physicalPlan.put(physicalCollateOperator);
//	} else {
//
//	  // Create a physical operator for each producer
//	  auto physicalOperators = std::vector<std::shared_ptr<normal::core::Operator>>();
//	  for (const auto &producer: producers) {
//		auto physicalOperator = logicalOperator->toOperator();
//
//		// FIXME: A hack to make sure the op is named after its partition
//		physicalOperator->setName(physicalOperator->name() + "/" + producer->name());
//
//		physicalOperators.push_back(physicalOperator);
//
//		// Connect this operator to its producer
//		producer->produce(physicalOperator);
//		physicalOperator->consume(producer);
//
//		// Add the physical scan operator to the plan
//		physicalPlan.put(physicalOperator);
//	  }
//
//	  // Visit the consumers of this operator
//	  if (logicalOperator->getConsumer() != nullptr) {
//		auto consumers = std::make_shared<std::vector<std::shared_ptr<operator_::LogicalOperator>>>();
//		consumers->push_back(logicalOperator->getConsumer());
//		visit(consumers, physicalOperators, physicalPlan);
//	  }
//	}
//  }
//
//}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> toPhysicalOperators (
        std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalOperator,
        std::shared_ptr<std::unordered_map<
          std::shared_ptr<normal::plan::operator_::LogicalOperator>,
          std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>>> &logicalToPhysical_map) {

  auto existPhysicalOperators = logicalToPhysical_map->find(logicalOperator);
  if (existPhysicalOperators == logicalToPhysical_map->end()) {
    auto physicalOperators = logicalOperator->toOperators();
    logicalToPhysical_map->insert({logicalOperator, physicalOperators});
    return physicalOperators;
  } else {
    return existPhysicalOperators->second;
  }
}

std::shared_ptr<PhysicalPlan> Planner::generate(const LogicalPlan &logicalPlan) {
  auto physicalPlan = std::make_shared<PhysicalPlan>();
  auto logicalOperators = logicalPlan.getOperators();
  auto logicalToPhysical_map = std::make_shared<std::unordered_map<
          std::shared_ptr<normal::plan::operator_::LogicalOperator>,
          std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>>>();

  for (const auto &logicalOperator: *logicalOperators) {
    auto logicalProducer = logicalOperator;
    auto logicalConsumer = logicalOperator->getConsumer();
    auto physicalProducers = toPhysicalOperators(logicalProducer, logicalToPhysical_map);

    while (logicalConsumer) {
      // check if this logical operator has been visited
      if (logicalToPhysical_map->find(logicalConsumer) != logicalToPhysical_map->end()) {
        break;
      }

      auto physicalConsumers = toPhysicalOperators(logicalConsumer, logicalToPhysical_map);

      // wire up
      for (const auto &physicalProducer: *physicalProducers) {
        for (const auto &physicalConsumer: *physicalConsumers) {
          physicalProducer->produce(physicalConsumer);
          physicalConsumer->produce(physicalProducer);
        }
      }

      logicalProducer = logicalConsumer;
      logicalConsumer = logicalProducer->getConsumer();
      physicalProducers = physicalConsumers;
    }
  }

  return physicalPlan;
}
