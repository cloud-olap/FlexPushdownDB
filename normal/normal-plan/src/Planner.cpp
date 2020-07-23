//
// Created by matt on 16/4/20.
//

#include <normal/plan/Planner.h>

#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/plan/operator_/ScanLogicalOperator.h>
#include <normal/plan/operator_/JoinLogicalOperator.h>
#include <normal/pushdown/shuffle/Shuffle.h>
#include <normal/plan/operator_/AggregateLogicalOperator.h>

using namespace normal::plan;

// Not applicable for aggregate, project and group logical operators (their physical operators depend on their producers)
std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> toPhysicalOperators (
        std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalOperator,
        std::shared_ptr<std::unordered_map<
          std::shared_ptr<normal::plan::operator_::LogicalOperator>,
          std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>>> &logicalToPhysical_map,
        std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> &allPhysicalOperators) {

  auto existPhysicalOperators = logicalToPhysical_map->find(logicalOperator);
  if (existPhysicalOperators == logicalToPhysical_map->end()) {
    auto physicalOperators = logicalOperator->toOperators();
    logicalToPhysical_map->insert({logicalOperator, physicalOperators});
    allPhysicalOperators->insert(allPhysicalOperators->end(), physicalOperators->begin(), physicalOperators->end());
    return physicalOperators;
  } else {
    return existPhysicalOperators->second;
  }
}

void wireUp (std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalProducer,
             std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalConsumer,
             std::shared_ptr<std::unordered_map<
                     std::shared_ptr<normal::plan::operator_::LogicalOperator>,
                     std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>>> &logicalToPhysical_map,
             std::shared_ptr<std::vector<std::shared_ptr<normal::plan::operator_::LogicalOperator>>> &wiredLogicalProducers,
             std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> allPhysicalOperators){

  // if logicalProducer is already wired, return
  if (std::find(wiredLogicalProducers->begin(), wiredLogicalProducers->end(), logicalProducer) != wiredLogicalProducers->end()) {
    return;
  }

  // extract stream-out physical operators
  std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> streamOutPhysicalOperators;

  if (logicalProducer->type()->is(operator_::type::OperatorTypes::joinOperatorType())){
    // get join physical operators
    auto joinPhysicalOperators = toPhysicalOperators(logicalProducer, logicalToPhysical_map, allPhysicalOperators);

    // joinProbes are stream-out operators
    streamOutPhysicalOperators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>(
            joinPhysicalOperators->begin() + joinPhysicalOperators->size() / 2, joinPhysicalOperators->end());
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::aggregateOperatorType())) {
    // get aggregate physical operators
    std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> aggregatePhysicalOperators;
    auto aggregatePhysicalOperators_pair = logicalToPhysical_map->find(logicalProducer);
    if (aggregatePhysicalOperators_pair == logicalToPhysical_map->end()) {
      auto aggregateLogicalOperator = std::static_pointer_cast<normal::plan::operator_::AggregateLogicalOperator>(logicalProducer);
      auto producerOfAggregateLogicalOperator = aggregateLogicalOperator->getProducer();
      wireUp(producerOfAggregateLogicalOperator, logicalProducer, logicalToPhysical_map, wiredLogicalProducers, allPhysicalOperators);
      aggregatePhysicalOperators = logicalToPhysical_map->find(logicalProducer)->second;
    } else {
      aggregatePhysicalOperators = aggregatePhysicalOperators_pair->second;
    }

    // the single aggregate or aggregateReduce is stream-out operator
    if (aggregatePhysicalOperators->size() == 1) {
      streamOutPhysicalOperators = aggregatePhysicalOperators;
    } else {
      streamOutPhysicalOperators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
      streamOutPhysicalOperators->emplace_back(aggregatePhysicalOperators->back());
    }
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::groupOperatorType())) {

  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::projectOperatorType())) {

  }

  else {
    streamOutPhysicalOperators = toPhysicalOperators(logicalProducer, logicalToPhysical_map, allPhysicalOperators);
  }


  // wire up to stream-in physical operators
  if (logicalConsumer->type()->is(operator_::type::OperatorTypes::joinOperatorType())) {
    auto joinPhysicalOperators = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators);
    auto joinLogicalOperator = std::static_pointer_cast<normal::plan::operator_::JoinLogicalOperator>(logicalConsumer);
    auto leftColumnName = joinLogicalOperator->getLeftColumnName();
    auto rightColumnName = joinLogicalOperator->getRightColumnName();

    // joinBuilds and joinProbes
    auto joinBuilds = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>(
            joinPhysicalOperators->begin(), joinPhysicalOperators->begin() + joinPhysicalOperators->size() / 2);
    auto joinProbes = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>(
            joinPhysicalOperators->begin() + joinPhysicalOperators->size() / 2, joinPhysicalOperators->end());

    // check logicalComsumer is left consumer or right consumer
    if (logicalProducer == joinLogicalOperator->getLeftProducer()) {
      // construct a shuffle physical operator for each stream-out operator
      auto shuffles = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::shuffle::Shuffle>>>();
      for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
        auto shuffle = normal::pushdown::shuffle::Shuffle::make(streamOutPhysicalOperator->name() + "-shuffle",
                                                                leftColumnName);
        // wire up
        streamOutPhysicalOperator->produce(shuffle);
        shuffle->consume(streamOutPhysicalOperator);
        for (const auto &joinBuild: *joinBuilds) {
          shuffle->produce(joinBuild);
          joinBuild->consume(shuffle);
        }
        allPhysicalOperators->emplace_back(shuffle);
      }
    } else {
      // construct a shuffle physical operator for each stream-out operator
      for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
        auto shuffle = normal::pushdown::shuffle::Shuffle::make(streamOutPhysicalOperator->name() + "-shuffle",
                                                                rightColumnName);
        // wire up
        streamOutPhysicalOperator->produce(shuffle);
        shuffle->consume(streamOutPhysicalOperator);
        for (const auto &joinProbe: *joinProbes) {
          shuffle->produce(joinProbe);
          joinProbe->consume(shuffle);
        }
        allPhysicalOperators->emplace_back(shuffle);
      }
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::aggregateOperatorType())){
    // get aggregate physical operators
    auto numConcurrentUnits = streamOutPhysicalOperators->size();
    std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> aggregatePhysicalOperators;
    auto aggregatePhysicalOperators_pair = logicalToPhysical_map->find(logicalConsumer);
    if (aggregatePhysicalOperators_pair == logicalToPhysical_map->end()) {
      auto aggregateLogicalOperator = std::static_pointer_cast<normal::plan::operator_::AggregateLogicalOperator>(logicalConsumer);
      aggregateLogicalOperator->setNumConcurrentUnits(numConcurrentUnits);
      aggregatePhysicalOperators = aggregateLogicalOperator->toOperators();
      logicalToPhysical_map->insert({aggregateLogicalOperator, aggregatePhysicalOperators});
      allPhysicalOperators->insert(allPhysicalOperators->end(), aggregatePhysicalOperators->begin(), aggregatePhysicalOperators->end());
    } else {
      aggregatePhysicalOperators = aggregatePhysicalOperators_pair->second;
    }

    // wire up to all except aggregateReduce
    for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
      for (auto index = 0; index < numConcurrentUnits; index++) {
        auto streamInPhysicalOperator = aggregatePhysicalOperators->at(index);
        streamOutPhysicalOperator->produce(streamInPhysicalOperator);
        streamInPhysicalOperator->consume(streamOutPhysicalOperator);
      }
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::groupOperatorType())) {

  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::projectOperatorType())) {

  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::collateOperatorType())){
    auto collatePhysicalOperator = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators)->at(0);
    for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
      streamOutPhysicalOperator->produce(collatePhysicalOperator);
      collatePhysicalOperator->consume(streamOutPhysicalOperator);
    }
  }

  else {
    std::runtime_error("Bad logicalConsumer type, check logical plan");
  }

}

std::shared_ptr<PhysicalPlan> Planner::generate(const LogicalPlan &logicalPlan) {
  auto physicalPlan = std::make_shared<PhysicalPlan>();
  auto logicalOperators = logicalPlan.getOperators();
  auto logicalToPhysical_map = std::make_shared<std::unordered_map<
          std::shared_ptr<normal::plan::operator_::LogicalOperator>,
          std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>>>();
  auto wiredLogicalProducers = std::make_shared<std::vector<std::shared_ptr<normal::plan::operator_::LogicalOperator>>>();
  auto allPhysicalOperators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();

  for (auto &logicalProducer: *logicalOperators) {
    auto logicalConsumer = logicalProducer->getConsumer();
    if (logicalConsumer) {
      wireUp(logicalProducer, logicalConsumer, logicalToPhysical_map, wiredLogicalProducers, allPhysicalOperators);
    }
  }

  for (const auto &physicalOperator: *allPhysicalOperators) {
    physicalPlan->put(physicalOperator);
  }

  return physicalPlan;
}
