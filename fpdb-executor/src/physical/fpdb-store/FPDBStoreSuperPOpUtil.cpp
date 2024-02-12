//
// Created by Yifei Yang on 3/31/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOpUtil.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <queue>

namespace fpdb::executor::physical::fpdb_store {

std::vector<std::string>
FPDBStoreSuperPOpUtil::getPredicateColumnNames(const std::shared_ptr<FPDBStoreSuperPOp> &fpdbStoreSuperPOp) {
  std::set<std::string> allPredicateColumnNames;
  for (const auto &opIt: fpdbStoreSuperPOp->getSubPlan()->getPhysicalOps()) {
    auto op = opIt.second;
    if (op->getType() == POpType::FILTER) {
      auto typedOp = std::static_pointer_cast<filter::FilterPOp>(op);
      auto predicateColumns = typedOp->getPredicate()->involvedColumnNames();
      allPredicateColumnNames.insert(predicateColumns.begin(), predicateColumns.end());
    } else if (op->getType() == POpType::BLOOM_FILTER_USE) {
      auto typedOp = std::static_pointer_cast<bloomfilter::BloomFilterUsePOp>(op);
      auto bloomFilterColumns = typedOp->getBloomFilterColumnNames();
      allPredicateColumnNames.insert(bloomFilterColumns.begin(), bloomFilterColumns.end());
    }
  }
  return {allPredicateColumnNames.begin(), allPredicateColumnNames.end()};
}

std::vector<std::set<std::string>>
FPDBStoreSuperPOpUtil::getProjectColumnGroups(const std::shared_ptr<FPDBStoreSuperPOp> &fpdbStoreSuperPOp) {
  // find root op
  auto expRootOp = fpdbStoreSuperPOp->getSubPlan()->getRootPOp();
  if (!expRootOp) {
    throw std::runtime_error(expRootOp.error());
  }
  auto rootOp = *expRootOp;

  // construct projectColumnGroups in DFS, from unary sets made from projectColumnNames of root
  auto projectColumnGroups = splitToUnarySet(rootOp->getProjectColumnNames());
  std::queue<std::shared_ptr<PhysicalOp>> opQueue;
  opQueue.push(rootOp);

  while (!opQueue.empty()) {
    auto op = opQueue.front();
    opQueue.pop();

    // transform projectColumnGroups
    switch (op->getType()) {
      case FPDB_STORE_FILE_SCAN:
      case FILTER:
      case COLLATE: {
        // noop because no new column names are generated
        break;
      }
      case PROJECT: {
        auto typedOp = std::static_pointer_cast<project::ProjectPOp>(op);
        transformProjectColumnGroupsProject(typedOp, projectColumnGroups);
        break;
      }
      case AGGREGATE: {
        auto typedOp = std::static_pointer_cast<aggregate::AggregatePOp>(op);
        transformProjectColumnGroupsAggregate(typedOp, projectColumnGroups);
        break;
      }
      default: {
        throw std::runtime_error(fmt::format("Unsupported physical operator type at FPDB store: {}", op->getTypeString()));
      }
    }

    // add producers, currently should only find one producer in pushdown subPlan
    auto producerNames = op->producers();
    if (producerNames.empty()) {
      continue;
    } else if (producerNames.size() > 1) {
      throw std::runtime_error("Currently pushdown only supports plan with serial operators");
    } else {
      auto expProducer = fpdbStoreSuperPOp->getSubPlan()->getPhysicalOp(*producerNames.begin());
      if (!expProducer.has_value()) {
        throw std::runtime_error(expProducer.error());
      }
      opQueue.push(*expProducer);
    }
  }

  return projectColumnGroups;
}

void FPDBStoreSuperPOpUtil::transformProjectColumnGroupsProject(
        const std::shared_ptr<project::ProjectPOp> &projectPOp,
        std::vector<std::set<std::string>> &projectColumnGroups) {
  std::vector<std::set<std::string>> inputColumnNameSets;
  std::vector<std::string> outputColumnNames;

  // get input and output column names from projectColumnNamePairs
  for (const auto &pair: projectPOp->getProjectColumnNamePairs()) {
    inputColumnNameSets.emplace_back(std::set<std::string>{pair.first});
    outputColumnNames.emplace_back(pair.second);
  }

  // get input and output column names from project exprs
  auto exprs = projectPOp->getExprs();
  auto exprNames = projectPOp->getExprNames();
  for (uint i = 0; i < exprs.size(); ++i) {
    inputColumnNameSets.emplace_back(exprs[i]->involvedColumnNames());
    outputColumnNames.emplace_back(exprNames[i]);
  }

  // transform
  transformProjectColumnGroups(inputColumnNameSets, outputColumnNames, projectColumnGroups);
}

void FPDBStoreSuperPOpUtil::transformProjectColumnGroupsAggregate(
        const std::shared_ptr<aggregate::AggregatePOp> &aggregatePOp,
        std::vector<std::set<std::string>> &projectColumnGroups) {
  std::vector<std::set<std::string>> inputColumnNameSets;
  std::vector<std::string> outputColumnNames;

  // get input and output column names
  for (const auto &function: aggregatePOp->getFunctions()) {
    auto expr = function->getExpression();
    std::set<std::string> inputColumnNames;
    if (expr != nullptr) {
      inputColumnNames = expr->involvedColumnNames();
    }
    auto outputColumnName = function->getOutputColumnName();

    inputColumnNameSets.emplace_back(inputColumnNames);
    outputColumnNames.emplace_back(outputColumnName);
  }

  // transform
  transformProjectColumnGroups(inputColumnNameSets, outputColumnNames, projectColumnGroups);
}

void FPDBStoreSuperPOpUtil::transformProjectColumnGroups(const std::vector<std::set<std::string>> &inputColumnNameSets,
                                                         const std::vector<std::string> &outputColumnNames,
                                                         std::vector<std::set<std::string>> &projectColumnGroups) {
  if (inputColumnNameSets.size() != outputColumnNames.size()) {
    throw std::runtime_error(fmt::format("Input and output column names have different size when transforming "
                             "projectColumnGroups: {} vs {}", inputColumnNameSets.size(), outputColumnNames.size()));
  }

  // transform
  for (uint i = 0; i < inputColumnNameSets.size(); ++i) {
    auto inputColumnNameSet = inputColumnNameSets[i];
    auto outputColumnName = outputColumnNames[i];
    for (auto &projectColumnGroup: projectColumnGroups) {
      if (projectColumnGroup.find(outputColumnName) != projectColumnGroup.end()) {
        projectColumnGroup.erase(outputColumnName);
        projectColumnGroup.insert(inputColumnNameSet.begin(), inputColumnNameSet.end());
      }
    }
  }
}

}
