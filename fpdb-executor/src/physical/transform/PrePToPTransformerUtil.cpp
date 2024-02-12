//
// Created by Yifei Yang on 2/22/22.
//

#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <fpdb/executor/physical/aggregate/function/MinMax.h>
#include <fpdb/executor/physical/aggregate/function/Avg.h>
#include <fpdb/executor/physical/aggregate/function/AvgReduce.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/expression/gandiva/Column.h>
#include <queue>

namespace fpdb::executor::physical {
  
void PrePToPTransformerUtil::connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                           vector<shared_ptr<PhysicalOp>> &consumers) {
  if (producers.size() != consumers.size()) {
    throw runtime_error(fmt::format("Bad one-to-one operator connection input, producers has {}, but consumers has {}",
                                    producers.size(), consumers.size()));
  }
  for (size_t i = 0; i < producers.size(); ++i) {
    producers[i]->produce(consumers[i]);
    consumers[i]->consume(producers[i]);
  }
}

void PrePToPTransformerUtil::connectOneToOne(shared_ptr<PhysicalOp> &producer,
                                             shared_ptr<PhysicalOp> &consumer) {
  producer->produce(consumer);
  consumer->consume(producer);
}

void PrePToPTransformerUtil::connectManyToOne(vector<shared_ptr<PhysicalOp>> &producers,
                                              shared_ptr<PhysicalOp> &consumer) {
  for (const auto &producer: producers) {
    producer->produce(consumer);
    consumer->consume(producer);
  }
}

void PrePToPTransformerUtil::connectOneToMany(shared_ptr<PhysicalOp> &producer,
                                              vector<shared_ptr<PhysicalOp>> &consumers) {
  for (const auto &consumer: consumers) {
    producer->produce(consumer);
    consumer->consume(producer);
  }
}

void PrePToPTransformerUtil::connectManyToMany(vector<shared_ptr<PhysicalOp>> &producers,
                             vector<shared_ptr<PhysicalOp>> &consumers) {
  for (const auto &producer: producers) {
    for (const auto &consumer: consumers) {
      producer->produce(consumer);
      consumer->consume(producer);
    }
  }
}

vector<shared_ptr<aggregate::AggregateFunction>>
PrePToPTransformerUtil::transformAggFunction(const string &outputColumnName,
                                         const shared_ptr<AggregatePrePFunction> &prePFunction,
                                         bool hasReduceOp) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return {make_shared<aggregate::Sum>(outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::COUNT: {
      return {make_shared<aggregate::Count>(outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::MIN: {
      return {make_shared<aggregate::MinMax>(true, outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::MAX: {
      return {make_shared<aggregate::MinMax>(false, outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::AVG: {
      if (hasReduceOp) {
        auto sumFunc = make_shared<aggregate::Sum>(
                AggregatePrePFunction::AVG_INTERMEDIATE_SUM_COLUMN_PREFIX + outputColumnName,
                prePFunction->getExpression());
        auto countFunc = make_shared<aggregate::Count>(
                AggregatePrePFunction::AVG_INTERMEDIATE_COUNT_COLUMN_PREFIX + outputColumnName,
                prePFunction->getExpression());
        return {sumFunc, countFunc};
      } else {
        return {make_shared<aggregate::Avg>(outputColumnName, prePFunction->getExpression())};
      }
    }
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type: {}", prePFunction->getTypeString()));
    }
  }
}

shared_ptr<aggregate::AggregateFunction>
PrePToPTransformerUtil::transformAggReduceFunction(const string &outputColumnName,
                                               const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM:
    case plan::prephysical::COUNT: {
      return make_shared<aggregate::Sum>(outputColumnName,
                                         fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::MIN: {
      return make_shared<aggregate::MinMax>(true,
                                            outputColumnName,
                                            fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::MAX: {
      return make_shared<aggregate::MinMax>(false,
                                            outputColumnName,
                                            fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::AVG: {
      return make_shared<aggregate::AvgReduce>(outputColumnName,nullptr);
    }
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type for parallel execution: {}", prePFunction->getTypeString()));
    }
  }
}

shared_ptr<PhysicalPlan> PrePToPTransformerUtil::rootOpToPlan(const shared_ptr<PhysicalOp> &rootOp,
                                                              const unordered_map<string, shared_ptr<PhysicalOp>> &opMap) {
  // collect operators in the subtree of the rootOp
  unordered_map<string, shared_ptr<PhysicalOp>> ops;
  queue<shared_ptr<PhysicalOp>> pendOpQueue;
  ops.emplace(rootOp->name(), rootOp);
  pendOpQueue.push(rootOp);

  while (!pendOpQueue.empty()) {
    auto op = pendOpQueue.front();
    for (const auto &producerName: op->producers()) {
      auto producerIt = opMap.find(producerName);
      if (producerIt == opMap.end()) {
        throw runtime_error(fmt::format("Producer '{}' not found in opMap when making physical plan from root op",
                                        producerName));
      }
      auto producer = producerIt->second;
      ops.emplace(producer->name(), producer);
      pendOpQueue.push(producer);
    }
    pendOpQueue.pop();
  }

  return make_shared<PhysicalPlan>(ops, rootOp->name());
}

pair<shared_ptr<PhysicalPlan>, std::string> PrePToPTransformerUtil::rootOpToPlanAndHost(
        const shared_ptr<PhysicalOp> &rootOp,
        const unordered_map<string, shared_ptr<PhysicalOp>> &opMap,
        const unordered_map<std::string, std::string> &objectToHost) {
  // collect operators in the subtree of the rootOp
  unordered_map<string, shared_ptr<PhysicalOp>> ops;
  std::optional<std::string> host = std::nullopt;
  queue<shared_ptr<PhysicalOp>> pendOpQueue;
  ops.emplace(rootOp->name(), rootOp);
  pendOpQueue.push(rootOp);

  while (!pendOpQueue.empty()) {
    auto op = pendOpQueue.front();
    if (op->getType() == POpType::FPDB_STORE_FILE_SCAN) {
      auto typedOp = static_pointer_cast<fpdb_store::FPDBStoreFileScanPOp>(op);
      auto objectIt = objectToHost.find(typedOp->getObject());
      if (objectIt != objectToHost.end()) {
        host = objectIt->second;
      }
    }
    for (const auto &producerName: op->producers()) {
      auto producerIt = opMap.find(producerName);
      if (producerIt == opMap.end()) {
        throw runtime_error(fmt::format("Producer '{}' not found in opMap when making physical plan from root op",
                                        producerName));
      }
      auto producer = producerIt->second;
      ops.emplace(producer->name(), producer);
      pendOpQueue.push(producer);
    }
    pendOpQueue.pop();
  }

  if (!host.has_value()) {
    throw runtime_error("Host of the sub-plan of the FPDBStoreSuperPOp is unknown");
  }
  return {make_shared<PhysicalPlan>(ops, rootOp->name()), *host};
}

void PrePToPTransformerUtil::updateOpToStoreNode(unordered_map<string, int> &opToStoreNode,
                                                 const shared_ptr<PhysicalOp> &rootOp,
                                                 const unordered_map<string, shared_ptr<PhysicalOp>> &opMap,
                                                 const unordered_map<std::string, std::string> &objectToHost,
                                                 const unordered_map<std::string, int> &hostToId) {
  auto planWithHost = rootOpToPlanAndHost(rootOp, opMap, objectToHost);
  auto hostId = hostToId.find(planWithHost.second)->second;
  for (const auto &op: planWithHost.first->getPhysicalOps()) {
    opToStoreNode[op.first] = hostId;
  }
}

void PrePToPTransformerUtil::addPhysicalOps(const vector<shared_ptr<PhysicalOp>> &newOps,
                                            unordered_map<string, shared_ptr<PhysicalOp>> &ops) {
  for (const auto &newOp: newOps) {
    if (ops.find(newOp->name()) != ops.end()) {
      throw runtime_error(fmt::format("Operator '{}' already exists when adding physical operators", newOp->name()));
    }
    ops.emplace(newOp->name(), newOp);
  }
}

unordered_map<string, int>
PrePToPTransformerUtil::getHostToNumOps(const vector<shared_ptr<PhysicalOp>> &fpdbStoreSuperPOps) {
  unordered_map<string, int> hostToNumOps;
  for (const auto &fpdbStoreSuperPOp: fpdbStoreSuperPOps) {
    if (fpdbStoreSuperPOp->getType() != POpType::FPDB_STORE_SUPER) {
      throw runtime_error("GetHostToNumOps should only be called with a vector of FPDBStoreSuperPOp");
    }
    auto host = static_pointer_cast<fpdb_store::FPDBStoreSuperPOp>(fpdbStoreSuperPOp)->getHost();
    hostToNumOps[host]++;
  }
  return hostToNumOps;
}
  
}
