//
// Created by Yifei Yang on 2/22/22.
//

#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <fpdb/executor/physical/aggregate/function/MinMax.h>
#include <fpdb/executor/physical/aggregate/function/Avg.h>
#include <fpdb/executor/physical/aggregate/function/AvgReduce.h>
#include <fpdb/executor/physical/aggregate/function/One.h>
#include <fpdb/executor/physical/aggregate/function/Stddev.h>
#include <fpdb/executor/physical/aggregate/function/StddevReduce.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/broadcast/BroadcastPOp.h>
#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/expression/gandiva/Multiply.h>
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

void PrePToPTransformerUtil::connectManyToOneByGroup(vector<vector<shared_ptr<PhysicalOp>>> &producers,
                                                     vector<shared_ptr<PhysicalOp>> &consumers) {
  if (producers.size() != consumers.size()) {
    throw std::runtime_error("Num groups not match between producers and consumers (many-one)");
  }
  for (uint i = 0; i < producers.size(); ++i) {
    connectManyToOne(producers[i], consumers[i]);
  }
}

void PrePToPTransformerUtil::connectOneToManyByGroup(vector<shared_ptr<PhysicalOp>> &producers,
                                                     vector<vector<shared_ptr<PhysicalOp>>> &consumers) {
  if (producers.size() != consumers.size()) {
    throw std::runtime_error("Num groups not match between producers and consumers (one-many)");
  }
  for (uint i = 0; i < producers.size(); ++i) {
    connectOneToMany(producers[i], consumers[i]);
  }
}

void PrePToPTransformerUtil::connectManyToManyByGroup(vector<vector<shared_ptr<PhysicalOp>>> &producers,
                                                      vector<vector<shared_ptr<PhysicalOp>>> &consumers) {
  if (producers.size() != consumers.size()) {
    throw std::runtime_error("Num groups not match between producers and consumers (many-many)");
  }
  for (uint i = 0; i < producers.size(); ++i) {
    connectManyToMany(producers[i], consumers[i]);
  }
}

vector<shared_ptr<aggregate::AggregateFunction>>
PrePToPTransformerUtil::transformAggFunction(const string &outputColumnName,
                                         const shared_ptr<AggregatePrePFunction> &prePFunction,
                                         bool hasReduceOp) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return {make_shared<aggregate::Sum>(outputColumnName, prePFunction->getExpression(), false)};
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
                prePFunction->getExpression(),
                false);
        auto countFunc = make_shared<aggregate::Count>(
                AggregatePrePFunction::AVG_INTERMEDIATE_COUNT_COLUMN_PREFIX + outputColumnName,
                prePFunction->getExpression());
        return {sumFunc, countFunc};
      } else {
        return {make_shared<aggregate::Avg>(outputColumnName, prePFunction->getExpression())};
      }
    }
    case plan::prephysical::ONE: {
      return {make_shared<aggregate::One>(outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::STDDEV_SAMP:
    case plan::prephysical::STDDEV_POP: {
      if (hasReduceOp) {
        const auto &expr = prePFunction->getExpression();
        auto sumFunc = make_shared<aggregate::Sum>(
                AggregatePrePFunction::STDDEV_INTERMEDIATE_SUM_COLUMN_PREFIX + outputColumnName,
                expr,
                false);
        auto countFunc = make_shared<aggregate::Count>(
                AggregatePrePFunction::STDDEV_INTERMEDIATE_COUNT_COLUMN_PREFIX + outputColumnName,
                expr);
        auto squareExpr = fpdb::expression::gandiva::times(expr, expr);
        auto sumOfSquaresFunc = make_shared<aggregate::Sum>(
                AggregatePrePFunction::STDDEV_INTERMEDIATE_SUM_OF_SQUARES_COLUMN_PREFIX + outputColumnName,
                squareExpr,
                false);
        return {sumFunc, countFunc, sumOfSquaresFunc};
      } else {
        aggregate::StddevType stddevType = prePFunction->getType() == plan::prephysical::STDDEV_SAMP ?
                                           aggregate::StddevType::SAMP : aggregate::StddevType::POP;
        return {make_shared<aggregate::Stddev>(stddevType, outputColumnName, prePFunction->getExpression())};
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
                                         fpdb::expression::gandiva::col(outputColumnName),
                                         prePFunction->getType() == plan::prephysical::COUNT);
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
      return make_shared<aggregate::AvgReduce>(outputColumnName, nullptr);
    }
    case plan::prephysical::ONE: {
      return make_shared<aggregate::One>(outputColumnName,
                                         fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::STDDEV_SAMP:
    case plan::prephysical::STDDEV_POP: {
      aggregate::StddevType stddevType = prePFunction->getType() == plan::prephysical::STDDEV_SAMP ?
                                          aggregate::StddevType::SAMP : aggregate::StddevType::POP;
      return make_shared<aggregate::StddevReduce>(stddevType, outputColumnName, nullptr);
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

vector<vector<shared_ptr<PhysicalOp>>>
PrePToPTransformerUtil::groupByNodeId(const vector<shared_ptr<PhysicalOp>> &ops,
                                      int numNodes) {
  vector<vector<shared_ptr<PhysicalOp>>> res(numNodes, vector<shared_ptr<PhysicalOp>>{});
  for (const auto &op: ops) {
    res[op->getNodeId()].emplace_back(op);
  }
  return res;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformerUtil::unGroupByNodeId(const vector<vector<shared_ptr<PhysicalOp>>> &ops) {
  vector<shared_ptr<PhysicalOp>> res;
  for (const auto &opVec: ops) {
    res.insert(res.end(), opVec.begin(), opVec.end());
  }
  return res;
}

bool PrePToPTransformerUtil::spreadTableToAllNodes(uint prePOpId,
                                                   unordered_map<string, shared_ptr<PhysicalOp>> &ops,
                                                   vector<vector<shared_ptr<PhysicalOp>>> &upConn,
                                                   vector<vector<shared_ptr<PhysicalOp>>> &res) {
  // no need for single-node exec
  uint numNodes = upConn.size();
  if (numNodes == 1) {
    return false;
  }

  // divide nodes into two parts, with and without table data
  vector<uint> withTableNodes, noTableNodes;
  for (uint i = 0; i < numNodes; ++i) {
    if (upConn[i].empty()) {
      noTableNodes.push_back(i);
    } else {
      withTableNodes.push_back(i);
    }
  }

  // no need to spread if all nodes have table data
  if (noTableNodes.empty()) {
    return false;
  }

  // loop to feed on "noTableNodes"
  res.resize(numNodes);
  vector<shared_ptr<PhysicalOp>> newOps;
  uint numWithTableNodes = withTableNodes.size();
  uint posInWithTableNodes = 0;
  unordered_map<uint, vector<shared_ptr<PhysicalOp>>> fromNodeSplits;       // split ops in "fromNode"
  for (uint toNode: noTableNodes) {
    // circular traversal of "withTableNodes"
    uint fromNode = withTableNodes[posInWithTableNodes++ % numWithTableNodes];
    // make split and broadcast ops for fromNode if not yet
    vector<shared_ptr<PhysicalOp>> split;
    auto fromIt = fromNodeSplits.find(fromNode);
    if (fromIt != fromNodeSplits.end()) {
      split = fromIt->second;
    } else {
      // make split
      for (const auto &upConnOp: upConn[fromNode]) {
        split.emplace_back(make_shared<split::SplitPOp>(
                fmt::format("Split[{}]-<node{}>-{}", prePOpId, fromNode, upConnOp->name()),
                vector<string>{}, /* unused */
                fromNode));
      }
      fromNodeSplits[fromNode] = split;
      connectOneToOne(upConn[fromNode], split);
      newOps.insert(newOps.end(), split.begin(), split.end());
      // make broadcast ops for fromNode itself (here each broadcast op simply forwards table to a single consumer)
      vector<shared_ptr<PhysicalOp>> broadcast;
      for (const auto &upConnOp: upConn[fromNode]) {
        broadcast.emplace_back(make_shared<broadcast::BroadcastPOp>(
                fmt::format("Broadcast[{}]-<node{}->{}>-{}", prePOpId, fromNode, fromNode, upConnOp->name()),
                vector<string>{} /* unused */,
                fromNode));
      }
      connectOneToOne(split, broadcast);
      newOps.insert(newOps.end(), broadcast.begin(), broadcast.end());
      // put into res
      res[fromNode] = broadcast;
    }
    // make broadcast ops for toNode (here each broadcast op simply forwards table to a single consumer)
    vector<shared_ptr<PhysicalOp>> broadcast;
    for (const auto &upConnOp: upConn[fromNode]) {
      broadcast.emplace_back(make_shared<broadcast::BroadcastPOp>(
              fmt::format("Broadcast[{}]-<node{}->{}>-{}", prePOpId, fromNode, toNode, upConnOp->name()),
              vector<string>{} /* unused */,
              toNode));
    }
    connectOneToOne(split, broadcast);
    newOps.insert(newOps.end(), broadcast.begin(), broadcast.end());
    // put into res
    res[toNode] = broadcast;
  }
  
  // loop on "withTableNodes" that are not used as any "fromNode"
  for (uint node: withTableNodes) {
    if (fromNodeSplits.find(node) == fromNodeSplits.end()) {
      res[node] = upConn[node];
    }
  }

  // add new ops and return
  addPhysicalOps(newOps, ops);
  return true;
}
  
}
