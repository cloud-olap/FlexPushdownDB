//
// Created by Yifei Yang on 11/20/21.
//

#include <fpdb/executor/physical/transform/PrePToPTransformer.h>
#include <fpdb/executor/physical/transform/PrePToS3PTransformer.h>
#include <fpdb/executor/physical/transform/PrePToFPDBStorePTransformer.h>
#include <fpdb/executor/physical/transform/pred-trans/PrePToPTransformerForPredTrans.h>
#include <fpdb/executor/physical/sort/SortPOp.h>
#include <fpdb/executor/physical/limitsort/LimitSortPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/group/GroupPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinProbePOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/broadcast/BroadcastPOp.h>
#include <fpdb/executor/physical/exchange/BatchExchangePOp.h>
#include <fpdb/executor/physical/exchange/BatchExchangeRRForwardPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreShuffleBatchLoadPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreTableCacheLoadPOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/transform/StoreTransformTraits.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/executor/Executor.h>
#include <fpdb/plan/Globals.h>
#include <fpdb/plan/prephysical/Util.h>
#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntry.h>
#include <fpdb/catalogue/obj-store/s3/S3Connector.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>

namespace fpdb::executor::physical {

PrePToPTransformer::PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                       const shared_ptr<CatalogueEntry> &catalogueEntry,
                                       const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                       const shared_ptr<Mode> &mode,
                                       int parallelDegree,
                                       int numNodes,
                                       // used by adapt exec
                                       long queryId,
                                       bool isDistributed,
                                       void* executor) :
  prePhysicalPlan_(prePhysicalPlan),
  catalogueEntry_(catalogueEntry),
  objStoreConnector_(objStoreConnector),
  mode_(mode),
  parallelDegree_(parallelDegree),
  numNodes_(numNodes),
  queryId_(queryId),
  isDistributed_(isDistributed),
  executor_(executor) {}

shared_ptr<PhysicalPlan> PrePToPTransformer::transform(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                                       const shared_ptr<CatalogueEntry> &catalogueEntry,
                                                       const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                                       const shared_ptr<Mode> &mode,
                                                       int parallelDegree,
                                                       int numNodes,
                                                       // used by adapt exec
                                                       long queryId,
                                                       bool isDistributed,
                                                       void* executor) {
  auto transformer = ENABLE_PRED_TRANS ?
          std::make_shared<PrePToPTransformerForPredTrans>(
                  prePhysicalPlan, catalogueEntry, objStoreConnector, mode,
                  parallelDegree, numNodes, queryId, isDistributed, executor) :
          std::make_shared<PrePToPTransformer>(
                  prePhysicalPlan, catalogueEntry, objStoreConnector, mode,
                  parallelDegree, numNodes, queryId, isDistributed, executor);
  const auto &physicalPlan = transformer->transform();

  // show dist-join types if needed
  transformer->showAdaptSelectedDistJoinTypes();

  return physicalPlan;
}

shared_ptr<PhysicalPlan> PrePToPTransformer::transform() {
  // transform from root in dfs
  auto upConnPOps = transformDfs(prePhysicalPlan_->getRootOp());

  // make a collate operator
  shared_ptr<PhysicalOp> collatePOp = make_shared<collate::CollatePOp>(
          "Collate",
          ColumnName::canonicalize(prePhysicalPlan_->getOutputColumnNames()),
          0);
  PrePToPTransformerUtil::connectManyToOne(upConnPOps, collatePOp);
  PrePToPTransformerUtil::addPhysicalOps({collatePOp}, physicalOps_);

  return make_shared<PhysicalPlan>(physicalOps_, collatePOp->name());
}

const shared_ptr<PrePhysicalPlan> &PrePToPTransformer::getPrePhysicalPlan() const {
  return prePhysicalPlan_;
}

vector<shared_ptr<PhysicalOp>> PrePToPTransformer::transformDfs(const shared_ptr<PrePhysicalOp> &prePOp) {
  switch (prePOp->getType()) {
    case PrePOpType::SORT: {
      const auto &sortPrePOp = std::static_pointer_cast<SortPrePOp>(prePOp);
      return transformSort(sortPrePOp);
    }
    case PrePOpType::LIMIT_SORT: {
      const auto &limitSortPrePOp = std::static_pointer_cast<LimitSortPrePOp>(prePOp);
      return transformLimitSort(limitSortPrePOp);
    }
    case PrePOpType::AGGREGATE: {
      const auto &aggregatePrePOp = std::static_pointer_cast<AggregatePrePOp>(prePOp);
      return transformAggregate(aggregatePrePOp);
    }
    case PrePOpType::GROUP: {
      const auto &groupPrePOp = std::static_pointer_cast<GroupPrePOp>(prePOp);
      return transformGroup(groupPrePOp);
    }
    case PrePOpType::PROJECT: {
      const auto &projectPrePOp = std::static_pointer_cast<ProjectPrePOp>(prePOp);
      return transformProject(projectPrePOp);
    }
    case PrePOpType::FILTER: {
      const auto &filterPrePOp = std::static_pointer_cast<FilterPrePOp>(prePOp);
      return transformFilter(filterPrePOp);
    }
    case PrePOpType::HASH_JOIN: {
      const auto &hashJoinPrePOp = std::static_pointer_cast<HashJoinPrePOp>(prePOp);
      return transformHashJoin(hashJoinPrePOp);
    }
    case PrePOpType::NESTED_LOOP_JOIN: {
      const auto &nestedLoopJoinPrePOp = std::static_pointer_cast<NestedLoopJoinPrePOp>(prePOp);
      return transformNestedLoopJoin(nestedLoopJoinPrePOp);
    }
    case PrePOpType::FILTERABLE_SCAN: {
      const auto &filterableScanPrePOp = std::static_pointer_cast<FilterableScanPrePOp>(prePOp);
      return transformFilterableScan(filterableScanPrePOp);
    }
    case PrePOpType::UNION_ALL: {
      const auto &unionAllPrePOp = std::static_pointer_cast<UnionAllPrePOp>(prePOp);
      return transformUnionAll(unionAllPrePOp);
    }
    case PrePOpType::SEPARABLE_SUPER: {
      const auto &separableSuperPrePOp = std::static_pointer_cast<SeparableSuperPrePOp>(prePOp);
      const auto &transRes = transformSeparableSuper(separableSuperPrePOp);

#if SHOW_DEBUG_METRICS == true
    // collect predicate transfer baseline metrics
    if (type_ == PrePToPTransformerType::REGULAR) {
      for (const auto &op: transRes) {
        op->setCollPredTransMetrics({true, prePOp->getId(),
                                     plan::prephysical::Util::getBaseTableDigest(prePOp),
                                     metrics::PredTransMetrics::PTMetricsUnitType::LOCAL_FILTER});
      }
      prePOpIdToConnOpsForPredTrans_[prePOp->getId()] = transRes;
    }
#endif

      return transRes;
    }
    default: {
      throw runtime_error(fmt::format("Unsupported prephysical operator type: {}", prePOp->getTypeString()));
    }
  }
}

vector<vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformProducers(const shared_ptr<PrePhysicalOp> &prePOp) {
  vector<vector<shared_ptr<PhysicalOp>>> transformRes;
  for (const auto &producer: prePOp->getProducers()) {
    transformRes.emplace_back(transformDfs(producer));
  }
  return transformRes;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformSort(const shared_ptr<SortPrePOp> &sortPrePOp) {
  // id
  auto prePOpId = sortPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(sortPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for sort, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<string> projectColumnNames{sortPrePOp->getProjectColumnNames().begin(),
                                    sortPrePOp->getProjectColumnNames().end()};

  shared_ptr<PhysicalOp> sortPOp = make_shared<sort::SortPOp>(fmt::format("Sort[{}]", prePOpId),
                                                              projectColumnNames,
                                                              rand() % numNodes_,
                                                              sortPrePOp->getSortKeys());

  // connect to upstream
  PrePToPTransformerUtil::connectManyToOne(upConnPOps, sortPOp);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps({sortPOp}, physicalOps_);
  return {sortPOp};
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformLimitSort(const shared_ptr<LimitSortPrePOp> &limitSortPrePOp) {
  // id
  auto prePOpId = limitSortPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(limitSortPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for limitSort, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<string> projectColumnNames{limitSortPrePOp->getProjectColumnNames().begin(),
                                    limitSortPrePOp->getProjectColumnNames().end()};

  shared_ptr<PhysicalOp> limitSortPOp = make_shared<limitsort::LimitSortPOp>(
          fmt::format("LimitSort[{}]", prePOpId),
          projectColumnNames,
          rand() % numNodes_,
          limitSortPrePOp->getK(),
          limitSortPrePOp->getSortKeys());

  // connect to upstream
  PrePToPTransformerUtil::connectManyToOne(upConnPOps, limitSortPOp);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps({limitSortPOp}, physicalOps_);
  return {limitSortPOp};
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp) {
  // id
  auto prePOpId = aggregatePrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(aggregatePrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for aggregate, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  bool isParallel = upConnPOps.size() > 1;
  vector<shared_ptr<PhysicalOp>> allPOps, selfConnUpPOps, selfConnDownPOps;
  vector<string> projectColumnNames{aggregatePrePOp->getProjectColumnNames().begin(),
                                    aggregatePrePOp->getProjectColumnNames().end()};
  vector<string> parallelAggProjectColumnNames;
  if (isParallel) {
    for (uint i = 0; i < aggregatePrePOp->getFunctions().size(); ++i) {
      const auto prePFunction = aggregatePrePOp->getFunctions()[i];
      const auto aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[i];
      if (prePFunction->getType() == AggregatePrePFunctionType::AVG) {
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_INTERMEDIATE_SUM_COLUMN_PREFIX + aggOutputColumnName);
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_INTERMEDIATE_COUNT_COLUMN_PREFIX + aggOutputColumnName);
      } else if (prePFunction->getType() == AggregatePrePFunctionType::STDDEV_SAMP ||
                 prePFunction->getType() == AggregatePrePFunctionType::STDDEV_POP) {
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::STDDEV_INTERMEDIATE_SUM_COLUMN_PREFIX + aggOutputColumnName);
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::STDDEV_INTERMEDIATE_COUNT_COLUMN_PREFIX + aggOutputColumnName);
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::STDDEV_INTERMEDIATE_SUM_OF_SQUARES_COLUMN_PREFIX + aggOutputColumnName);
      } else {
        parallelAggProjectColumnNames.emplace_back(aggOutputColumnName);
      }
    }
  } else {
    parallelAggProjectColumnNames = projectColumnNames;
  }

  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t j = 0; j < aggregatePrePOp->getFunctions().size(); ++j) {
      const auto &prePFunction = aggregatePrePOp->getFunctions()[j];
      const auto &aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[j];
      const auto &transAggFunctions = PrePToPTransformerUtil::transformAggFunction(aggOutputColumnName,
                                                                                   prePFunction,
                                                                                   isParallel);
      aggFunctions.insert(aggFunctions.end(), transAggFunctions.begin(), transAggFunctions.end());
    }

    allPOps.emplace_back(make_shared<aggregate::AggregatePOp>(
            fmt::format("Aggregate[{}]-{}", prePOpId, i),
            parallelAggProjectColumnNames,
            upConnPOps[i]->getNodeId(),
            aggFunctions,
            upConnPOps.size() == 1 ? true : false));
  }

  // if num > 1, then we need an aggregate reduce operator
  if (upConnPOps.size() == 1) {
    selfConnUpPOps = allPOps;
    selfConnDownPOps = allPOps;
  } else {
    // aggregate reduce functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggReduceFunctions;
    for (size_t j = 0; j < aggregatePrePOp->getFunctions().size(); ++j) {
      const auto &prePFunction = aggregatePrePOp->getFunctions()[j];
      const auto &aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[j];
      aggReduceFunctions.emplace_back(PrePToPTransformerUtil::transformAggReduceFunction(aggOutputColumnName,
                                                                                         prePFunction));
    }

    shared_ptr<PhysicalOp> aggReducePOp = make_shared<aggregate::AggregatePOp>(
            fmt::format("Aggregate[{}]-Reduce", prePOpId),
            projectColumnNames,
            rand() % numNodes_,
            aggReduceFunctions,
            true);
    PrePToPTransformerUtil::connectManyToOne(allPOps, aggReducePOp);
    selfConnUpPOps = allPOps;
    selfConnDownPOps.emplace_back(aggReducePOp);
    allPOps.emplace_back(aggReducePOp);
  }

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, selfConnUpPOps);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return selfConnDownPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformGroup(const shared_ptr<GroupPrePOp> &groupPrePOp) {
  vector<shared_ptr<PhysicalOp>> transRes;
  if (USE_TWO_PHASE_GROUP_BY) {
    transRes = transformGroupTwoPhase(groupPrePOp);
  } else {
    transRes = transformGroupOnePhase(groupPrePOp);
  }

#if SHOW_DEBUG_METRICS == true
  // update ops to collect predicate transfer baseline metrics
  if (type_ == PrePToPTransformerType::REGULAR) {
    auto optPrePOp = prephysical::Util::traceScanOriginWithNoJoinInPath(groupPrePOp);
    if (optPrePOp != nullptr) {
      // remove flag for former ops
      uint prePOpId = optPrePOp->getId();
      auto transResIt = prePOpIdToConnOpsForPredTrans_.find(prePOpId);
      if (transResIt != prePOpIdToConnOpsForPredTrans_.end()) {
        for (const auto &op: transResIt->second) {
          op->unsetCollPredTransMetrics();
        }
      }

      // set flag for latter ops
      for (const auto &op: transRes) {
        op->setCollPredTransMetrics({true, prePOpId,
                                     plan::prephysical::Util::getBaseTableDigest(groupPrePOp),
                                     metrics::PredTransMetrics::PTMetricsUnitType::LOCAL_FILTER});
      }
    }
  }
#endif

  return transRes;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformGroupOnePhase(const shared_ptr<GroupPrePOp> &groupPrePOp) {
  // id
  auto prePOpId = groupPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(groupPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for group, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps, groupPOps;
  vector<string> projectColumnNames{groupPrePOp->getProjectColumnNames().begin(),
                                    groupPrePOp->getProjectColumnNames().end()};
  for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
      const auto &prePFunction = groupPrePOp->getFunctions()[j];
      const auto &aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[j];
      const auto &transAggFunctions = PrePToPTransformerUtil::transformAggFunction(aggOutputColumnName,
                                                                                   prePFunction,
                                                                                   false);
      aggFunctions.insert(aggFunctions.end(), transAggFunctions.begin(), transAggFunctions.end());
    }

    groupPOps.emplace_back(make_shared<group::GroupPOp>(fmt::format("Group[{}]-{}", prePOpId, i),
                                                        projectColumnNames,
                                                        i % numNodes_,
                                                        groupPrePOp->getGroupColumnNames(),
                                                        aggFunctions));
  }
  allPOps.insert(allPOps.end(), groupPOps.begin(), groupPOps.end());

  // if num > 1, then we add a shuffle stage ahead
  if (parallelDegree_ * numNodes_ == 1) {
    // connect to upstream
    PrePToPTransformerUtil::connectManyToOne(upConnPOps, groupPOps[0]);
  } else {
    vector<shared_ptr<PhysicalOp>> shufflePOps;
    for (const auto &upConnPOp: upConnPOps) {
      shufflePOps.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, upConnPOp->name()),
              upConnPOp->getProjectColumnNames(),
              upConnPOp->getNodeId(),
              groupPrePOp->getGroupColumnNames()));
    }

    // check if we need to push shuffle to store
    bool withHashJoinPushdown = false;
    if (catalogueEntry_->getType() == CatalogueEntryType::OBJ_STORE &&
        std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_)->getStoreType() ==
          obj_store::ObjStoreType::FPDB_STORE) {
      const auto &shuffleAddRes =
              PrePToFPDBStorePTransformer::addSeparablePOp(upConnPOps, shufflePOps, mode_, &physicalOps_,
                                                           groupPrePOp->getProducers()[0]->getId(),
                                                           numNodes_, parallelDegree_);
      auto opsForShuffle = std::get<0>(shuffleAddRes);
      auto addiOps = std::get<1>(shuffleAddRes);
      withHashJoinPushdown = std::get<2>(shuffleAddRes);
      allPOps.insert(allPOps.end(), addiOps.begin(), addiOps.end());
      upConnPOps = opsForShuffle;
    } else {
      allPOps.insert(allPOps.end(), shufflePOps.begin(), shufflePOps.end());
      // connect to upstream
      PrePToPTransformerUtil::connectOneToOne(upConnPOps, shufflePOps);
      upConnPOps = shufflePOps;
    }
    bool shufflePushed = upConnPOps[0]->getType() != POpType::SHUFFLE;

    // enable batch exchange in dist exec, only do this when pushdown is disabled
    bool useDistShuffleBatchExchange = ENABLE_DIST_SHUFFLE_BATCH_EXCHANGE &&
                                       numNodes_ > 1 &&
                                       (mode_->id() == ModeId::PULL_UP || mode_->id() == ModeId::CACHING_ONLY);
    vector<POpVec> finalShuffles;
    if (useDistShuffleBatchExchange) {
      finalShuffles = batchExchangeShuffle(shufflePOps, prePOpId, "<group>", allPOps, nullptr);
    }

    // connect to downstream
    if (withHashJoinPushdown && shufflePushed) {

      PrePToPTransformerUtil::connectOneToOne(upConnPOps, groupPOps);
    } else {
      if (useDistShuffleBatchExchange) {
        auto groupOpsByNode = PrePToPTransformerUtil::groupByNodeId(groupPOps, numNodes_);
        PrePToPTransformerUtil::connectManyToManyByGroup(finalShuffles, groupOpsByNode);
      } else {
        PrePToPTransformerUtil::connectManyToMany(upConnPOps, groupPOps);
      }
    }

    // shuffle batch load
    if (shufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
      batchLoadShufflePushdown(upConnPOps, groupPOps, allPOps);
    }
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return groupPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformGroupTwoPhase(const shared_ptr<GroupPrePOp> &groupPrePOp) {
  // id
  auto prePOpId = groupPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(groupPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for group, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];
  bool isParallel = upConnPOps.size() > 1;

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps, selfConnDownPOps;;
  vector<string> finalProjectColumnNames{groupPrePOp->getProjectColumnNames().begin(),
                                         groupPrePOp->getProjectColumnNames().end()};
  vector<string> parallelGroupProjectColumnNames;
  if (isParallel) {
    parallelGroupProjectColumnNames.insert(parallelGroupProjectColumnNames.end(),
                                           groupPrePOp->getGroupColumnNames().begin(),
                                           groupPrePOp->getGroupColumnNames().end());
    for (uint i = 0; i < groupPrePOp->getFunctions().size(); ++i) {
      const auto prePFunction = groupPrePOp->getFunctions()[i];
      const auto aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[i];
      if (prePFunction->getType() == AggregatePrePFunctionType::AVG) {
        parallelGroupProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_INTERMEDIATE_SUM_COLUMN_PREFIX + aggOutputColumnName);
        parallelGroupProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_INTERMEDIATE_COUNT_COLUMN_PREFIX + aggOutputColumnName);
      } else if (prePFunction->getType() == AggregatePrePFunctionType::STDDEV_SAMP ||
                 prePFunction->getType() == AggregatePrePFunctionType::STDDEV_POP) {
        parallelGroupProjectColumnNames.emplace_back(AggregatePrePFunction::STDDEV_INTERMEDIATE_SUM_COLUMN_PREFIX + aggOutputColumnName);
        parallelGroupProjectColumnNames.emplace_back(AggregatePrePFunction::STDDEV_INTERMEDIATE_COUNT_COLUMN_PREFIX + aggOutputColumnName);
        parallelGroupProjectColumnNames.emplace_back(AggregatePrePFunction::STDDEV_INTERMEDIATE_SUM_OF_SQUARES_COLUMN_PREFIX + aggOutputColumnName);
      } else {
        parallelGroupProjectColumnNames.emplace_back(aggOutputColumnName);
      }
    }
  } else {
    parallelGroupProjectColumnNames = finalProjectColumnNames;
  }

  // phase 1: parallel group ops
  vector<shared_ptr<PhysicalOp>> groupPOps;
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
      const auto &prePFunction = groupPrePOp->getFunctions()[j];
      const auto &aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[j];
      const auto &transAggFunctions = PrePToPTransformerUtil::transformAggFunction(aggOutputColumnName,
                                                                                   prePFunction,
                                                                                   isParallel);
      aggFunctions.insert(aggFunctions.end(), transAggFunctions.begin(), transAggFunctions.end());
    }

    groupPOps.emplace_back(make_shared<group::GroupPOp>(fmt::format("Group[{}]-{}", prePOpId, i),
                                                        parallelGroupProjectColumnNames,
                                                        upConnPOps[i]->getNodeId(),
                                                        groupPrePOp->getGroupColumnNames(),
                                                        aggFunctions));
  }

  // check if we need to push phase 1 to store
  if (catalogueEntry_->getType() == CatalogueEntryType::OBJ_STORE &&
      std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_)->getStoreType() ==
        obj_store::ObjStoreType::FPDB_STORE) {
    const auto &groupAddRes = PrePToFPDBStorePTransformer::addSeparablePOp(upConnPOps, groupPOps, mode_, &physicalOps_);
    auto opsForGroup = std::get<0>(groupAddRes);
    auto addiOps = std::get<1>(groupAddRes);
    allPOps.insert(allPOps.end(), addiOps.begin(), addiOps.end());
    upConnPOps = opsForGroup;
  } else {
    allPOps.insert(allPOps.end(), groupPOps.begin(), groupPOps.end());
    // connect to upstream
    PrePToPTransformerUtil::connectOneToOne(upConnPOps, groupPOps);
    upConnPOps = groupPOps;
  }

  // if num > 1, then we need make a shuffle stage after parallel group ops and then add parallel group reduce ops
  if (!isParallel) {
    selfConnDownPOps = upConnPOps;
  } else {
    // shuffle ops
    vector<shared_ptr<PhysicalOp>> shufflePOps;
    shufflePOps.reserve(upConnPOps.size());
    for (const auto &upConnPOp: upConnPOps) {
      shufflePOps.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, upConnPOp->name()),
              upConnPOp->getProjectColumnNames(),
              upConnPOp->getNodeId(),
              groupPrePOp->getGroupColumnNames()));
    }

    // check if we need to push shuffle to store
    bool withHashJoinPushdown = false;
    if (catalogueEntry_->getType() == CatalogueEntryType::OBJ_STORE &&
        std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_)->getStoreType() ==
        obj_store::ObjStoreType::FPDB_STORE) {
      const auto &shuffleAddRes =
              PrePToFPDBStorePTransformer::addSeparablePOp(upConnPOps, shufflePOps, mode_, &physicalOps_,
                                                           groupPrePOp->getProducers()[0]->getId(),
                                                           numNodes_, parallelDegree_);
      auto opsForShuffle = std::get<0>(shuffleAddRes);
      auto addiOps = std::get<1>(shuffleAddRes);
      withHashJoinPushdown = std::get<2>(shuffleAddRes);
      allPOps.insert(allPOps.end(), addiOps.begin(), addiOps.end());
      upConnPOps = opsForShuffle;
    } else {
      allPOps.insert(allPOps.end(), shufflePOps.begin(), shufflePOps.end());
      // connect to upstream
      PrePToPTransformerUtil::connectOneToOne(upConnPOps, shufflePOps);
      upConnPOps = shufflePOps;
    }
    bool shufflePushed = upConnPOps[0]->getType() != POpType::SHUFFLE;

    // enable batch exchange in dist exec, only do this when pushdown is disabled
    bool useDistShuffleBatchExchange = ENABLE_DIST_SHUFFLE_BATCH_EXCHANGE &&
                                       numNodes_ > 1 &&
                                       (mode_->id() == ModeId::PULL_UP || mode_->id() == ModeId::CACHING_ONLY);
    vector<POpVec> finalShuffles;
    if (useDistShuffleBatchExchange) {
      finalShuffles = batchExchangeShuffle(shufflePOps, prePOpId, "<group>", allPOps, nullptr);
    }

    // phase 2: parallel group reduce ops
    vector<shared_ptr<PhysicalOp>> groupReducePOps;
    groupReducePOps.reserve(parallelDegree_ * numNodes_);
    for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
      // aggregate reduce functions, better to let each operator has its own copy of aggregate functions
      vector<shared_ptr<aggregate::AggregateFunction>> aggReduceFunctions;
      for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
        const auto &prePFunction = groupPrePOp->getFunctions()[j];
        const auto &aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[j];
        aggReduceFunctions.emplace_back(PrePToPTransformerUtil::transformAggReduceFunction(aggOutputColumnName,
                                                                                           prePFunction));
      }

      groupReducePOps.emplace_back(make_shared<group::GroupPOp>(
              fmt::format("Group[{}]-reduce-{}", prePOpId, i),
              finalProjectColumnNames,
              i % numNodes_,
              groupPrePOp->getGroupColumnNames(),
              aggReduceFunctions));
    }
    allPOps.insert(allPOps.end(), groupReducePOps.begin(), groupReducePOps.end());
    if (withHashJoinPushdown && shufflePushed) {
      PrePToPTransformerUtil::connectOneToOne(upConnPOps, groupReducePOps);
    } else {
      if (useDistShuffleBatchExchange) {
        auto groupReduceByNode = PrePToPTransformerUtil::groupByNodeId(groupReducePOps, numNodes_);
        PrePToPTransformerUtil::connectManyToManyByGroup(finalShuffles, groupReduceByNode);
      } else {
        PrePToPTransformerUtil::connectManyToMany(upConnPOps, groupReducePOps);
      }
    }

    // shuffle batch load
    if (shufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
      batchLoadShufflePushdown(upConnPOps, groupReducePOps, allPOps);
    }

    selfConnDownPOps = groupReducePOps;
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return selfConnDownPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp) {
  // id
  auto prePOpId = projectPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(projectPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for project, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps;
  vector<string> projectColumnNames{projectPrePOp->getProjectColumnNames().begin(),
                                    projectPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    allPOps.emplace_back(make_shared<project::ProjectPOp>(fmt::format("Project[{}]-{}", prePOpId, i),
                                                          projectColumnNames,
                                                          upConnPOps[i]->getNodeId(),
                                                          projectPrePOp->getExprs(),
                                                          projectPrePOp->getExprNames(),
                                                          projectPrePOp->getProjectColumnNamePairs()));
  }

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, allPOps);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return allPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp) {
  // id
  auto prePOpId = filterPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(filterPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for filter, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps;
  vector<string> projectColumnNames{filterPrePOp->getProjectColumnNames().begin(),
                                    filterPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    allPOps.emplace_back(make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}", prePOpId, i),
                                                        projectColumnNames,
                                                        upConnPOps[i]->getNodeId(),
                                                        filterPrePOp->getPredicate(),
                                                        nullptr));
  }

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, allPOps);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return allPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformHashJoin(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp) {
  // id
  auto prePOpId = hashJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(hashJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for hashJoin, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }
  auto upLeftConnPOps = producersTransRes[0];
  auto upRightConnPOps = producersTransRes[1];

  // get (estimated) row count of both input tables, use runtime card when applicable
  const auto &leftPrePOp = hashJoinPrePOp->getProducers()[0];
  const auto &rightPrePOp = hashJoinPrePOp->getProducers()[1];
  int64_t leftRowCount = leftPrePOp->getRowCount();
  int64_t rightRowCount = rightPrePOp->getRowCount();
  bool recordCard = false, recordAdaptSelectedDistJoinType = false;
  if (DIST_JOIN_TYPE == join::DistJoinType::COST_BASED_ADAPT) {
    if (executor_ == nullptr) {
      throw std::runtime_error("Executor not set during adaptive dist-join.");
    }
    if (USE_DOUBLE_EXEC_ADAPT) {
      cache::OutputCardCache::OutputCardKey leftCardKey(leftPrePOp->getId());
      cache::OutputCardCache::OutputCardKey rightCardKey(rightPrePOp->getId());
      auto expLeftCard = ((Executor*)(executor_))->cardCache_.consume(leftCardKey);
      auto expRightCard = ((Executor*)(executor_))->cardCache_.consume(rightCardKey);
      if (expLeftCard.has_value() && expRightCard.has_value()) {
        // this run has card to consume
        leftRowCount = *expLeftCard;
        rightRowCount = *expRightCard;
        recordAdaptSelectedDistJoinType = metrics::SHOW_DIST_JOIN_TYPE;
      } else {
        // this run should record card
        if (USE_ARROW_HASH_JOIN_IMPL) {
          recordCard = true;
        } else {
          throw std::runtime_error("Adaptive dist-join strategy selection with Vanilla hash-join is not supported");
        }
      }
    } else {
      throw std::runtime_error("Adaptive dist-join strategy selection without 'USE_DOUBLE_EXEC_ADAPT' is not supported");
    }
  }

  // we may need to switch left/right sides based on updated runtime cardinalities
  if (leftRowCount > rightRowCount) {
    hashJoinPrePOp->switchSide();
    std::swap(leftRowCount, rightRowCount);
    std::swap(upLeftConnPOps, upRightConnPOps);
  }

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps;
  vector<string> projectColumnNames{hashJoinPrePOp->getProjectColumnNames().begin(),
                                    hashJoinPrePOp->getProjectColumnNames().end()};
  auto joinType = hashJoinPrePOp->getJoinType();
  const auto &leftColumnNames = hashJoinPrePOp->getLeftColumnNames();
  const auto &rightColumnNames = hashJoinPrePOp->getRightColumnNames();
  join::HashJoinPredicate hashJoinPredicate(leftColumnNames, rightColumnNames);
  const auto &hashJoinPredicateStr = hashJoinPredicate.toString();

  // classify created ops into different phases regarding pred-trans
  POpVec ptPhaseOps, joinPhaseOps;

  // hash join operators, if using arrow's impl, we make hashJoinArrowPOps,
  // otherwise we make hashJoinBuildPOps and hashJoinProbePOps
  vector<shared_ptr<PhysicalOp>> hashJoinBuildPOps, hashJoinProbePOps;
  vector<shared_ptr<PhysicalOp>> hashJoinArrowPOps;
  if (USE_ARROW_HASH_JOIN_IMPL) {
    for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
      hashJoinArrowPOps.emplace_back(make_shared<join::HashJoinArrowPOp>(
              fmt::format("HashJoinArrow[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              projectColumnNames,
              i % numNodes_,
              hashJoinPredicate,
              joinType));
    }
    allPOps.insert(allPOps.end(), hashJoinArrowPOps.begin(), hashJoinArrowPOps.end());
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), hashJoinArrowPOps.begin(), hashJoinArrowPOps.end());
#endif
  } else {
    for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
      hashJoinBuildPOps.emplace_back(make_shared<join::HashJoinBuildPOp>(
              fmt::format("HashJoinBuild[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              projectColumnNames,
              i % numNodes_,
              leftColumnNames));
      hashJoinProbePOps.emplace_back(make_shared<join::HashJoinProbePOp>(
              fmt::format("HashJoinProbe[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              projectColumnNames,
              i % numNodes_,
              hashJoinPredicate,
              joinType));
    }
    allPOps.insert(allPOps.end(), hashJoinBuildPOps.begin(), hashJoinBuildPOps.end());
    allPOps.insert(allPOps.end(), hashJoinProbePOps.begin(), hashJoinProbePOps.end());
    PrePToPTransformerUtil::connectOneToOne(hashJoinBuildPOps, hashJoinProbePOps);
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), hashJoinBuildPOps.begin(), hashJoinBuildPOps.end());
    joinPhaseOps.insert(joinPhaseOps.end(), hashJoinProbePOps.begin(), hashJoinProbePOps.end());
#endif
  }

  // decide whether to use broadcast of reshuffled distributed join
  // always use partitioned join when recording cardinality
  bool useBcast = recordCard ? false : useBroadcastJoin(hashJoinPrePOp->getJoinType(), leftRowCount, rightRowCount);
  if (useBcast) {
    // broadcast distributed join
    transformHashJoinBcast(hashJoinPrePOp, hashJoinPredicateStr,
                           upLeftConnPOps, upRightConnPOps,
                           hashJoinArrowPOps, allPOps,
                           ptPhaseOps, joinPhaseOps, recordCard);
  } else {
    // partitioned distributed join
    transformHashJoinPtion(hashJoinPrePOp, hashJoinPredicateStr,
                           upLeftConnPOps, upRightConnPOps,
                           hashJoinArrowPOps, hashJoinBuildPOps, hashJoinProbePOps,
                           allPOps, ptPhaseOps, joinPhaseOps, recordCard);
  }

  // record selected dist-join type if need to show it
  if (recordAdaptSelectedDistJoinType) {
    adaptDistJoinTypes_.push_back({
      join::DistJoinTypeUtil::toJoinDigest(leftPrePOp->getId(), rightPrePOp->getId(), hashJoinPredicateStr),
      useBcast ? join::DistJoinType::BCAST : join::DistJoinType::PTION});
  }

#if SHOW_DEBUG_METRICS == true
  // classify created ops into phases regarding pred-trans
  for (const auto &op: ptPhaseOps) {
    op->setPTPhaseType(metrics::PredTransMetrics::PRED_TRANS_PHASE);
  }
  for (const auto &op: joinPhaseOps) {
    op->setPTPhaseType(metrics::PredTransMetrics::JOIN_PHASE);
  }
#endif

  // add the second part and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return USE_ARROW_HASH_JOIN_IMPL ? hashJoinArrowPOps : hashJoinProbePOps;
}

bool PrePToPTransformer::useBroadcastJoin(JoinType joinType, int64_t leftRowCount, int64_t rightRowCount) {
  // if using partitioned dist join
  // for single-node, there is no difference between broadcast and partitioned, here by default we call partitioned func
  if (DIST_JOIN_TYPE == join::DistJoinType::PTION || numNodes_ == 1) {
    return false;
  }

  // broadcast join cannot be used for left join, since unmatched records of the left table may have duplicates across
  // different nodes, similar for left semi join
  if (joinType == JoinType::LEFT || joinType == JoinType::FULL || joinType == JoinType::LEFT_SEMI) {
    return false;
  }

  // if using broadcast dist join
  if (DIST_JOIN_TYPE == join::DistJoinType::BCAST) {
    return true;
  }

  return join::DistJoinTypeUtil::optimize(leftRowCount, rightRowCount, numNodes_) == join::DistJoinType::BCAST;
}

void PrePToPTransformer::transformHashJoinBcast(
        const std::shared_ptr<HashJoinPrePOp> &hashJoinPrePOp, const std::string &hashJoinPredicateStr,
        POpVec &upLeftConnPOps, POpVec &upRightConnPOps,
        POpVec &hashJoinArrowPOps, POpVec &allPOps,
        POpVec &ptPhaseOps, POpVec &joinPhaseOps, bool recordCard) {
  // pushdown is currently not supported
  if (mode_->id() == ModeId::PUSHDOWN_ONLY || mode_->id() == ModeId::HYBRID) {
    throw std::runtime_error("Pushdown is not supported by broadcast distributed join");
  }
  // currently only support Arrow's hash-join impl.
  if (!USE_ARROW_HASH_JOIN_IMPL) {
    throw std::runtime_error("Only Arrow's hash-join implementation is supported by broadcast distributed join");
  }
  uint prePOpId = hashJoinPrePOp->getId();
  auto joinType = hashJoinPrePOp->getJoinType();
  const auto &leftColumnNames = hashJoinPrePOp->getLeftColumnNames();
  const auto &rightColumnNames = hashJoinPrePOp->getRightColumnNames();

  // group input (left and right) and output (hash join) ops by node
  auto upLeftConnByNode = PrePToPTransformerUtil::groupByNodeId(upLeftConnPOps, numNodes_);
  auto upRightConnByNode = PrePToPTransformerUtil::groupByNodeId(upRightConnPOps, numNodes_);
  auto hashJoinArrowByNode = PrePToPTransformerUtil::groupByNodeId(hashJoinArrowPOps, numNodes_);

  // spread right input to all nodes if not yet, to let all nodes participate bf/hj probe
  std::vector<POpVec> updatedUpRightConnByNode;
  if (PrePToPTransformerUtil::spreadTableToAllNodes(hashJoinPrePOp->getProducers()[1]->getId(),
                                                    physicalOps_, upRightConnByNode, updatedUpRightConnByNode)) {
    upRightConnByNode = updatedUpRightConnByNode;
  }

  // shuffle right within each node
  std::vector<POpVec> shuffleRight;
  for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
    POpVec shuffleRightSingleNode;
    for (auto &upRightConnPOp: upRightConnByNode[nodeId]) {
      shuffleRightSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, upRightConnPOp->name()),
              upRightConnPOp->getProjectColumnNames(),
              nodeId,
              rightColumnNames));
    }
    shuffleRight.emplace_back(shuffleRightSingleNode);
    allPOps.insert(allPOps.end(), shuffleRightSingleNode.begin(), shuffleRightSingleNode.end());
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), shuffleRightSingleNode.begin(), shuffleRightSingleNode.end());
#endif
    // connect upstream to shuffle
    PrePToPTransformerUtil::connectOneToOne(upRightConnByNode[nodeId], shuffleRightSingleNode);
  }
  
  // left
  struct {
    std::vector<POpVec> shuffleLeftWithBE_;     // used with batch exchange
    std::vector<POpVec> broadcastLeftNoBE_;     // used without batch exchange
  } shuffleLeftSt;
  if (ENABLE_DIST_BCAST_BATCH_EXCHANGE) {
    // first bcast left data with batch exchange, then shuffle within each node
    // bcast to collect source data
    POpVec broadcastLeft;
    for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
      if (upLeftConnByNode[nodeId].empty()) {
        // skip if no left input exists in this node
        continue;
      }
      std::shared_ptr<PhysicalOp> broadcast = std::make_shared<broadcast::BroadcastPOp>(
              fmt::format("Broadcast[{}]-{}-{}", prePOpId, hashJoinPredicateStr, nodeId),
              std::vector<std::string>{}, /*unused*/
              nodeId,
              true);
      broadcastLeft.emplace_back(broadcast);
      PrePToPTransformerUtil::connectManyToOne(upLeftConnByNode[nodeId], broadcast);
    }
    allPOps.insert(allPOps.end(), broadcastLeft.begin(), broadcastLeft.end());
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), broadcastLeft.begin(), broadcastLeft.end());
#endif
    // split to collect data from all bcast
    POpVec splitLeft;
    for (int nodeId = 0; nodeId < numNodes_; ++ nodeId) {
      splitLeft.emplace_back(std::make_shared<split::SplitPOp>(
              fmt::format("Split[{}]-{}-{}", prePOpId, hashJoinPredicateStr, nodeId),
              std::vector<std::string>{}, /*unused*/
              nodeId));
    }
    allPOps.insert(allPOps.end(), splitLeft.begin(), splitLeft.end());
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), splitLeft.begin(), splitLeft.end());
#endif
    PrePToPTransformerUtil::connectManyToMany(broadcastLeft, splitLeft);
    // shuffle within each node
    for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
      POpVec shuffleLeftSingleNode;
      for (int i = 0; i < parallelDegree_; ++i) {
        int opId = i * numNodes_ + nodeId;
        shuffleLeftSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
                fmt::format("Shuffle[{}]-{}-{}", prePOpId, hashJoinPredicateStr, opId),
                std::vector<std::string>{}, /*unused*/
                nodeId,
                leftColumnNames));
      }
      shuffleLeftSt.shuffleLeftWithBE_.emplace_back(shuffleLeftSingleNode);
      allPOps.insert(allPOps.end(), shuffleLeftSingleNode.begin(), shuffleLeftSingleNode.end());
#if SHOW_DEBUG_METRICS == true
      joinPhaseOps.insert(joinPhaseOps.end(), shuffleLeftSingleNode.begin(), shuffleLeftSingleNode.end());
#endif
      PrePToPTransformerUtil::connectOneToMany(splitLeft[nodeId], shuffleLeftSingleNode);
    }
  } else {
    // first shuffle the left data, then bcast each shuffled piece to all nodes
    std::vector<POpVec> shuffleLeft;
    for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
      POpVec shuffleLeftSingleNode;
      for (auto &upLeftConnPOp: upLeftConnByNode[nodeId]) {
        shuffleLeftSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
                fmt::format("Shuffle[{}]-{}", prePOpId, upLeftConnPOp->name()),
                upLeftConnPOp->getProjectColumnNames(),
                nodeId,
                leftColumnNames));
      }
      shuffleLeft.emplace_back(shuffleLeftSingleNode);
      allPOps.insert(allPOps.end(), shuffleLeftSingleNode.begin(), shuffleLeftSingleNode.end());
#if SHOW_DEBUG_METRICS == true
      joinPhaseOps.insert(joinPhaseOps.end(), shuffleLeftSingleNode.begin(), shuffleLeftSingleNode.end());
#endif
      // connect upstream to shuffle
      PrePToPTransformerUtil::connectOneToOne(upLeftConnByNode[nodeId], shuffleLeftSingleNode);
    }
    // create broadcast ops to receive results from shuffle left ops
    for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
      POpVec broadcastSingleNode;
      // skip if no left input exists in this node
      if (!shuffleLeft[nodeId].empty()) {
        for (int i = 0; i < parallelDegree_; ++i) {
          int opId = i * numNodes_ + nodeId;
          broadcastSingleNode.emplace_back(std::make_shared<broadcast::BroadcastPOp>(
                  fmt::format("Broadcast[{}]-{}-{}", prePOpId, hashJoinPredicateStr, opId),
                  std::vector<std::string>{}, /*unused*/
                  nodeId));
        }
      }
      allPOps.insert(allPOps.end(), broadcastSingleNode.begin(), broadcastSingleNode.end());
      shuffleLeftSt.broadcastLeftNoBE_.emplace_back(broadcastSingleNode);
#if SHOW_DEBUG_METRICS == true
      joinPhaseOps.insert(joinPhaseOps.end(), broadcastSingleNode.begin(), broadcastSingleNode.end());
#endif
      // connect shuffle left to broadcast
      PrePToPTransformerUtil::connectManyToMany(shuffleLeft[nodeId], broadcastSingleNode);
    }
  }

  // check if using bloom filter, note:
  //  - bloom filter is not needed after predicate transfer
  //  - bloom filter cannot be used for right joins
  bool useBloomFilter = USE_BLOOM_FILTER && !fpdb::plan::ENABLE_PRED_TRANS
                        && joinType != JoinType::RIGHT && joinType != JoinType::FULL;
  if (useBloomFilter) {
#if SHOW_DEBUG_METRICS == true
    // check if the join is between two base tables, i.e. neither input is not a joined table
    auto optScanPrePOp = fpdb::plan::prephysical::Util::traceScanOriginWithNoJoinInPath(
            hashJoinPrePOp->getProducers()[1]);
#endif

    // bloom filter create and use ops
    std::vector<POpVec> bfCreate;
    for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
      POpVec bfCreateSingleNode, bfUseSingleNode;
      for (int i = 0; i < parallelDegree_; ++i) {
        int opId = i * numNodes_ + nodeId;
        auto bfCreatePOp = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
                fmt::format("BloomFilterCreate[{}]-{}-{}", prePOpId, hashJoinPredicateStr, opId),
                std::vector<std::string>{}, /*unused*/
                nodeId,
                leftColumnNames);
        auto bfUsePOp = std::make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse[{}]-{}-{}", prePOpId, hashJoinPredicateStr, opId),
                upRightConnPOps[0]->getProjectColumnNames(),
                nodeId,
                rightColumnNames);
        bfCreateSingleNode.emplace_back(bfCreatePOp);
        bfUseSingleNode.emplace_back(bfUsePOp);

        // connect bloom filter create to bloom filter use
        bfCreatePOp->addBloomFilterUsePOp(bfUsePOp);
        bfUsePOp->consume(bfCreatePOp);

        // connect bloom filter create and use to hash join
        auto hashJoinPOp = std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowByNode[nodeId][i]);
        bfCreatePOp->produce(hashJoinPOp);
        bfUsePOp->produce(hashJoinPOp);
        hashJoinPOp->addBuildProducer(bfCreatePOp);
        hashJoinPOp->addProbeProducer(bfUsePOp);
      }

      // add ops
      allPOps.insert(allPOps.end(), bfCreateSingleNode.begin(), bfCreateSingleNode.end());
      allPOps.insert(allPOps.end(), bfUseSingleNode.begin(), bfUseSingleNode.end());
      bfCreate.emplace_back(bfCreateSingleNode);

      // connect shuffle right to bloom filter use
      PrePToPTransformerUtil::connectManyToMany(shuffleRight[nodeId], bfUseSingleNode);

#if SHOW_DEBUG_METRICS == true
      // add bloom filter ops to pred-trans phase
      ptPhaseOps.insert(ptPhaseOps.end(), bfCreateSingleNode.begin(), bfCreateSingleNode.end());
      ptPhaseOps.insert(ptPhaseOps.end(), bfUseSingleNode.begin(), bfUseSingleNode.end());

      // collect predicate transfer metrics
      if (optScanPrePOp != nullptr) {
        for (auto &bfUsePOp: bfUseSingleNode) {
          bfUsePOp->setCollPredTransMetrics(
                  {true, optScanPrePOp->getId(),
                   plan::prephysical::Util::getBaseTableDigest(hashJoinPrePOp->getProducers()[1]),
                   metrics::PredTransMetrics::PTMetricsUnitType::BLOOM_FILTER});
        }
      }
#endif
    }

    // connect broadcast (left input) to bloom filter create
    if (ENABLE_DIST_BCAST_BATCH_EXCHANGE) {
      PrePToPTransformerUtil::connectManyToManyByGroup(shuffleLeftSt.shuffleLeftWithBE_, bfCreate);
    } else {
      for (int i = 0; i < parallelDegree_; ++i) {
        POpVec broadcastSenders, broadcastReceivers;
        for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
          if (!shuffleLeftSt.broadcastLeftNoBE_[nodeId].empty()) {
            broadcastSenders.emplace_back(shuffleLeftSt.broadcastLeftNoBE_[nodeId][i]);
          }
          broadcastReceivers.emplace_back(bfCreate[nodeId][i]);
        }
        PrePToPTransformerUtil::connectManyToMany(broadcastSenders, broadcastReceivers);
      }
    }
  } else {
    if (ENABLE_DIST_BCAST_BATCH_EXCHANGE) {
      // connect shuffle left to hash join
      for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
        for (auto &shuffle: shuffleLeftSt.shuffleLeftWithBE_[nodeId]) {
          for (auto &hashJoin: hashJoinArrowByNode[nodeId]) {
            shuffle->produce(hashJoin);
            std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoin)->addBuildProducer(shuffle);
          }
        }
      }
    } else {
      // connect broadcast (left input) to hash join
      for (int i = 0; i < parallelDegree_; ++i) {
        POpVec broadcastSenders, hashJoinReceivers;
        for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
          if (!shuffleLeftSt.broadcastLeftNoBE_[nodeId].empty()) {
            broadcastSenders.emplace_back(shuffleLeftSt.broadcastLeftNoBE_[nodeId][i]);
          }
          hashJoinReceivers.emplace_back(hashJoinArrowPOps[i * numNodes_ + nodeId]);
        }
        for (auto &sender: broadcastSenders) {
          for (auto &receiver: hashJoinReceivers) {
            sender->produce(receiver);
            std::static_pointer_cast<join::HashJoinArrowPOp>(receiver)->addBuildProducer(sender);
          }
        }
      }
    }

    // connect shuffle right to hash join
    for (int nodeId = 0; nodeId < numNodes_; ++nodeId) {
      for (auto &shuffle: shuffleRight[nodeId]) {
        for (auto &hashJoin: hashJoinArrowByNode[nodeId]) {
          shuffle->produce(hashJoin);
          std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoin)->addProbeProducer(shuffle);
        }
      }
    }
  }

  // record card if needed
  if (recordCard) {
    cache::OutputCardCache::OutputCardKey leftCardKey(hashJoinPrePOp->getProducers()[0]->getId());
    cache::OutputCardCache::OutputCardKey rightCardKey(hashJoinPrePOp->getProducers()[1]->getId());
    for (const auto &op: hashJoinArrowByNode[0]) {
      std::static_pointer_cast<join::HashJoinArrowPOp>(op)->recordOutputCard(leftCardKey, true);
    }
    for (const auto &op: hashJoinArrowPOps) {
      std::static_pointer_cast<join::HashJoinArrowPOp>(op)->recordOutputCard(rightCardKey, false);
    }
  }
}

void PrePToPTransformer::transformHashJoinPtion(
        const std::shared_ptr<HashJoinPrePOp> &hashJoinPrePOp, const std::string &hashJoinPredicateStr,
        POpVec &upLeftConnPOps, POpVec &upRightConnPOps,
        POpVec &hashJoinArrowPOps, POpVec &hashJoinBuildPOps, POpVec &hashJoinProbePOps,
        POpVec &allPOps, POpVec &ptPhaseOps, POpVec &joinPhaseOps, bool recordCard) {
  int prePOpId = hashJoinPrePOp->getId();
  auto joinType = hashJoinPrePOp->getJoinType();
  auto leftColumnNames = hashJoinPrePOp->getLeftColumnNames();
  auto rightColumnNames = hashJoinPrePOp->getRightColumnNames();

  // create shuffle ops
  vector<shared_ptr<PhysicalOp>> shuffleLeftPOps, shuffleRightPOps;
  for (const auto &upLeftConnPOp: upLeftConnPOps) {
    shuffleLeftPOps.emplace_back(make_shared<shuffle::ShufflePOp>(
            fmt::format("Shuffle[{}]-{}", prePOpId, upLeftConnPOp->name()),
            upLeftConnPOp->getProjectColumnNames(),
            upLeftConnPOp->getNodeId(),
            leftColumnNames));
  }
  for (const auto &upRightConnPOp : upRightConnPOps) {
    shuffleRightPOps.emplace_back(make_shared<shuffle::ShufflePOp>(
            fmt::format("Shuffle[{}]-{}", prePOpId, upRightConnPOp->name()),
            upRightConnPOp->getProjectColumnNames(),
            upRightConnPOp->getNodeId(),
            rightColumnNames));
  }

  // check if we need to push shuffle to store
  bool leftWithHashJoinPushdown = false, rightWithHashJoinPushdown = false;
  if (catalogueEntry_->getType() == CatalogueEntryType::OBJ_STORE &&
      std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_)->getStoreType() ==
      obj_store::ObjStoreType::FPDB_STORE) {
    // left, connection to upstream will be made in addSeparablePOp()
    const auto &shuffleLeftAddRes =
            PrePToFPDBStorePTransformer::addSeparablePOp(upLeftConnPOps, shuffleLeftPOps, mode_, &physicalOps_,
                                                         hashJoinPrePOp->getProducers()[0]->getId(),
                                                         numNodes_, parallelDegree_);
    auto opsForShuffleLeft = std::get<0>(shuffleLeftAddRes);
    auto leftAddiOps = std::get<1>(shuffleLeftAddRes);
    leftWithHashJoinPushdown = std::get<2>(shuffleLeftAddRes);
    allPOps.insert(allPOps.end(), leftAddiOps.begin(), leftAddiOps.end());
    upLeftConnPOps = opsForShuffleLeft;
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), leftAddiOps.begin(), leftAddiOps.end());
#endif

    // right, connection to upstream will be made in addSeparablePOp()
    const auto &shuffleRightAddRes =
            PrePToFPDBStorePTransformer::addSeparablePOp(upRightConnPOps, shuffleRightPOps, mode_, &physicalOps_,
                                                         hashJoinPrePOp->getProducers()[1]->getId(),
                                                         numNodes_, parallelDegree_);
    auto opsForShuffleRight = std::get<0>(shuffleRightAddRes);
    auto rightAddiOps = std::get<1>(shuffleRightAddRes);
    rightWithHashJoinPushdown = std::get<2>(shuffleRightAddRes);
    allPOps.insert(allPOps.end(), rightAddiOps.begin(), rightAddiOps.end());
    upRightConnPOps = opsForShuffleRight;
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), rightAddiOps.begin(), rightAddiOps.end());
#endif
  } else {
    allPOps.insert(allPOps.end(), shuffleLeftPOps.begin(), shuffleLeftPOps.end());
    allPOps.insert(allPOps.end(), shuffleRightPOps.begin(), shuffleRightPOps.end());
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), shuffleLeftPOps.begin(), shuffleLeftPOps.end());
    joinPhaseOps.insert(joinPhaseOps.end(), shuffleRightPOps.begin(), shuffleRightPOps.end());
#endif

    // connect to upstream
    PrePToPTransformerUtil::connectOneToOne(upLeftConnPOps, shuffleLeftPOps);
    PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, shuffleRightPOps);
    upLeftConnPOps = shuffleLeftPOps;
    upRightConnPOps = shuffleRightPOps;
  }
  bool leftShufflePushed = upLeftConnPOps[0]->getType() != POpType::SHUFFLE;
  bool rightShufflePushed = upRightConnPOps[0]->getType() != POpType::SHUFFLE;

  // enable batch exchange in dist exec, only do this when pushdown is disabled
  bool useDistShuffleBatchExchange = ENABLE_DIST_SHUFFLE_BATCH_EXCHANGE &&
                                     numNodes_ > 1 &&
                                     (mode_->id() == ModeId::PULL_UP || mode_->id() == ModeId::CACHING_ONLY);
  vector<POpVec> leftFinalShuffles, rightFinalShuffles;
  if (useDistShuffleBatchExchange) {
    leftFinalShuffles = batchExchangeShuffle(shuffleLeftPOps, prePOpId, hashJoinPredicateStr + "-build",
                                             allPOps, &joinPhaseOps);
    rightFinalShuffles = batchExchangeShuffle(shuffleRightPOps, prePOpId, hashJoinPredicateStr + "-probe",
                                              allPOps, &joinPhaseOps);
  }

  // add the first part, because "physicalOps_" needs to be updated before pushing bf after hash-join pushdown
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  allPOps.clear();

  // check if using bloom filter, note:
  //  - bloom filter is not needed after predicate transfer
  //  - bloom filter cannot be used for right joins
  bool useBloomFilter = USE_BLOOM_FILTER && !fpdb::plan::ENABLE_PRED_TRANS
                        && joinType != JoinType::RIGHT && joinType != JoinType::FULL;

  // if using bloom filter
  if (useBloomFilter) {
    auto &joinBuildPOps = USE_ARROW_HASH_JOIN_IMPL ? hashJoinArrowPOps : hashJoinBuildPOps;
    auto &joinProbePOps = USE_ARROW_HASH_JOIN_IMPL ? hashJoinArrowPOps : hashJoinProbePOps;
    vector<shared_ptr<PhysicalOp>> bloomFilterCreatePOps, bloomFilterUsePOps;

    // BloomFilterCreatePOp
    for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
      bloomFilterCreatePOps.emplace_back(make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              upLeftConnPOps[0]->getProjectColumnNames(),
              joinBuildPOps[i]->getNodeId(),
              leftColumnNames));
    }
    allPOps.insert(allPOps.end(), bloomFilterCreatePOps.begin(), bloomFilterCreatePOps.end());
#if SHOW_DEBUG_METRICS == true
    ptPhaseOps.insert(ptPhaseOps.end(), bloomFilterCreatePOps.begin(), bloomFilterCreatePOps.end());
#endif

    // connect bloom filter create to upstream and downstream (joinBuildPOps)
    // there are actually two choices to connect BloomFilterCreatePOp into the plan:
    //   1)     producer (node i)              2)            producer (node i)
    //                 |                                        /        \
    //      BloomFilterCreate (node j)           consumer (node j)   BloomFilterCreate (node j)
    //                 |
    //          consumer (node j)
    // because BloomFilterCreatePOp is made at the same node of consumer (joinBuild) instead of producer (shuffle),
    // 2) incurs double network transfer for each cross-node tupleSet message (i != j), so we pick 1) here
    if (leftWithHashJoinPushdown && leftShufflePushed) {
      PrePToPTransformerUtil::connectOneToOne(upLeftConnPOps, bloomFilterCreatePOps);
    } else {
      if (useDistShuffleBatchExchange) {
        auto bfCreateByNode = PrePToPTransformerUtil::groupByNodeId(bloomFilterCreatePOps, numNodes_);
        PrePToPTransformerUtil::connectManyToManyByGroup(leftFinalShuffles, bfCreateByNode);
      } else {
        PrePToPTransformerUtil::connectManyToMany(upLeftConnPOps, bloomFilterCreatePOps);
      }
    }
    // shuffle batch load
    if (leftShufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
      batchLoadShufflePushdown(upLeftConnPOps, bloomFilterCreatePOps, allPOps);
    }
    if (USE_ARROW_HASH_JOIN_IMPL) {
      for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
        bloomFilterCreatePOps[i]->produce(joinBuildPOps[i]);
        static_pointer_cast<join::HashJoinArrowPOp>(joinBuildPOps[i])->addBuildProducer(bloomFilterCreatePOps[i]);
      }
    } else {
      PrePToPTransformerUtil::connectOneToOne(bloomFilterCreatePOps, joinBuildPOps);
    }

    // check if we need to push bloom filter use to store:
    //   - data is in object store
    //   - object store is fpdb-store
    //   - shuffle is separable
    //   - bloom filter use is separable
    //   - upConnRightPOps are all FPDBStoreSuperPOp (exception with hash-join pushdown)
    // note if shuffle pushdown is disabled, we cannot push down bloom filter either
    bool isBloomFilterUseSeparable = true;
    if (catalogueEntry_->getType() != CatalogueEntryType::OBJ_STORE) {
      isBloomFilterUseSeparable = false;
    }
    if (isBloomFilterUseSeparable && static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_)
                                             ->getStoreType() != obj_store::ObjStoreType::FPDB_STORE) {
      isBloomFilterUseSeparable = false;
    }
    if (isBloomFilterUseSeparable && !StoreTransformTraits::FPDBStoreStoreTransformTraits()
            ->isSeparable(POpType::SHUFFLE)) {
      isBloomFilterUseSeparable = false;
    }
    if (isBloomFilterUseSeparable && !StoreTransformTraits::FPDBStoreStoreTransformTraits()
            ->isSeparable(POpType::BLOOM_FILTER_USE)) {
      isBloomFilterUseSeparable = false;
    }
    if (isBloomFilterUseSeparable) {
      if (!rightWithHashJoinPushdown) {
        for (const auto &upRightConnPOp: upRightConnPOps) {
          if (upRightConnPOp->getType() != POpType::FPDB_STORE_SUPER) {
            isBloomFilterUseSeparable = false;
            break;
          }
        }
      }
    }

    if (isBloomFilterUseSeparable) {
      // we can push down bloom filter
      pushdownBloomFilter(bloomFilterCreatePOps, upRightConnPOps, joinProbePOps,
                          rightColumnNames, rightWithHashJoinPushdown);

      // connect upRightConnPOps (FPDBStoreSuperPOp/FPDBStoreTableCacheLoadPOp) to downstream (joinProbePOps)
      if (rightWithHashJoinPushdown && rightShufflePushed) {
        if (USE_ARROW_HASH_JOIN_IMPL) {
          for (uint i = 0; i < upRightConnPOps.size(); ++i) {
            upRightConnPOps[i]->produce(joinProbePOps[i]);
            static_pointer_cast<join::HashJoinArrowPOp>(joinProbePOps[i])->addProbeProducer(upRightConnPOps[i]);
          }
        } else {
          PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, joinProbePOps);
        }
      } else {
        if (USE_ARROW_HASH_JOIN_IMPL) {
          for (const auto &joinProbePOp: joinProbePOps) {
            for (const auto &upRightConnPOp: upRightConnPOps) {
              upRightConnPOp->produce(joinProbePOp);
              static_pointer_cast<join::HashJoinArrowPOp>(joinProbePOp)->addProbeProducer(upRightConnPOp);
            }
          }
        } else {
          PrePToPTransformerUtil::connectManyToMany(upRightConnPOps, joinProbePOps);
        }
      }

      // shuffle batch load
      if (rightShufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
        bool isJoinLeftConn = false;
        batchLoadShufflePushdown(upRightConnPOps, joinProbePOps, allPOps, &isJoinLeftConn);
      }
    } else {
      // we cannot push down bloom filter

#if SHOW_DEBUG_METRICS == true
      // check if the join is between two base tables, i.e. neither input is not a joined table
      auto optScanPrePOp = fpdb::plan::prephysical::Util::traceScanOriginWithNoJoinInPath(
              hashJoinPrePOp->getProducers()[1]);
#endif

      // BloomFilterUsePOp
      for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
        auto bloomFilterUsePOp = make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
                upRightConnPOps[0]->getProjectColumnNames(),
                joinProbePOps[i]->getNodeId(),
                rightColumnNames);
        bloomFilterUsePOps.emplace_back(bloomFilterUsePOp);

        // connect bloom filter create to bloom filter use
        static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOps[i])
                ->addBloomFilterUsePOp(bloomFilterUsePOp);
        bloomFilterUsePOp->consume(bloomFilterCreatePOps[i]);

#if SHOW_DEBUG_METRICS == true
        // collect predicate transfer metrics
        if (optScanPrePOp != nullptr) {
          bloomFilterUsePOp->setCollPredTransMetrics(
                  {true, optScanPrePOp->getId(),
                   plan::prephysical::Util::getBaseTableDigest(hashJoinPrePOp->getProducers()[1]),
                   metrics::PredTransMetrics::PTMetricsUnitType::BLOOM_FILTER});
        }
#endif
      }
      allPOps.insert(allPOps.end(), bloomFilterUsePOps.begin(), bloomFilterUsePOps.end());
#if SHOW_DEBUG_METRICS == true
      ptPhaseOps.insert(ptPhaseOps.end(), bloomFilterUsePOps.begin(), bloomFilterUsePOps.end());
#endif

      // connect bloom filter use to upstream and downstream (joinProbePOps)
      if (rightWithHashJoinPushdown && rightShufflePushed) {
        PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, bloomFilterUsePOps);
      } else {
        if (useDistShuffleBatchExchange) {
          auto bfUseByNode = PrePToPTransformerUtil::groupByNodeId(bloomFilterUsePOps, numNodes_);
          PrePToPTransformerUtil::connectManyToManyByGroup(rightFinalShuffles, bfUseByNode);
        } else {
          PrePToPTransformerUtil::connectManyToMany(upRightConnPOps, bloomFilterUsePOps);
        }
      }
      if (USE_ARROW_HASH_JOIN_IMPL) {
        for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
          bloomFilterUsePOps[i]->produce(joinProbePOps[i]);
          static_pointer_cast<join::HashJoinArrowPOp>(joinProbePOps[i])->addProbeProducer(bloomFilterUsePOps[i]);
        }
      } else {
        PrePToPTransformerUtil::connectOneToOne(bloomFilterUsePOps, joinProbePOps);
      }

      // shuffle batch load
      if (rightShufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
        batchLoadShufflePushdown(upRightConnPOps, bloomFilterUsePOps, allPOps);
      }
    }
  } else {
    // connect hash join to upstream
    if (USE_ARROW_HASH_JOIN_IMPL) {
      if (useDistShuffleBatchExchange) {
        auto hjArrowByNode = PrePToPTransformerUtil::groupByNodeId(hashJoinArrowPOps, numNodes_);
        for (int i = 0; i < numNodes_; ++i) {
          for (const auto &hjArrow: hjArrowByNode[i]) {
            for (const auto &leftFinalShuffle: leftFinalShuffles[i]) {
              leftFinalShuffle->produce(hjArrow);
              static_pointer_cast<join::HashJoinArrowPOp>(hjArrow)->addBuildProducer(leftFinalShuffle);
            }
            for (const auto &rightFinalShuffle: rightFinalShuffles[i]) {
              rightFinalShuffle->produce(hjArrow);
              static_pointer_cast<join::HashJoinArrowPOp>(hjArrow)->addProbeProducer(rightFinalShuffle);
            }
          }
        }
      } else {
        if (leftWithHashJoinPushdown && leftShufflePushed) {
          for (uint i = 0; i < upLeftConnPOps.size(); ++i) {
            upLeftConnPOps[i]->produce(hashJoinArrowPOps[i]);
            static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowPOps[i])->addBuildProducer(upLeftConnPOps[i]);
          }
        } else {
          for (const auto &hashJoinArrowPOp: hashJoinArrowPOps) {
            for (const auto &upLeftConnPOp: upLeftConnPOps) {
              upLeftConnPOp->produce(hashJoinArrowPOp);
              static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowPOp)->addBuildProducer(upLeftConnPOp);
            }
          }
        }
        if (rightWithHashJoinPushdown && rightShufflePushed) {
          for (uint i = 0; i < upRightConnPOps.size(); ++i) {
            upRightConnPOps[i]->produce(hashJoinArrowPOps[i]);
            static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowPOps[i])->addProbeProducer(upRightConnPOps[i]);
          }
        } else {
          for (const auto &hashJoinArrowPOp: hashJoinArrowPOps) {
            for (const auto &upRightConnPOp: upRightConnPOps) {
              upRightConnPOp->produce(hashJoinArrowPOp);
              static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowPOp)->addProbeProducer(upRightConnPOp);
            }
          }
        }
        // shuffle batch load
        if (leftShufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
          bool isJoinLeftConn = true;
          batchLoadShufflePushdown(upLeftConnPOps, hashJoinArrowPOps, allPOps, &isJoinLeftConn);
        }
        if (rightShufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
          bool isJoinLeftConn = false;
          batchLoadShufflePushdown(upRightConnPOps, hashJoinArrowPOps, allPOps, &isJoinLeftConn);
        }
      }
    } else {
      if (useDistShuffleBatchExchange) {
        auto hjBuildByNode = PrePToPTransformerUtil::groupByNodeId(hashJoinBuildPOps, numNodes_);
        auto hjProbeByNode = PrePToPTransformerUtil::groupByNodeId(hashJoinProbePOps, numNodes_);
        PrePToPTransformerUtil::connectManyToManyByGroup(leftFinalShuffles, hjBuildByNode);
        PrePToPTransformerUtil::connectManyToManyByGroup(rightFinalShuffles, hjProbeByNode);
      } else {
        if (leftWithHashJoinPushdown && leftShufflePushed) {
          PrePToPTransformerUtil::connectOneToOne(upLeftConnPOps, hashJoinBuildPOps);
        } else {
          PrePToPTransformerUtil::connectManyToMany(upLeftConnPOps, hashJoinBuildPOps);
        }
        if (rightWithHashJoinPushdown && rightShufflePushed) {
          PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, hashJoinProbePOps);
        } else {
          PrePToPTransformerUtil::connectManyToMany(upRightConnPOps, hashJoinProbePOps);
        }
        // shuffle batch load
        if (leftShufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
          batchLoadShufflePushdown(upLeftConnPOps, hashJoinBuildPOps, allPOps);
        }
        if (rightShufflePushed && ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD) {
          batchLoadShufflePushdown(upRightConnPOps, hashJoinProbePOps, allPOps);
        }
      }
    }
  }

  // record card if needed
  if (recordCard) {
    cache::OutputCardCache::OutputCardKey leftCardKey(hashJoinPrePOp->getProducers()[0]->getId());
    cache::OutputCardCache::OutputCardKey rightCardKey(hashJoinPrePOp->getProducers()[1]->getId());
    for (const auto &op: hashJoinArrowPOps) {
      std::static_pointer_cast<join::HashJoinArrowPOp>(op)->recordOutputCard(leftCardKey, true);
      std::static_pointer_cast<join::HashJoinArrowPOp>(op)->recordOutputCard(rightCardKey, false);
    }
  }
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformNestedLoopJoin(const shared_ptr<NestedLoopJoinPrePOp> &nestedLoopJoinPrePOp) {
  // id
  auto prePOpId = nestedLoopJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(nestedLoopJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for nestedLoopJoin, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }
  auto upLeftConnPOps = producersTransRes[0];
  auto upRightConnPOps = producersTransRes[1];

#if SHOW_DEBUG_METRICS == true
  // classify created ops into different phases regarding pred-trans
  POpVec joinPhaseOps;
#endif

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps;
  vector<string> projectColumnNames{nestedLoopJoinPrePOp->getProjectColumnNames().begin(),
                                    nestedLoopJoinPrePOp->getProjectColumnNames().end()};
  auto joinType = nestedLoopJoinPrePOp->getJoinType();
  std::optional<shared_ptr<fpdb::expression::gandiva::Expression>> predicate;
  if (nestedLoopJoinPrePOp->getPredicate()) {
    predicate = nestedLoopJoinPrePOp->getPredicate();
  }

  vector<shared_ptr<PhysicalOp>> nestedLoopJoinPOps;
  for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
    shared_ptr<join::NestedLoopJoinPOp> nestedLoopJoinPOp =
            make_shared<join::NestedLoopJoinPOp>(fmt::format("NestedLoopJoin[{}]-{}", prePOpId, i),
                                                 projectColumnNames,
                                                 i % numNodes_,
                                                 predicate,
                                                 joinType);
    // connect to left inputs
    for (const auto &upLeftConnPOp: upLeftConnPOps) {
      upLeftConnPOp->produce(nestedLoopJoinPOp);
      nestedLoopJoinPOp->addLeftProducer(upLeftConnPOp);
    }
    nestedLoopJoinPOps.emplace_back(nestedLoopJoinPOp);
  }
  allPOps.insert(allPOps.end(), nestedLoopJoinPOps.begin(), nestedLoopJoinPOps.end());
#if SHOW_DEBUG_METRICS == true
  joinPhaseOps.insert(joinPhaseOps.end(), nestedLoopJoinPOps.begin(), nestedLoopJoinPOps.end());
#endif

  // connect to right inputs, if num > 1, then we need split operators for right producers
  if (parallelDegree_ * numNodes_ == 1) {
    for (const auto &upRightConnPOp: upRightConnPOps) {
      upRightConnPOp->produce(nestedLoopJoinPOps[0]);
      static_pointer_cast<join::NestedLoopJoinPOp>(nestedLoopJoinPOps[0])->addRightProducer(upRightConnPOp);
    }
  } else {
    vector<shared_ptr<PhysicalOp>> splitPOps;
    for (const auto &upRightConnPOp : upRightConnPOps) {
      shared_ptr<split::SplitPOp> splitPOp = make_shared<split::SplitPOp>(
              fmt::format("Split[{}]-{}", prePOpId, upRightConnPOp->name()),
              upRightConnPOp->getProjectColumnNames(),
              upRightConnPOp->getNodeId());
      splitPOps.emplace_back(splitPOp);

      // Connect splitPOp with all nestedLoopJoinPOps
      for (const auto &nestedLoopJoinPOp: nestedLoopJoinPOps) {
        splitPOp->produce(nestedLoopJoinPOp);
        static_pointer_cast<join::NestedLoopJoinPOp>(nestedLoopJoinPOp)->addRightProducer(splitPOp);
      }
    }
    allPOps.insert(allPOps.end(), splitPOps.begin(), splitPOps.end());
#if SHOW_DEBUG_METRICS == true
    joinPhaseOps.insert(joinPhaseOps.end(), splitPOps.begin(), splitPOps.end());
#endif

    // connect to upstream
    PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, splitPOps);
  }

#if SHOW_DEBUG_METRICS == true
  for (const auto &op: joinPhaseOps) {
    op->setPTPhaseType(metrics::PredTransMetrics::JOIN_PHASE);
  }
#endif

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return nestedLoopJoinPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp) {
  if (catalogueEntry_->getType() != CatalogueEntryType::OBJ_STORE) {
    throw std::runtime_error(fmt::format("Unsupported catalogue entry type for filterable scan prephysical operator "
                                         "during predicate transfer: {}", catalogueEntry_->getTypeName()));
  }
  // use the same transform for SeparableSuperPrePOp
  auto separableSuperPrePOp = std::make_shared<SeparableSuperPrePOp>(filterableScanPrePOp->getId(),
                                                                     filterableScanPrePOp->getRowCount(),
                                                                     filterableScanPrePOp);
  return transformSeparableSuper(separableSuperPrePOp);
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformSeparableSuper(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp) {
  if (catalogueEntry_->getType() != CatalogueEntryType::OBJ_STORE) {
    throw runtime_error(fmt::format("Unsupported catalogue entry type for separable super prephysical operator: {}",
                        catalogueEntry_->getTypeName()));
  }
  auto objStoreCatalogueEntry = std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_);

  switch (objStoreCatalogueEntry->getStoreType()) {
    case ObjStoreType::S3: {
      if (objStoreConnector_->getStoreType() != obj_store::ObjStoreType::S3) {
        throw runtime_error("object store type for catalogue entry and connector mismatch, catalogue entry is 'S3'");
      }
      auto s3Connector = static_pointer_cast<obj_store::S3Connector>(objStoreConnector_);
      auto transformRes = PrePToS3PTransformer::transform(separableSuperPrePOp, mode_, numNodes_, s3Connector);
      PrePToPTransformerUtil::addPhysicalOps(transformRes.second, physicalOps_);
      return transformRes.first;
    }
    case ObjStoreType::FPDB_STORE: {
      if (objStoreConnector_->getStoreType() != obj_store::ObjStoreType::FPDB_STORE) {
        throw runtime_error("object store type for catalogue entry and connector mismatch, catalogue entry is 'FPDB-Store'");
      }
      auto fpdbStoreConnector = static_pointer_cast<obj_store::FPDBStoreConnector>(objStoreConnector_);
      auto transformRes = PrePToFPDBStorePTransformer::transform(separableSuperPrePOp, mode_, numNodes_,
                                                                 parallelDegree_, parallelDegree_, fpdbStoreConnector);
      PrePToPTransformerUtil::addPhysicalOps(transformRes.second, physicalOps_);
      return transformRes.first;
    }
    default:
      throw runtime_error(fmt::format("Unsupported object store type for separable super prephysical operator: {}",
                                      objStoreCatalogueEntry->getStoreTypeName()));
  }
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformUnionAll(const shared_ptr<UnionAllPrePOp> &unionAllPrePOp) {
  // id
  auto prePOpId = unionAllPrePOp->getId();

  // transform producers and group producers by node id
  const auto &producersTransRes = transformProducers(unionAllPrePOp);
  if (producersTransRes.empty()) {
    throw runtime_error(fmt::format("No producers found for union all"));
  }
  POpVec flatProducerPOps;
  for (const auto &producerPOps: producersTransRes) {
    flatProducerPOps.insert(flatProducerPOps.end(), producerPOps.begin(), producerPOps.end());
  }
  auto flatProducerPOpsByNode = PrePToPTransformerUtil::groupByNodeId(flatProducerPOps, numNodes_);

  // split op and project ops per node
  POpVec allPOps, downConnPOps;
  vector<string> projectColumnNames{unionAllPrePOp->getProjectColumnNames().begin(),
                                    unionAllPrePOp->getProjectColumnNames().end()};
  vector<pair<string, string>> projectColumnNamePairs;
  for (const auto &column: projectColumnNames) {
    projectColumnNamePairs.emplace_back(make_pair(column, column));
  }
  for (int i = 0; i < numNodes_; ++i) {
    if (flatProducerPOpsByNode[i].empty()) {
      continue;
    }
    // split
    shared_ptr<PhysicalOp> splitPOp = make_shared<split::SplitPOp>(
            fmt::format("Split-UnionAll[{}]-{}", prePOpId, i),
            vector<string>{} /*unused*/, i);
    allPOps.emplace_back(splitPOp);
    PrePToPTransformerUtil::connectManyToOne(flatProducerPOpsByNode[i], splitPOp);
    // project
    POpVec projectPOps;
    for (int j = 0; j < parallelDegree_; ++j) {
      int opId = j * numNodes_ + i;
      projectPOps.emplace_back(make_shared<project::ProjectPOp>(
              fmt::format("Project-UnionAll[{}]-{}", prePOpId, opId),
              projectColumnNames,
              i,
              vector<shared_ptr<fpdb::expression::gandiva::Expression>>{},
              vector<string>{},
              projectColumnNamePairs));
    }
    allPOps.insert(allPOps.end(), projectPOps.begin(), projectPOps.end());
    downConnPOps.insert(downConnPOps.end(), projectPOps.begin(), projectPOps.end());
    PrePToPTransformerUtil::connectOneToMany(splitPOp, projectPOps);
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return downConnPOps;
}

void PrePToPTransformer::pushdownBloomFilter(const vector<shared_ptr<PhysicalOp>> &bloomFilterCreatePOps,
                                             vector<shared_ptr<PhysicalOp>> &upRightConnPOps,
                                             vector<shared_ptr<PhysicalOp>> &joinProbePOps,
                                             const vector<string> &rightColumnNames,
                                             bool withHashJoinPushdown) {
  if (withHashJoinPushdown) {
    pushdownBloomFilterWithHashJoinPushdown(bloomFilterCreatePOps, upRightConnPOps, rightColumnNames);
  } else {
    pushdownBloomFilterNoHashJoinPushdown(bloomFilterCreatePOps, upRightConnPOps, joinProbePOps, rightColumnNames);
  }
}

void PrePToPTransformer::pushdownBloomFilterNoHashJoinPushdown(
        const vector<shared_ptr<PhysicalOp>> &bloomFilterCreatePOps,
        vector<shared_ptr<PhysicalOp>> &upRightConnPOps,
        vector<shared_ptr<PhysicalOp>> &joinProbePOps,
        const vector<string> &rightColumnNames) {
  // set bloom filter create info
  auto fpdbStoreConnector = static_pointer_cast<FPDBStoreConnector>(objStoreConnector_);
  for (const auto &bloomFilterCreatePOp: bloomFilterCreatePOps) {
    static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOp)->setBloomFilterInfo(
            FPDBStoreBloomFilterCreateInfo{PrePToPTransformerUtil::getHostToNumOps(upRightConnPOps),
                                           fpdbStoreConnector->getFlightPort()});
  }

  // set bloom filter use info (embedded bloom filter) to last sub op inside upConnRightPOps
  // also connect bloom filter create with fpdb store super
  for (const auto &upRightConnPOp: upRightConnPOps) {
    auto subPlan = static_pointer_cast<FPDBStoreSuperPOp>(upRightConnPOp)->getSubPlan();
    auto expLast = subPlan->getLast();
    if (!expLast.has_value()) {
      throw runtime_error(expLast.error());
    }
    for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
      (*expLast)->addConsumerToBloomFilterInfo(joinProbePOps[i]->name(),
                                               bloomFilterCreatePOps[i]->name(),
                                               rightColumnNames);
      // connect
      static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOps[i])
              ->addFPDBStoreBloomFilterConsumer(upRightConnPOp);
      static_pointer_cast<FPDBStoreSuperPOp>(upRightConnPOp)
              ->addFPDBStoreBloomFilterProducer(bloomFilterCreatePOps[i]);
    }
  }
}

void PrePToPTransformer::pushdownBloomFilterWithHashJoinPushdown(
        const vector<shared_ptr<PhysicalOp>> &bloomFilterCreatePOps,
        vector<shared_ptr<PhysicalOp>> &upRightConnPOps,
        const vector<string> &rightColumnNames) {
  // get all FPDBStoreSuperPOps, here "upRightConnPOps" are CollectPOps
  vector<shared_ptr<PhysicalOp>> fpdbStoreSuperPOps;
  for (const auto &fpdbStoreTableCacheLoadPOpName:
          static_pointer_cast<broadcast::BroadcastPOp>(upRightConnPOps[0])->getOrderedProducers()) {
    auto fpdbStoreTableCacheLoadPOp = physicalOps_.find(fpdbStoreTableCacheLoadPOpName)->second;
    auto fpdbStoreSuperPOpName =
            static_pointer_cast<fpdb_store::FPDBStoreTableCacheLoadPOp>(fpdbStoreTableCacheLoadPOp)->getProducer();
    fpdbStoreSuperPOps.emplace_back(physicalOps_.find(fpdbStoreSuperPOpName)->second);
  }

  // set bloom filter create info
  auto fpdbStoreConnector = static_pointer_cast<FPDBStoreConnector>(objStoreConnector_);
  for (const auto &bloomFilterCreatePOp: bloomFilterCreatePOps) {
    static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOp)->setBloomFilterInfo(
            FPDBStoreBloomFilterCreateInfo{PrePToPTransformerUtil::getHostToNumOps(fpdbStoreSuperPOps),
                                           fpdbStoreConnector->getFlightPort()});
  }

  // set bloom filter use info (embedded bloom filter) to last sub op inside upConnRightPOps
  // also connect bloom filter create with fpdb store super
  for (uint i = 0; i < fpdbStoreSuperPOps.size(); ++i) {
    const auto &expLasts =
            static_pointer_cast<fpdb_store::FPDBStoreSuperPOp>(fpdbStoreSuperPOps[i])->getSubPlan()->getLasts();
    if (!expLasts.has_value()) {
      throw runtime_error(expLasts.error());
    }
    for (int j = 0; j < parallelDegree_ * numNodes_; ++j) {
      for (const auto &last: *expLasts) {
        last->addConsumerToBloomFilterInfo(
                static_pointer_cast<broadcast::BroadcastPOp>(upRightConnPOps[j])->getOrderedProducers()[i],
                bloomFilterCreatePOps[j]->name(),
                rightColumnNames);
      }

      // connect
      static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOps[j])
              ->addFPDBStoreBloomFilterConsumer(fpdbStoreSuperPOps[i]);
      static_pointer_cast<fpdb_store::FPDBStoreSuperPOp>(fpdbStoreSuperPOps[i])
              ->addFPDBStoreBloomFilterProducer(bloomFilterCreatePOps[j]);
    }
  }
}

vector<POpVec> PrePToPTransformer::batchExchangeShuffle(const POpVec &distPreShufflePOps,
                                                        uint prePOpId,
                                                        const std::string opNameBase,
                                                        POpVec &allPOps,
                                                        POpVec *joinPhasePOps) {
  // group by "distPreShufflePOps" by node, which now shuffle data to nodes instead of threads
  const auto &shuffleColumns = static_pointer_cast<shuffle::ShufflePOp>(distPreShufflePOps[0])->getShuffleColumnNames();
  auto distPreShuffleByNode = PrePToPTransformerUtil::groupByNodeId(distPreShufflePOps, numNodes_);

  // "BatchExchangePOp" as the consumers of "distPreShufflePOps"
  // only create "BatchExchangePOp" on the nodes where there exist "distPreShufflePOps"
  POpVec batchExchangeByNode;
  POpVec batchExchangeByNodeWithEmpty(numNodes_, nullptr);   // same as "batchExchangeByNode" but with empty slots
                                                             // if we don't create one on that node
  for (int i = 0; i < numNodes_; ++i) {
    if (distPreShuffleByNode[i].empty()) {
      continue;
    }
    shared_ptr<PhysicalOp> batchExchange = make_shared<exchange::BatchExchangePOp>(
            fmt::format("BatchExchange[{}]-{}-<node{}>", prePOpId, opNameBase, i),
            vector<string>{} /*unused*/, i);
    PrePToPTransformerUtil::connectManyToOne(distPreShuffleByNode[i], batchExchange);
    batchExchangeByNode.emplace_back(batchExchange);
    batchExchangeByNodeWithEmpty[i] = batchExchange;
  }
  allPOps.insert(allPOps.end(), batchExchangeByNode.begin(), batchExchangeByNode.end());
#if SHOW_DEBUG_METRICS == true
  if (joinPhasePOps != nullptr) {
    joinPhasePOps->insert(joinPhasePOps->end(), batchExchangeByNode.begin(), batchExchangeByNode.end());
  }
#endif

  // "SplitPOp" as the consumers of "BatchExchangePOp"
  // if "ENABLE_PARALLEL_BATCH_EXCHANGE", create multiple "SplitPOp" in each node and a "BatchExchangeRRForwardPOp"
  // as their producer; otherwise, create just one "SplitPOp" in each node
  vector<string> batchExchangeReceivers;
  vector<POpVec> splitVecByNode;      // used when "ENABLE_PARALLEL_BATCH_EXCHANGE = true"
  POpVec splitByNode;                 // used when "ENABLE_PARALLEL_BATCH_EXCHANGE = false"
  if (ENABLE_PARALLEL_BATCH_EXCHANGE) {
    POpVec batchExchangeRRForwardByNode;
    for (int i = 0; i < numNodes_; ++i) {
      // "BatchExchangeRRForwardPOp"
      string batchExchangeReceiver = fmt::format("BatchExchangeRRForward[{}]-{}-<node{}>", prePOpId, opNameBase, i);
      batchExchangeReceivers.emplace_back(batchExchangeReceiver);
      batchExchangeRRForwardByNode.emplace_back(make_shared<exchange::BatchExchangeRRForwardPOp>(
              batchExchangeReceiver, vector<string>{} /*unused*/, i));
      // "SplitPOp"
      POpVec splitSingleNode;
      for (int j = 0; j < BATCH_EXCHANGE_PARALLEL_DEGREE; ++j) {
        splitSingleNode.emplace_back(make_shared<split::SplitPOp>(
                fmt::format("Split-BE[{}]-{}-<node{}>-{}", prePOpId, opNameBase, i, j),
                vector<string>{} /*unused*/, i));
      }
      splitVecByNode.emplace_back(splitSingleNode);
      allPOps.insert(allPOps.end(), splitSingleNode.begin(), splitSingleNode.end());
#if SHOW_DEBUG_METRICS == true
      if (joinPhasePOps != nullptr) {
        joinPhasePOps->insert(joinPhasePOps->end(), splitSingleNode.begin(), splitSingleNode.end());
      }
#endif
    }
    PrePToPTransformerUtil::connectManyToMany(batchExchangeByNode, batchExchangeRRForwardByNode);
    PrePToPTransformerUtil::connectOneToManyByGroup(batchExchangeRRForwardByNode, splitVecByNode);
    allPOps.insert(allPOps.end(), batchExchangeRRForwardByNode.begin(), batchExchangeRRForwardByNode.end());
#if SHOW_DEBUG_METRICS == true
    if (joinPhasePOps != nullptr) {
      joinPhasePOps->insert(joinPhasePOps->end(), batchExchangeRRForwardByNode.begin(), batchExchangeRRForwardByNode.end());
    }
#endif
  } else {
    // "SplitPOp"
    for (int i = 0; i < numNodes_; ++i) {
      string batchExchangeReceiver = fmt::format("Split-BE[{}]-{}-<node{}>", prePOpId, opNameBase, i);
      batchExchangeReceivers.emplace_back(batchExchangeReceiver);
      splitByNode.emplace_back(make_shared<split::SplitPOp>(
              batchExchangeReceiver, vector<string>{} /*unused*/, i));
    }
    PrePToPTransformerUtil::connectManyToMany(batchExchangeByNode, splitByNode);
    allPOps.insert(allPOps.end(), splitByNode.begin(), splitByNode.end());
#if SHOW_DEBUG_METRICS == true
    if (joinPhasePOps != nullptr) {
      joinPhasePOps->insert(joinPhasePOps->end(), splitByNode.begin(), splitByNode.end());
    }
#endif
  }

  // let "distPreShufflePOps" be aware of batch exchange, i.e. let them know all the receivers of "BatchExchangePOp"
  for (const auto &distPreShuffle: distPreShufflePOps) {
    static_pointer_cast<shuffle::ShufflePOp>(distPreShuffle)->enableDistBatchExchange(
            batchExchangeByNodeWithEmpty[distPreShuffle->getNodeId()]->name(), batchExchangeReceivers);
  }

  // a second set of "ShufflePOp" as the consumers of "SplitPOp", to further shuffle single-node data into threads
  vector<POpVec> finalShuffleByNode;
  for (int i = 0; i < numNodes_; ++i) {
    POpVec finalShuffleSingleNode;
    for (int j = 0; j < parallelDegree_; ++j) {
      finalShuffleSingleNode.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle-F[{}]-{}-<node{}>-{}", prePOpId, opNameBase, i, j),
              distPreShufflePOps[0]->getProjectColumnNames(),
              i,
              shuffleColumns));
    }
    finalShuffleByNode.emplace_back(finalShuffleSingleNode);
    allPOps.insert(allPOps.end(), finalShuffleSingleNode.begin(), finalShuffleSingleNode.end());
#if SHOW_DEBUG_METRICS == true
    if (joinPhasePOps != nullptr) {
      joinPhasePOps->insert(joinPhasePOps->end(), finalShuffleSingleNode.begin(), finalShuffleSingleNode.end());
    }
#endif
  }
  if (ENABLE_PARALLEL_BATCH_EXCHANGE) {
    PrePToPTransformerUtil::connectManyToManyByGroup(splitVecByNode, finalShuffleByNode);
  } else {
    PrePToPTransformerUtil::connectOneToManyByGroup(splitByNode, finalShuffleByNode);
  }

  return finalShuffleByNode;
}

void PrePToPTransformer::batchLoadShufflePushdown(const vector<shared_ptr<PhysicalOp>> &opsForShuffle,
                                          const vector<shared_ptr<PhysicalOp>> &shuffleConsumers,
                                          vector<shared_ptr<PhysicalOp>> &allPOps,
                                          bool* isHashJoinArrowLeftConn) {
  // split shuffle's consumers by nodes
  vector<vector<shared_ptr<PhysicalOp>>> shuffleConsumersByNode{(size_t) numNodes_};
  for (const auto &shuffleConsumer: shuffleConsumers) {
    shuffleConsumersByNode[shuffleConsumer->getNodeId()].emplace_back(shuffleConsumer);
    // reset producers
    if (isHashJoinArrowLeftConn == nullptr) {
      if (shuffleConsumer->getType() == POpType::BLOOM_FILTER_USE) {
        static_pointer_cast<bloomfilter::BloomFilterUsePOp>(shuffleConsumer)->clearProducersExceptBloomFilterCreate();
      } else {
        shuffleConsumer->clearProducers();
      }
    } else {
      if (*isHashJoinArrowLeftConn) {
        static_pointer_cast<join::HashJoinArrowPOp>(shuffleConsumer)->clearBuildProducers();
      } else {
        static_pointer_cast<join::HashJoinArrowPOp>(shuffleConsumer)->clearProbeProducers();
      }
    }
  }

  for (const auto &opForShuffle: opsForShuffle) {
    // shuffle batch load
    vector<shared_ptr<PhysicalOp>> shuffleBatchLoadPOps{(size_t) numNodes_};
    set<string> shuffleBatchLoadPOpNames;
    for (int i = 0; i < numNodes_; ++i) {
      shared_ptr<PhysicalOp> shuffleBatchLoadPOp = make_shared<fpdb_store::FPDBStoreShuffleBatchLoadPOp>(
              fmt::format("ShuffleBatchLoad-{}-node-{}", opForShuffle->name(), i),
              vector<string>{},      // never used
              i);
      shuffleBatchLoadPOps[i] = shuffleBatchLoadPOp;
      shuffleBatchLoadPOpNames.emplace(shuffleBatchLoadPOp->name());
      // connect to upConnPOp
      shuffleBatchLoadPOp->consume(opForShuffle);
      // connect to shuffle's consumers, need to specially handle when HashJoinArrowPOp is shuffle's consumer
      if (isHashJoinArrowLeftConn == nullptr) {
        PrePToPTransformerUtil::connectOneToMany(shuffleBatchLoadPOp, shuffleConsumersByNode[i]);
      } else {
        for (const auto &shuffleConsumer: shuffleConsumersByNode[i]) {
          shuffleBatchLoadPOp->produce(shuffleConsumer);
          if (*isHashJoinArrowLeftConn) {
            static_pointer_cast<join::HashJoinArrowPOp>(shuffleConsumer)->addBuildProducer(shuffleBatchLoadPOp);
          } else {
            static_pointer_cast<join::HashJoinArrowPOp>(shuffleConsumer)->addProbeProducer(shuffleBatchLoadPOp);
          }
        }
      }
    }
    allPOps.insert(allPOps.end(), shuffleBatchLoadPOps.begin(), shuffleBatchLoadPOps.end());

    // reset consumers of upConnPOp
    opForShuffle->setConsumers(shuffleBatchLoadPOpNames);
  }
}

void PrePToPTransformer::clear() {
  physicalOps_.clear();
}

void PrePToPTransformer::showAdaptSelectedDistJoinTypes() const {
  if (metrics::SHOW_DIST_JOIN_TYPE && !adaptDistJoinTypes_.empty()) {
    std::stringstream ss;
    ss << endl << "Adaptively selected dist-join types |" << endl;
    ss << left << setw(110) << setfill('-') << "" << endl;
    ss << setfill(' ');
    ss << left << setw(75) << "Join";
    ss << left << setw(35) << "Type";
    ss << endl;
    ss << left << setw(110) << setfill('-') << "" << endl;
    ss << setfill(' ');
    for (const auto &pair: adaptDistJoinTypes_) {
      ss << left << setw(75) << pair.first;
      ss << left << setw(35) << join::DistJoinTypeUtil::toString(pair.second);
      ss << endl;
    }
    ss << left << setw(110) << setfill('-') << "" << endl;
    ss << setfill(' ');
    ss << endl;
    printf("%s", ss.str().c_str());
  }
}

}
