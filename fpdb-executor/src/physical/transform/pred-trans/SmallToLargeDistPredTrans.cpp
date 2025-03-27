//
// Created by Yifei Yang on 3/12/24.
//

#include <fpdb/executor/physical/transform/pred-trans/SmallToLargePredTransOrder.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterSplitPOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterConcatPOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterInitPOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterFinalizePOp.h>
#include <fpdb/executor/physical/bloomfilter/DistGlobalBloomFilterInitPOp.h>
#include <fpdb/executor/physical/bloomfilter/DistGlobalBloomFilterMergePOp.h>
#include <fpdb/executor/physical/broadcast/BroadcastPOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/split/NodewiseSplitPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/project/TupleSetSizeProjectPOp.h>
#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/exchange/BatchExchangePOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowPOp.h>
#include <fpdb/executor/Executor.h>
#include <fpdb/plan/prephysical/Util.h>

/**
 * Implementation of functions of "SmallToLargePredTransOrder" regarding dist. pred-trans
 */
namespace fpdb::executor::physical {

void makeOnePairFilterOpsDistBcastVal(
        int numNodes, int parallelDegree,
        const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
        POpVec& in, POpVec& out,
        std::vector<POpVec>&, std::vector<POpVec>& filterProbe,
        POpVec& newOps, const metrics::PredTransCSMetrics::PTCSMetricsInfo&,
        bool recordCard = false) {
  bool forward = (dirSbl == "F") ? true : false;
  POpVec bfInitOps;
  for (int i = 0; i < numNodes; ++i) {
    // project to collect join key vals of build table in each node
    std::vector<std::pair<std::string, std::string>> buildProjectColumnPairs;
    for (const auto &buildColumn: buildColumns) {
      buildProjectColumnPairs.emplace_back(std::make_pair(buildColumn, buildColumn));
    }
    std::shared_ptr<PhysicalOp> buildProject = std::make_shared<project::ProjectPOp>(
            fmt::format("Project({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
            buildColumns,
            i,
            std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{},
            std::vector<std::string>{},
            buildProjectColumnPairs);
    in.emplace_back(buildProject);
    newOps.emplace_back(buildProject);
    // split for the probe table in each node
    std::shared_ptr<PhysicalOp> probeSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    out.emplace_back(probeSplit);
    newOps.emplace_back(probeSplit);
    // global bf init for the join key vals of build table in each node
    std::shared_ptr<PhysicalOp> bfInit = std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
            fmt::format("GlobalBloomFilterInit({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i,
            buildColumns,
            1 /* here we use vals from all nodes to build one bf*/);
    bfInitOps.emplace_back(bfInit);
    newOps.emplace_back(bfInit);
    // BF ops
    POpVec singleBfCreateVec, singleBfUseVec;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      singleBfCreateVec.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i,
              buildColumns,
              true));
      singleBfUseVec.emplace_back(std::make_shared<bloomfilter::BloomFilterUsePOp>(
              fmt::format("BloomFilterUse({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i,
              probeColumns,
              1 /* for each node there is a single bf constructed using broadcast join key vals*/));
    }
    newOps.insert(newOps.end(), singleBfCreateVec.begin(), singleBfCreateVec.end());
    newOps.insert(newOps.end(), singleBfUseVec.begin(), singleBfUseVec.end());
    filterProbe.emplace_back(singleBfUseVec);
    PrePToPTransformerUtil::connectOneToMany(bfInit, singleBfCreateVec);
    PrePToPTransformerUtil::connectOneToMany(probeSplit, singleBfUseVec);
    // "GlobalBloomFilterFinalizePOp"
    std::shared_ptr<PhysicalOp> bfFinalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
            fmt::format("GlobalBloomFilterFinalize({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(bfFinalize, bfInit, singleBfCreateVec);
    bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(bfFinalize, filterProbe[i], {});
    newOps.emplace_back(bfFinalize);
  }
  // connect "in_" (buildProject) to bf init
  PrePToPTransformerUtil::connectManyToMany(in, bfInitOps);
  // record card if needed
  if (recordCard) {
    std::static_pointer_cast<bloomfilter::GlobalBloomFilterInitPOp>(bfInitOps[0])
            ->recordPredTransCard(cache::PredTransCardCache::PredTransCardKey(step, forward, true, true));
    for (const auto &op: out) {
      std::static_pointer_cast<split::SplitPOp>(op)
              ->recordPredTransCard(cache::PredTransCardCache::PredTransCardKey(step, forward, false, true));
    }
    for (const auto &opVec: filterProbe) {
      for (const auto &op: opVec) {
        std::static_pointer_cast<bloomfilter::BloomFilterUsePOp>(op)
                ->recordPredTransCard(cache::PredTransCardCache::PredTransCardKey(step, forward, false, false));
      }
    }
  }
}

void makeOnePairFilterOpsDistBcastBf(
        int numNodes, int parallelDegree,
        const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
        POpVec& in, POpVec& out,
        std::vector<POpVec>&, std::vector<POpVec>& filterProbe,
        POpVec& newOps, const metrics::PredTransCSMetrics::PTCSMetricsInfo &ptCSMetricsInfoBase,
        bool) {
#if SHOW_DEBUG_METRICS == true
  auto buildPTCSMetricsInfo = ptCSMetricsInfoBase;
  auto probePTCSMetricsInfo = ptCSMetricsInfoBase;
  buildPTCSMetricsInfo.bfTimeType_ = metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::BUILD;
  probePTCSMetricsInfo.bfTimeType_ = metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::PROBE;
#endif
  // common portion regardless of whether to "USE_DIST_GLOBAL_BF"
  POpVec bfInitOps, bfFinalizeOps, bfBroadcastOps;
  for (int i = 0; i < numNodes; ++i) {
    // BF ops
    POpVec singleBfCreateVec, singleBfUseVec;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      singleBfCreateVec.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
        fmt::format("BloomFilterCreate({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, opId),
        std::vector<std::string>{} /*unused*/,
        i,
        buildColumns,
        true));
      singleBfUseVec.emplace_back(std::make_shared<bloomfilter::BloomFilterUsePOp>(
        fmt::format("BloomFilterUse({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, opId),
        std::vector<std::string>{} /*unused*/,
        i,
        probeColumns,
        USE_DIST_GLOBAL_BF ? 1 : numNodes));
    }
    newOps.insert(newOps.end(), singleBfCreateVec.begin(), singleBfCreateVec.end());
    newOps.insert(newOps.end(), singleBfUseVec.begin(), singleBfUseVec.end());
    // "GlobalBloomFilterInitPOp" for the build side and "SplitPOp" for the probe side
    std::shared_ptr<PhysicalOp> init = std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
      fmt::format("GlobalBloomFilterInit({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i,
      buildColumns,
      USE_DIST_GLOBAL_BF ? 1 : numNodes);
    bfInitOps.emplace_back(init);
    PrePToPTransformerUtil::connectOneToMany(init, singleBfCreateVec);
    std::shared_ptr<PhysicalOp> split = std::make_shared<split::SplitPOp>(
      fmt::format("Split({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i);
    PrePToPTransformerUtil::connectOneToMany(split, singleBfUseVec);
    newOps.insert(newOps.end(), {init, split});
    // set "out" and "filterProbe"
    out.emplace_back(split);
    filterProbe.emplace_back(singleBfUseVec);
    // "GlobalBloomFilterFinalizePOp"
    std::shared_ptr<PhysicalOp> finalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
      fmt::format("GlobalBloomFilterFinalize({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i);
    bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(finalize, init, singleBfCreateVec);
    bfFinalizeOps.emplace_back(finalize);
    newOps.emplace_back(finalize);
    // broadcast bf across nodes
    std::shared_ptr<PhysicalOp> bfBroadcastOp = std::make_shared<broadcast::BroadcastPOp>(
      fmt::format("Broadcast-BF({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i);
    bfBroadcastOps.emplace_back(bfBroadcastOp);
    newOps.emplace_back(bfBroadcastOp);
    // connect "BroadcastPOp" to "BloomFilterUsePOp" to forward received remote bf
    PrePToPTransformerUtil::connectOneToMany(bfBroadcastOp, singleBfUseVec);
#if SHOW_DEBUG_METRICS == true
    for (const auto &op: singleBfCreateVec) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    for (const auto &op: singleBfUseVec) {
      op->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    }
    init->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    finalize->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    split->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    bfBroadcastOp->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
#endif
  }

  // whether to merge as a single dist global BF then broadcast, or do pairwise broadcast
  if (USE_DIST_GLOBAL_BF) {
    // "DistGlobalBloomFilterInit" after collecting input size from all nodes
    int nodeSyncDistBf = rand() % numNodes;
    std::shared_ptr<PhysicalOp> distBfInit = std::make_shared<DistGlobalBloomFilterInitPOp>(
      fmt::format("DistGlobalBloomFilterInitPOp({})<{}>-{}", dirSbl, step, hashJoinPredicateStr),
      std::vector<std::string>{} /*unused*/,
      nodeSyncDistBf);
    newOps.emplace_back(distBfInit);
#if SHOW_DEBUG_METRICS == true
    distBfInit->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
#endif
    for (int i = 0; i < numNodes; ++i) {
      // collect build side input size from all nodes
      std::shared_ptr<PhysicalOp> buildInputBroadcast = std::make_shared<broadcast::BroadcastPOp>(
        fmt::format("Broadcast-BI({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
        std::vector<std::string>{} /*unused*/,
        i);
      std::shared_ptr<PhysicalOp> sizeProject = std::make_shared<project::TupleSetSizeProjectPOp>(
        fmt::format("TupleSetSizeProject({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
        std::vector<std::string>{} /*unused*/,
        i);
      newOps.insert(newOps.end(), {buildInputBroadcast, sizeProject});
      PrePToPTransformerUtil::connectOneToOne(buildInputBroadcast, sizeProject);
      PrePToPTransformerUtil::connectOneToOne(buildInputBroadcast, bfInitOps[i]);
      PrePToPTransformerUtil::connectOneToOne(sizeProject, distBfInit);
      PrePToPTransformerUtil::connectOneToOne(distBfInit, bfInitOps[i]);
      // set "in"
      in.emplace_back(buildInputBroadcast);
#if SHOW_DEBUG_METRICS == true
      buildInputBroadcast->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      sizeProject->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
#endif
    }

    // whether to merge the dist global bf in parallel
    if (USE_PARALLEL_DIST_GLOBAL_BF_MERGE) {
      // create a separate bf split, bf concate and dist bf merge for each node
      POpVec bfSplitOps, bfConcatOps, distBfMergeOps;
      for (int i = 0; i < numNodes; ++i) {
        bfSplitOps.emplace_back(std::make_shared<BloomFilterSplitPOp>(
          fmt::format("BloomFilterSplitPOp({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
          std::vector<std::string>{} /*unused*/,
          i));
        bfConcatOps.emplace_back(std::make_shared<BloomFilterConcatPOp>(
          fmt::format("BloomFilterConcatPOp({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
          std::vector<std::string>{} /*unused*/,
          i));
        distBfMergeOps.emplace_back(std::make_shared<DistGlobalBloomFilterMergePOp>(
          fmt::format("DistGlobalBloomFilterMergePOp({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
          std::vector<std::string>{} /*unused*/,
          i));
      }
      newOps.insert(newOps.end(), bfSplitOps.begin(), bfSplitOps.end());
      newOps.insert(newOps.end(), bfConcatOps.begin(), bfConcatOps.end());
      newOps.insert(newOps.end(), distBfMergeOps.begin(), distBfMergeOps.end());
      // connect bf finalize to bf split
      for (int i = 0; i < numNodes; ++i) {
        bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(bfFinalizeOps[i], {bfSplitOps[i]}, {});
      }
      // connect bf split to dist bf merge
      PrePToPTransformerUtil::connectManyToMany(bfSplitOps, distBfMergeOps);
      // connect dist bf merge to bf concat, need to do one by one for ordering needed by bf concat
      for (int i = 0; i < numNodes; ++i) {
        for (int j = 0; j < numNodes; ++j) {
          if (i == j) {
            bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(distBfMergeOps[i], {bfConcatOps[j]}, {});
          } else {
            bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(distBfMergeOps[i], {}, {bfConcatOps[j]});
          }
        }
      }
      // connect bf concat to bf broadcast
      PrePToPTransformerUtil::connectOneToOne(bfConcatOps, bfBroadcastOps);
#if SHOW_DEBUG_METRICS == true
      for (const auto &op: bfSplitOps) {
        op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      }
      for (const auto &op: bfConcatOps) {
        op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      }
      for (const auto &op: distBfMergeOps) {
        op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      }
#endif
    } else {
      // a single "DistGlobalBloomFilterMerge" to merge BF from all nodes
      std::shared_ptr<PhysicalOp> distBfMerge = std::make_shared<DistGlobalBloomFilterMergePOp>(
        fmt::format("DistGlobalBloomFilterMergePOp({})<{}>-{}", dirSbl, step, hashJoinPredicateStr),
        std::vector<std::string>{} /*unused*/,
        nodeSyncDistBf);
      newOps.emplace_back(distBfMerge);
      // merge BF from all nodes
      POpVec mergedBfRemoteConsumers;
      for (int i = 0; i < numNodes; ++i) {
        if (i == nodeSyncDistBf) {
          bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(bfFinalizeOps[i], {distBfMerge}, {});
        } else {
          bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(bfFinalizeOps[i], {}, {distBfMerge});
          // remote consumer of the merged BF
          mergedBfRemoteConsumers.emplace_back(bfBroadcastOps[i]);
        }
      }
      // broadcast merged BF to all nodes
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(
          distBfMerge, filterProbe[nodeSyncDistBf], mergedBfRemoteConsumers);
#if SHOW_DEBUG_METRICS == true
      distBfMerge->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
#endif
    }
  } else {
    // broadcast bf to all the other nodes
    for (int i = 0; i < numNodes; ++i) {
      // set "in"
      in.emplace_back(bfInitOps[i]);
      // set BF local and remote consumers
      POpVec bfRemoteConsumers;
      for (int j = 0; j < numNodes; ++j) {
        if (i == j) {
          continue;
        }
        bfRemoteConsumers.emplace_back(bfBroadcastOps[j]);
      }
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(bfFinalizeOps[i], filterProbe[i], bfRemoteConsumers);
    }
  }
}

void makeOnePairFilterOpsDistPtionPostProbeVal(
        int numNodes, const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &probeColumns, std::vector<POpVec>& filterProbe, POpVec& newOps,
        std::vector<std::vector<POpVec>> &probeBfUses, int bfParallelPerGroup,
        const std::vector<POpVec> &probeShuffles) {
  /// step 4: send back reduced join key vals of the probe table to their original node respectively
  // "BatchExchange" to collect reduced probe table join key vals
  POpVec postProbeExchanges, postProbeNodewiseSplits;
  std::vector<std::string> postProbeExchangeReceivers;
  for (int i = 0; i < numNodes; ++i) {
    postProbeExchanges.emplace_back(std::make_shared<exchange::BatchExchangePOp>(
            fmt::format("BatchExchange({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i));
    std::string postProbeExchangeReceiver =
            fmt::format("NodewiseSplit({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, i);
    postProbeNodewiseSplits.emplace_back(std::make_shared<split::NodewiseSplitPOp>(
            postProbeExchangeReceiver,
            std::vector<std::string>{} /*unused*/,
            i,
            numNodes));
    postProbeExchangeReceivers.emplace_back(postProbeExchangeReceiver);
  }
  newOps.insert(newOps.end(), postProbeExchanges.begin(), postProbeExchanges.end());
  newOps.insert(newOps.end(), postProbeNodewiseSplits.begin(), postProbeNodewiseSplits.end());
  PrePToPTransformerUtil::connectManyToMany(postProbeExchanges, postProbeNodewiseSplits);
  // a dummy "Shuffle" for each bf use group to forward reduced probe table join key vals to "postProbeExchanges",
  // grouped by incoming nodes
  for (int i = 0; i < numNodes; ++i) {
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      std::shared_ptr<PhysicalOp> postProbeDummyShuffle = std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-postProbe(dummy)-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              probeColumns);
      newOps.emplace_back(postProbeDummyShuffle);
      PrePToPTransformerUtil::connectManyToOne(probeBfUses[i][group], postProbeDummyShuffle);
      PrePToPTransformerUtil::connectOneToOne(postProbeDummyShuffle, postProbeExchanges[i]);
      // let the dummy shuffle be aware of batch exchange
      std::static_pointer_cast<shuffle::ShufflePOp>(postProbeDummyShuffle)
              ->enableDistBatchExchange(postProbeExchanges[i]->name(), {postProbeExchangeReceivers[group]});
    }
  }

  /// step 5: use reduced join key vals to reduce the probe table, in each node, grouped by shuffling
  for (int i = 0; i < numNodes; ++i) {
    POpVec bfUseSingleNode;
    std::vector<POpVec> bfUseGroups;
    for (int group = 0; group < numNodes; ++group) {
      // BF init, grouped by shuffling
      int opId = group * numNodes + i;
      std::shared_ptr<PhysicalOp> postProbeBfInit = std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
              fmt::format("GlobalBloomFilterInit({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i,
              probeColumns,
              1 /* here the bf is only used within the current node*/);
      newOps.emplace_back(postProbeBfInit);
      std::static_pointer_cast<split::NodewiseSplitPOp>(postProbeNodewiseSplits[i])->produce(postProbeBfInit, group);
      postProbeBfInit->consume(postProbeNodewiseSplits[i]);
      // BF create, grouped by shuffling
      POpVec bfCreateGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        opId = (j * numNodes + group) * numNodes + i;
        bfCreateGroup.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
                fmt::format("BloomFilterCreate({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                probeColumns,
                true));
      }
      newOps.insert(newOps.end(), bfCreateGroup.begin(), bfCreateGroup.end());
      PrePToPTransformerUtil::connectOneToMany(postProbeBfInit, bfCreateGroup);
      // BF use, grouped by shuffling
      POpVec bfUseGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        opId = (j * numNodes + group) * numNodes + i;
        std::shared_ptr<PhysicalOp> bfUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse({})<{}>-{}-finalProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                probeColumns,
                1 /*for each group there is a single bf*/);
        bfUseSingleNode.emplace_back(bfUse);
        bfUseGroup.emplace_back(bfUse);
        newOps.emplace_back(bfUse);
      }
      bfUseGroups.emplace_back(bfUseGroup);
      // "GlobalBloomFilterFinalizePOp"
      opId = group * numNodes + i;
      std::shared_ptr<PhysicalOp> postProbeBfFinalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
              fmt::format("GlobalBloomFilterFinalize({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i);
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(postProbeBfFinalize, postProbeBfInit, bfCreateGroup);
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(postProbeBfFinalize, bfUseGroup, {});
      newOps.emplace_back(postProbeBfFinalize);
    }
    // connect BF use to input data
    POpVec finalProbeSplits;
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      finalProbeSplits.emplace_back(std::make_shared<split::SplitPOp>(
              fmt::format("Split({})<{}>-{}-finalProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i));
    }
    newOps.insert(newOps.end(), finalProbeSplits.begin(), finalProbeSplits.end());
    for (const auto &probeShuffle: probeShuffles[i]) {
      std::static_pointer_cast<shuffle::ShufflePOp>(probeShuffle)->produceAddiConsumerVec(finalProbeSplits);
      for (const auto &finalProbeSplit: finalProbeSplits) {
        finalProbeSplit->consume(probeShuffle);
      }
    }
    PrePToPTransformerUtil::connectOneToManyByGroup(finalProbeSplits, bfUseGroups);
    // "bfUseSingleNode" produces finally reduced probe table in each node
    filterProbe.emplace_back(bfUseSingleNode);
  }
}

void makeOnePairFilterOpsDistPtionVal(
        int numNodes, int parallelDegree,
        const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
        POpVec& in, POpVec& out,
        std::vector<POpVec>&, std::vector<POpVec>& filterProbe,
        POpVec& newOps, const metrics::PredTransCSMetrics::PTCSMetricsInfo&,
        bool) {
  if (!ENABLE_DIST_SHUFFLE_BATCH_EXCHANGE) {
    throw std::runtime_error("Node-level shuffle requires enabling dist shuffle batch exchange.");
  }

  /// step 1: use shuffled (in the node level) join key vals of the build table to build a BF in each node
  POpVec buildExchanges, buildBfInits;
  std::vector<POpVec> buildShuffles;
  std::vector<std::string> buildExchangeReceivers;
  std::vector<std::pair<std::string, std::string>> buildProjectColumnPairs;
  for (const auto &buildColumn: buildColumns) {
    buildProjectColumnPairs.emplace_back(std::make_pair(buildColumn, buildColumn));
  }
  for (int i = 0; i < numNodes; ++i) {
    // project join key vals of the build table, which also serves as "in"
    std::shared_ptr<PhysicalOp> buildProject = std::make_shared<project::ProjectPOp>(
            fmt::format("Project({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            buildColumns,
            i,
            std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{},
            std::vector<std::string>{},
            buildProjectColumnPairs);
    in.emplace_back(buildProject);
    newOps.emplace_back(buildProject);
    // shuffle the join key vals of the build table in the node level
    std::shared_ptr<PhysicalOp> buildSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    newOps.emplace_back(buildSplit);
    PrePToPTransformerUtil::connectOneToOne(buildProject, buildSplit);
    POpVec buildShuffleSingleNode;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      buildShuffleSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              buildColumns));
    }
    newOps.insert(newOps.end(), buildShuffleSingleNode.begin(), buildShuffleSingleNode.end());
    PrePToPTransformerUtil::connectOneToMany(buildSplit, buildShuffleSingleNode);
    buildShuffles.emplace_back(buildShuffleSingleNode);
    // BatchExchange to collect shuffled data in the sender node and transfer to receiver nodes
    std::shared_ptr<PhysicalOp> buildExchange = make_shared<exchange::BatchExchangePOp>(
            fmt::format("BatchExchange({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            vector<string>{} /*unused*/, i);
    buildExchanges.emplace_back(buildExchange);
    newOps.emplace_back(buildExchange);
    PrePToPTransformerUtil::connectManyToOne(buildShuffleSingleNode, buildExchange);
    // GlobalBloomFilterInit in the receiver nodes to collect shuffled data from BatchExchange
    std::string buildExchangeReceiver =
            fmt::format("GlobalBloomFilterInit({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i);
    std::shared_ptr<PhysicalOp> buildBfInit = std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
            buildExchangeReceiver,
            std::vector<std::string>{} /*unused*/,
            i,
            buildColumns,
            1 /* here the bf is only used within the current node*/);
    buildBfInits.emplace_back(buildBfInit);
    newOps.emplace_back(buildBfInit);
    buildExchangeReceivers.emplace_back(buildExchangeReceiver);
  }
  // connect "buildExchanges" to "buildBfInits"
  PrePToPTransformerUtil::connectManyToMany(buildExchanges, buildBfInits);
  // let shuffles be aware of batch exchange
  for (int i = 0; i < numNodes; ++i) {
    for (const auto &op: buildShuffles[i]) {
      std::static_pointer_cast<shuffle::ShufflePOp>(op)
              ->enableDistBatchExchange(buildExchanges[i]->name(), buildExchangeReceivers);
    }
  }

  /// step 2: shuffle (in the node level) the probe table (will also be used in step 5), but only exchange join key vals
  /// "project join key vals" is performed within "BatchExchange" instead of using separate "Project"
  POpVec probeExchanges, probeNodewiseSplits;
  std::vector<POpVec> probeShuffles;
  std::vector<std::string> probeExchangeReceivers;
  for (int i = 0; i < numNodes; ++i) {
    // shuffle the probe table in the node level, which also serves as "out"
    std::shared_ptr<PhysicalOp> probeSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    newOps.emplace_back(probeSplit);
    out.emplace_back(probeSplit);
    POpVec probeShuffleSingleNode;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      probeShuffleSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              probeColumns));
    }
    newOps.insert(newOps.end(), probeShuffleSingleNode.begin(), probeShuffleSingleNode.end());
    PrePToPTransformerUtil::connectOneToMany(probeSplit, probeShuffleSingleNode);
    probeShuffles.emplace_back(probeShuffleSingleNode);
    // BatchExchange to collect shuffled data in the sender node and transfer to receiver nodes
    std::shared_ptr<PhysicalOp> probeExchange = make_shared<exchange::BatchExchangePOp>(
            fmt::format("BatchExchange({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, i),
            probeColumns /*only exchange join key vals*/,
            i);
    probeExchanges.emplace_back(probeExchange);
    newOps.emplace_back(probeExchange);
    PrePToPTransformerUtil::connectManyToOne(probeShuffleSingleNode, probeExchange);
    // NodewiseSplit in the receiver nodes to collect shuffled data from BatchExchange, and then
    // split data from each sender node respectively
    std::string probeExchangeReceiver =
            fmt::format("NodewiseSplit({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, i);
    std::shared_ptr<PhysicalOp> probeNodewiseSplit = std::make_shared<split::NodewiseSplitPOp>(
            probeExchangeReceiver,
            std::vector<std::string>{} /*unused*/,
            i,
            numNodes);
    probeNodewiseSplits.emplace_back(probeNodewiseSplit);
    newOps.emplace_back(probeNodewiseSplit);
    probeExchangeReceivers.emplace_back(probeExchangeReceiver);
  }
  // connect "probeExchanges" to "probeNodewiseSplits"
  PrePToPTransformerUtil::connectManyToMany(probeExchanges, probeNodewiseSplits);
  // let shuffles be aware of batch exchange
  for (int i = 0; i < numNodes; ++i) {
    for (const auto &op: probeShuffles[i]) {
      std::static_pointer_cast<shuffle::ShufflePOp>(op)
              ->enableDistBatchExchange(probeExchanges[i]->name(), probeExchangeReceivers);
    }
  }

  /// step 3: filter join key vals of the probe table using BF constructed from join key vals of the build table
  /// in each node, need to keep filtered data grouped by incoming nodes
  std::vector<std::vector<POpVec>> probeBfUses;
  int bfParallelPerGroup = std::ceil((double) parallelDegree / (double) numNodes);
  for (int i = 0; i < numNodes; ++i) {
    // BF create
    POpVec bfCreateSingleNode;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      bfCreateSingleNode.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i,
              buildColumns,
              true));
    }
    newOps.insert(newOps.end(), bfCreateSingleNode.begin(), bfCreateSingleNode.end());
    PrePToPTransformerUtil::connectOneToMany(buildBfInits[i], bfCreateSingleNode);
    // "GlobalBloomFilterFinalizePOp"
    std::shared_ptr<PhysicalOp> buildBfFinalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
            fmt::format("GlobalBloomFilterFinalize({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(buildBfFinalize, buildBfInits[i], bfCreateSingleNode);
    newOps.emplace_back(buildBfFinalize);
    // BF use, grouped by incoming nodes
    std::vector<POpVec> bfUseSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      POpVec bfUseGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        int opId = (j * numNodes + group) * numNodes + i;
        std::shared_ptr<PhysicalOp> bfUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                probeColumns,
                1 /* for each node there is a single bf constructed using shuffled join key vals*/);
        bfUseGroup.emplace_back(bfUse);
        newOps.emplace_back(bfUse);
        // connect this group of bf to its input
        std::static_pointer_cast<split::NodewiseSplitPOp>(probeNodewiseSplits[i])->produce(bfUse, group);
        bfUse->consume(probeNodewiseSplits[i]);
      }
      bfUseSingleNode.emplace_back(bfUseGroup);
      // broadcast BF into this group
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(buildBfFinalize, bfUseGroup, {});
    }
    probeBfUses.emplace_back(bfUseSingleNode);
  }

  /// step 4 and 5: post-probe
  /// send back reduced join key vals of the probe table to their original node respectively, and
  /// use reduced join key vals to reduce the probe table
  makeOnePairFilterOpsDistPtionPostProbeVal(numNodes, dirSbl, step, hashJoinPredicateStr,
                                            probeColumns, filterProbe, newOps,
                                            probeBfUses, bfParallelPerGroup, probeShuffles);
}

void makeOnePairFilterOpsDistPtionPostProbeOneSideBf(
        int numNodes, const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &postProbeColumns, const std::vector<std::string> &finalProbeColumns,
        std::vector<POpVec>& filterProbe, POpVec& newOps, std::vector<std::vector<POpVec>> &probeBfUses,
        int bfParallelPerGroup, const std::vector<POpVec> &probeShuffles,
        const metrics::PredTransCSMetrics::PTCSMetricsInfo &ptCSMetricsInfoBase) {
#if SHOW_DEBUG_METRICS == true
  auto buildPTCSMetricsInfo = ptCSMetricsInfoBase;
  auto probePTCSMetricsInfo = ptCSMetricsInfoBase;
  buildPTCSMetricsInfo.bfTimeType_ = metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::BUILD;
  probePTCSMetricsInfo.bfTimeType_ = metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::PROBE;
#endif

  /// step 4: construct BFs using reduced join key vals of the probe table, then send them back to their
  /// original node respectively
  std::vector<POpVec> postProbeBfFinalizes, postProbeBroadcasts;    // postProbeBroadcasts[i][i] = nullptr
  for (int i = 0; i < numNodes; ++i) {
    // GlobalBloomFilterInit to collect reduced join key vals of the probe table for each group
    POpVec postProbeBfInitSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      postProbeBfInitSingleNode.emplace_back(std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
              fmt::format("GlobalBloomFilterInit({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i,
              postProbeColumns,
              1 /* here the BF is only used standalone */));
    }
    newOps.insert(newOps.end(), postProbeBfInitSingleNode.begin(), postProbeBfInitSingleNode.end());
    PrePToPTransformerUtil::connectManyToOneByGroup(probeBfUses[i], postProbeBfInitSingleNode);
    // BF create in each group
    std::vector<POpVec> postProbeBfCreateSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      POpVec postProbeBfCreateGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        int opId = (j * numNodes + group) * numNodes + i;
        postProbeBfCreateGroup.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
                fmt::format("BloomFilterCreate({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                postProbeColumns,
                true));
      }
      newOps.insert(newOps.end(), postProbeBfCreateGroup.begin(), postProbeBfCreateGroup.end());
      postProbeBfCreateSingleNode.emplace_back(postProbeBfCreateGroup);
    }
    PrePToPTransformerUtil::connectOneToManyByGroup(postProbeBfInitSingleNode, postProbeBfCreateSingleNode);
    // "GlobalBloomFilterFinalizePOp"
    POpVec postProbeBfFinalizeSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      std::shared_ptr<PhysicalOp> postProbeBfFinalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
              fmt::format("GlobalBloomFilterFinalize({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i);
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(
              postProbeBfFinalize, postProbeBfInitSingleNode[group], postProbeBfCreateSingleNode[group]);
      postProbeBfFinalizeSingleNode.emplace_back(postProbeBfFinalize);
    }
    newOps.insert(newOps.end(), postProbeBfFinalizeSingleNode.begin(), postProbeBfFinalizeSingleNode.end());
    postProbeBfFinalizes.emplace_back(postProbeBfFinalizeSingleNode);
    // Broadcast to exchange BFs
    POpVec postProbeBroadcastSingleNode(numNodes, nullptr);
    for (int group = 0; group < numNodes; ++group) {
      if (i == group) {
        continue;
      }
      int opId = group * numNodes + i;
      postProbeBroadcastSingleNode[group] = std::make_shared<broadcast::BroadcastPOp>(
              fmt::format("Broadcast({})<{}>-{}-postProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i);
      newOps.emplace_back(postProbeBroadcastSingleNode[group]);
    }
    postProbeBroadcasts.emplace_back(postProbeBroadcastSingleNode);
#if SHOW_DEBUG_METRICS == true
    for (const auto &op: postProbeBfInitSingleNode) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    for (const auto &opVec: postProbeBfCreateSingleNode) {
      for (const auto &op: opVec) {
        op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      }
    }
    for (const auto &op: postProbeBfFinalizeSingleNode) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    for (const auto &op: postProbeBroadcastSingleNode) {
      if (op != nullptr) {
        op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      }
    }
#endif
  }
  // connect "postProbeBfFinalizes" to "postProbeBroadcasts" when not on the same node
  for (int i = 0; i < numNodes; ++i) {
    for (int group = 0; group < numNodes; ++group) {
      if (i == group) {
        continue;
      }
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(
              postProbeBfFinalizes[i][group], {}, {postProbeBroadcasts[group][i]});
    }
  }

  /// step 5: use BFs constructed reduced join key vals to reduce the probe table, in each node, grouped by shuffling
  for (int i = 0; i < numNodes; ++i) {
    // BF use, grouped by shuffling
    POpVec finalProbeBfUseSingleNode;
    std::vector<POpVec> finalProbeBfUseGroups;
    for (int group = 0; group < numNodes; ++group) {
      POpVec finalProbeBfUseGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        int opId = (j * numNodes + group) * numNodes + i;
        finalProbeBfUseGroup.emplace_back(std::make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse({})<{}>-{}-finalProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                finalProbeColumns,
                1 /*for each group there is a single bf*/));
      }
      finalProbeBfUseGroups.emplace_back(finalProbeBfUseGroup);
      finalProbeBfUseSingleNode.insert(finalProbeBfUseSingleNode.end(),
                                       finalProbeBfUseGroup.begin(), finalProbeBfUseGroup.end());
      newOps.insert(newOps.end(), finalProbeBfUseGroup.begin(), finalProbeBfUseGroup.end());
      // connect either BF finalize or Broadcast to BF use
      if (i == group) {
        bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(
                postProbeBfFinalizes[i][i], finalProbeBfUseGroup, {});
      } else {
        PrePToPTransformerUtil::connectOneToMany(postProbeBroadcasts[i][group], finalProbeBfUseGroup);
      }
    }
    // connect input data to BF use
    POpVec finalProbeSplits;
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      finalProbeSplits.emplace_back(std::make_shared<split::SplitPOp>(
              fmt::format("Split({})<{}>-{}-finalProbe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i));
    }
    newOps.insert(newOps.end(), finalProbeSplits.begin(), finalProbeSplits.end());
    for (const auto &probeShuffle: probeShuffles[i]) {
      std::static_pointer_cast<shuffle::ShufflePOp>(probeShuffle)->produceAddiConsumerVec(finalProbeSplits);
      for (const auto &finalProbeSplit: finalProbeSplits) {
        finalProbeSplit->consume(probeShuffle);
      }
    }
    PrePToPTransformerUtil::connectOneToManyByGroup(finalProbeSplits, finalProbeBfUseGroups);
    // "finalProbeBfUseSingleNode" produces finally reduced probe table in each node
    filterProbe.emplace_back(finalProbeBfUseSingleNode);

#if SHOW_DEBUG_METRICS == true
    for (const auto &op: finalProbeBfUseSingleNode) {
      op->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    }
    for (const auto &op: finalProbeSplits) {
      op->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    }
#endif
  }
}

void makeOnePairFilterOpsDistPtionSrcBfDstVal(
        int numNodes, int parallelDegree,
        const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
        POpVec& in, POpVec& out,
        std::vector<POpVec>&, std::vector<POpVec>& filterProbe,
        POpVec& newOps, const metrics::PredTransCSMetrics::PTCSMetricsInfo &ptCSMetricsInfoBase,
        bool) {
  if (!ENABLE_DIST_SHUFFLE_BATCH_EXCHANGE) {
    throw std::runtime_error("Node-level shuffle requires enabling dist shuffle batch exchange.");
  }
#if SHOW_DEBUG_METRICS == true
  auto buildPTCSMetricsInfo = ptCSMetricsInfoBase;
  auto probePTCSMetricsInfo = ptCSMetricsInfoBase;
  buildPTCSMetricsInfo.bfTimeType_ = metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::BUILD;
  probePTCSMetricsInfo.bfTimeType_ = metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::PROBE;
  probePTCSMetricsInfo.collFiltering_ = false;    // only collect filtering for `post probe`
#endif

  /// step 1: shuffle join key vals (in the node level) of the build table without exchange in each node,
  /// then exchange BF constructed from the shuffle results (done by step 1 and 3 together)
  int bfParallelPerGroup = std::ceil((double) parallelDegree / (double) numNodes);
  std::vector<std::pair<std::string, std::string>> buildProjectColumnPairs;
  for (const auto &buildColumn: buildColumns) {
    buildProjectColumnPairs.emplace_back(std::make_pair(buildColumn, buildColumn));
  }
  POpVec buildBroadcasts;
  std::vector<POpVec> buildBfFinalizes;
  for (int i = 0; i < numNodes; ++i) {
    // project join key vals of the build table, which also serves as "in"
    std::shared_ptr<PhysicalOp> buildProject = std::make_shared<project::ProjectPOp>(
            fmt::format("Project({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            buildColumns,
            i,
            std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{},
            std::vector<std::string>{},
            buildProjectColumnPairs);
    in.emplace_back(buildProject);
    newOps.emplace_back(buildProject);
    // shuffle the join key vals of the build table in the node level, but do not exchange them across nodes
    std::shared_ptr<PhysicalOp> buildSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    newOps.emplace_back(buildSplit);
    PrePToPTransformerUtil::connectOneToOne(buildProject, buildSplit);
    POpVec buildShuffleSingleNode;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      buildShuffleSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              buildColumns));
    }
    newOps.insert(newOps.end(), buildShuffleSingleNode.begin(), buildShuffleSingleNode.end());
    PrePToPTransformerUtil::connectOneToMany(buildSplit, buildShuffleSingleNode);
    // GlobalBloomFilterInit to collect shuffled data in each node
    POpVec buildBfInitSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      buildBfInitSingleNode.emplace_back(std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
              fmt::format("GlobalBloomFilterInit({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i,
              buildColumns,
              numNodes /* here each node receive BFs from nodes*/));
    }
    newOps.insert(newOps.end(), buildBfInitSingleNode.begin(), buildBfInitSingleNode.end());
    PrePToPTransformerUtil::connectManyToMany(buildShuffleSingleNode, buildBfInitSingleNode);
    // BF create in each group
    std::vector<POpVec> buildBfCreateSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      POpVec buildBfCreateGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        int opId = (j * numNodes + group) * numNodes + i;
        buildBfCreateGroup.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
                fmt::format("BloomFilterCreate({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                buildColumns,
                true));
      }
      newOps.insert(newOps.end(), buildBfCreateGroup.begin(), buildBfCreateGroup.end());
      buildBfCreateSingleNode.emplace_back(buildBfCreateGroup);
      PrePToPTransformerUtil::connectOneToMany(buildBfInitSingleNode[group], buildBfCreateGroup);
    }
    // "GlobalBloomFilterFinalizePOp"
    POpVec buildBfFinalizeSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      std::shared_ptr<PhysicalOp> buildBfFinalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
              fmt::format("GlobalBloomFilterFinalize({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i);
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(
              buildBfFinalize, buildBfInitSingleNode[group], buildBfCreateSingleNode[group]);
      buildBfFinalizeSingleNode.emplace_back(buildBfFinalize);
    }
    newOps.insert(newOps.end(), buildBfFinalizeSingleNode.begin(), buildBfFinalizeSingleNode.end());
    buildBfFinalizes.emplace_back(buildBfFinalizeSingleNode);
    // Broadcast to exchange shuffled BFs
    std::shared_ptr<PhysicalOp> buildBroadcast = std::make_shared<broadcast::BroadcastPOp>(
            fmt::format("Broadcast({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    newOps.emplace_back(buildBroadcast);
    buildBroadcasts.emplace_back(buildBroadcast);
#if SHOW_DEBUG_METRICS == true
    buildProject->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    buildSplit->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    for (const auto &op: buildShuffleSingleNode) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    for (const auto &op: buildBfInitSingleNode) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    for (const auto &opVec: buildBfCreateSingleNode) {
      for (const auto &op: opVec) {
        op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      }
    }
    for (const auto &op: buildBfFinalizeSingleNode) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    buildBroadcast->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
#endif
  }

  /// step 2: shuffle (in the node level) the probe table (will also be used in step 5), but only exchange join key vals
  /// "project join key vals" is performed within "BatchExchange" instead of using separate "Project"
  POpVec probeExchanges, probeNodewiseSplits;
  std::vector<POpVec> probeShuffles;
  std::vector<std::string> probeExchangeReceivers;
  for (int i = 0; i < numNodes; ++i) {
    // shuffle the probe table in the node level, which also serves as "out"
    std::shared_ptr<PhysicalOp> probeSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    newOps.emplace_back(probeSplit);
    out.emplace_back(probeSplit);
    POpVec probeShuffleSingleNode;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      probeShuffleSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              probeColumns));
    }
    newOps.insert(newOps.end(), probeShuffleSingleNode.begin(), probeShuffleSingleNode.end());
    PrePToPTransformerUtil::connectOneToMany(probeSplit, probeShuffleSingleNode);
    probeShuffles.emplace_back(probeShuffleSingleNode);
    // BatchExchange to collect shuffled data in the sender node and transfer to receiver nodes
    std::shared_ptr<PhysicalOp> probeExchange = make_shared<exchange::BatchExchangePOp>(
            fmt::format("BatchExchange({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, i),
            probeColumns /*only exchange join key vals*/,
            i);
    probeExchanges.emplace_back(probeExchange);
    newOps.emplace_back(probeExchange);
    PrePToPTransformerUtil::connectManyToOne(probeShuffleSingleNode, probeExchange);
    // NodewiseSplit in the receiver nodes to collect shuffled data from BatchExchange, and then
    // split data from each sender node respectively
    std::string probeExchangeReceiver =
            fmt::format("NodewiseSplit({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, i);
    std::shared_ptr<PhysicalOp> probeNodewiseSplit = std::make_shared<split::NodewiseSplitPOp>(
            probeExchangeReceiver,
            std::vector<std::string>{} /*unused*/,
            i,
            numNodes);
    probeNodewiseSplits.emplace_back(probeNodewiseSplit);
    newOps.emplace_back(probeNodewiseSplit);
    probeExchangeReceivers.emplace_back(probeExchangeReceiver);
#if SHOW_DEBUG_METRICS == true
    probeSplit->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    for (const auto &op: probeShuffleSingleNode) {
      op->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    }
    probeExchange->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    probeNodewiseSplit->setCollPredTransCSMetrics(probePTCSMetricsInfo);
#endif
  }
  // connect "probeExchanges" to "probeNodewiseSplits"
  PrePToPTransformerUtil::connectManyToMany(probeExchanges, probeNodewiseSplits);
  // let shuffles be aware of batch exchange
  for (int i = 0; i < numNodes; ++i) {
    for (const auto &op: probeShuffles[i]) {
      std::static_pointer_cast<shuffle::ShufflePOp>(op)
              ->enableDistBatchExchange(probeExchanges[i]->name(), probeExchangeReceivers);
    }
  }

  /// step 3: filter join key vals of the probe table using the received exchanged shuffled BFs from the build table,
  /// need to keep filtered data grouped by incoming nodes
  std::vector<std::vector<POpVec>> probeBfUses;
  for (int i = 0; i < numNodes; ++i) {
    // BF use in each group
    std::vector<POpVec> probeBfUseSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      POpVec probeBfUseGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        int opId = (j * numNodes + group) * numNodes + i;
        std::shared_ptr<PhysicalOp> bfUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                probeColumns,
                numNodes /* for each node there are BFs from all nodes*/);
        newOps.emplace_back(bfUse);
        probeBfUseGroup.emplace_back(bfUse);
        // connect this group of bf to its input
        std::static_pointer_cast<split::NodewiseSplitPOp>(probeNodewiseSplits[i])->produce(bfUse, group);
        bfUse->consume(probeNodewiseSplits[i]);
      }
      probeBfUseSingleNode.emplace_back(probeBfUseGroup);
    }
    probeBfUses.emplace_back(probeBfUseSingleNode);
#if SHOW_DEBUG_METRICS == true
    for (const auto &opVec: probeBfUseSingleNode) {
      for (const auto &op: opVec) {
        op->setCollPredTransCSMetrics(probePTCSMetricsInfo);
      }
    }
#endif
  }
  for (int i = 0; i < numNodes; ++i) {
    // connect "buildBfFinalizes" to "buildBroadcasts" to broadcast local BF to other nodes
    for (int j = 0; j < numNodes; ++j) {
      if (i == j) {
        continue;
      } else {
        bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(buildBfFinalizes[i][j], {}, {buildBroadcasts[j]});
      }
    }
    // connect local BF ("buildBfFinalizes") and remote BFs ("buildBroadcasts")  to "probeBfUses"
    for (auto &probeBfUseGroup: probeBfUses[i]) {
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(buildBfFinalizes[i][i], probeBfUseGroup, {});
      PrePToPTransformerUtil::connectOneToMany(buildBroadcasts[i], probeBfUseGroup);
    }
  }

  /// step 4 and 5: post-probe
  /// construct BFs using reduced join key vals of the probe table, then send them back to their original node, and
  /// use these BFs reduce the probe table, in each node, grouped by shuffling
  makeOnePairFilterOpsDistPtionPostProbeOneSideBf(numNodes, dirSbl, step, hashJoinPredicateStr,
                                                  probeColumns, probeColumns, filterProbe, newOps,
                                                  probeBfUses, bfParallelPerGroup, probeShuffles,
                                                  ptCSMetricsInfoBase);
}

void makeOnePairFilterOpsDistPtionSrcValDstBf(
        int numNodes, int parallelDegree,
        const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
        POpVec& in, POpVec& out,
        std::vector<POpVec>&, std::vector<POpVec>& filterProbe,
        POpVec& newOps, const metrics::PredTransCSMetrics::PTCSMetricsInfo &ptCSMetricsInfoBase,
        bool) {
  if (!ENABLE_DIST_SHUFFLE_BATCH_EXCHANGE) {
    throw std::runtime_error("Node-level shuffle requires enabling dist shuffle batch exchange.");
  }
#if SHOW_DEBUG_METRICS == true
  auto buildPTCSMetricsInfo = ptCSMetricsInfoBase;
  auto probePTCSMetricsInfo = ptCSMetricsInfoBase;
  buildPTCSMetricsInfo.bfTimeType_ = metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::BUILD;
  probePTCSMetricsInfo.bfTimeType_ = metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::PROBE;
  probePTCSMetricsInfo.collFiltering_ = false;    // only collect filtering for `post probe`
#endif

  /// step 1: shuffle join key vals (in the node level) of the build table, and exchange across the nodes
  POpVec buildExchanges, buildSplitsBE;
  std::vector<POpVec> buildShuffles;
  std::vector<std::string> buildExchangeReceivers;
  std::vector<std::pair<std::string, std::string>> buildProjectColumnPairs;
  for (const auto &buildColumn: buildColumns) {
    buildProjectColumnPairs.emplace_back(std::make_pair(buildColumn, buildColumn));
  }
  for (int i = 0; i < numNodes; ++i) {
    // project join key vals of the build table, which also serves as "in"
    std::shared_ptr<PhysicalOp> buildProject = std::make_shared<project::ProjectPOp>(
            fmt::format("Project({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            buildColumns,
            i,
            std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{},
            std::vector<std::string>{},
            buildProjectColumnPairs);
    in.emplace_back(buildProject);
    newOps.emplace_back(buildProject);
    // shuffle the join key vals of the build table in the node level
    std::shared_ptr<PhysicalOp> buildSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    newOps.emplace_back(buildSplit);
    PrePToPTransformerUtil::connectOneToOne(buildProject, buildSplit);
    POpVec buildShuffleSingleNode;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      buildShuffleSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              buildColumns));
    }
    newOps.insert(newOps.end(), buildShuffleSingleNode.begin(), buildShuffleSingleNode.end());
    PrePToPTransformerUtil::connectOneToMany(buildSplit, buildShuffleSingleNode);
    buildShuffles.emplace_back(buildShuffleSingleNode);
    // BatchExchange to collect shuffled data in the sender node and transfer to receiver nodes
    std::shared_ptr<PhysicalOp> buildExchange = make_shared<exchange::BatchExchangePOp>(
            fmt::format("BatchExchange({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            vector<string>{} /*unused*/, i);
    buildExchanges.emplace_back(buildExchange);
    newOps.emplace_back(buildExchange);
    PrePToPTransformerUtil::connectManyToOne(buildShuffleSingleNode, buildExchange);
    // Broadcast in the receiver nodes to collect shuffled data from BatchExchange, since the build side needs to be
    // filtered on multiple BFs from the probe side
    std::string buildExchangeReceiver =
            fmt::format("Split-BE({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i);
    std::shared_ptr<PhysicalOp> buildSplitBE = std::make_shared<split::SplitPOp>(
            buildExchangeReceiver,
            std::vector<std::string>{} /*unused*/,
            i);
    buildSplitsBE.emplace_back(buildSplitBE);
    newOps.emplace_back(buildSplitBE);
    buildExchangeReceivers.emplace_back(buildExchangeReceiver);
#if SHOW_DEBUG_METRICS == true
    buildProject->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    buildSplit->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    for (const auto &op: buildShuffleSingleNode) {
      op->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    }
    buildExchange->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    buildSplitBE->setCollPredTransCSMetrics(probePTCSMetricsInfo);
#endif
  }
  // connect "buildExchanges" to "buildSplitsBE"
  PrePToPTransformerUtil::connectManyToMany(buildExchanges, buildSplitsBE);
  // let shuffles be aware of batch exchange
  for (int i = 0; i < numNodes; ++i) {
    for (const auto &op: buildShuffles[i]) {
      std::static_pointer_cast<shuffle::ShufflePOp>(op)
              ->enableDistBatchExchange(buildExchanges[i]->name(), buildExchangeReceivers);
    }
  }

  /// step 2: shuffle (in the node level) the probe table (will also be used in step 5), but only exchange BFs
  /// constructed from the join key vals of the shuffle results (done by step 2 and 3 together)
  int bfParallelPerGroup = std::ceil((double) parallelDegree / (double) numNodes);
  std::vector<POpVec> probeShuffles, probeBfFinalizes, probeBroadcasts;   // probeBroadcasts[i][i] = nullptr;
  for (int i = 0; i < numNodes; ++i) {
    // shuffle the probe table in the node level, which also serves as "out"
    std::shared_ptr<PhysicalOp> probeSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    newOps.emplace_back(probeSplit);
    out.emplace_back(probeSplit);
    POpVec probeShuffleSingleNode;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      probeShuffleSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              probeColumns));
    }
    newOps.insert(newOps.end(), probeShuffleSingleNode.begin(), probeShuffleSingleNode.end());
    PrePToPTransformerUtil::connectOneToMany(probeSplit, probeShuffleSingleNode);
    probeShuffles.emplace_back(probeShuffleSingleNode);
    // GlobalBloomFilterInit to collect shuffled data in each node
    POpVec probeBfInitSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      probeBfInitSingleNode.emplace_back(std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
              fmt::format("GlobalBloomFilterInit({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i,
              probeColumns,
              1 /* here each BF is not used with other BFs together within a single BF use */));
    }
    newOps.insert(newOps.end(), probeBfInitSingleNode.begin(), probeBfInitSingleNode.end());
    PrePToPTransformerUtil::connectManyToMany(probeShuffleSingleNode, probeBfInitSingleNode);
    // BF create in each group
    std::vector<POpVec> probeBfCreateSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      POpVec probeBfCreateGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        int opId = (j * numNodes + group) * numNodes + i;
        probeBfCreateGroup.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
                fmt::format("BloomFilterCreate({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                probeColumns,
                true));
      }
      newOps.insert(newOps.end(), probeBfCreateGroup.begin(), probeBfCreateGroup.end());
      probeBfCreateSingleNode.emplace_back(probeBfCreateGroup);
      PrePToPTransformerUtil::connectOneToMany(probeBfInitSingleNode[group], probeBfCreateGroup);
    }
    // "GlobalBloomFilterFinalizePOp"
    POpVec probeBfFinalizeSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      int opId = group * numNodes + i;
      std::shared_ptr<PhysicalOp> probeBfFinalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
              fmt::format("GlobalBloomFilterFinalize({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i);
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(
              probeBfFinalize, probeBfInitSingleNode[group], probeBfCreateSingleNode[group]);
      probeBfFinalizeSingleNode.emplace_back(probeBfFinalize);
    }
    newOps.insert(newOps.end(), probeBfFinalizeSingleNode.begin(), probeBfFinalizeSingleNode.end());
    probeBfFinalizes.emplace_back(probeBfFinalizeSingleNode);
    // Broadcast to exchange shuffled BFs
    POpVec probeBroadcastSingleNode(numNodes, nullptr);
    for (int group = 0; group < numNodes; ++group) {
      if (i == group) {
        continue;
      }
      int opId = group * numNodes + i;
      probeBroadcastSingleNode[group] = std::make_shared<broadcast::BroadcastPOp>(
              fmt::format("Broadcast({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i);
      newOps.emplace_back(probeBroadcastSingleNode[group]);
    }
    probeBroadcasts.emplace_back(probeBroadcastSingleNode);
#if SHOW_DEBUG_METRICS == true
    probeSplit->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    for (const auto &op: probeShuffleSingleNode) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    for (const auto &op: probeBfInitSingleNode) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    for (const auto &opVec: probeBfCreateSingleNode) {
      for (const auto &op: opVec) {
        op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      }
    }
    for (const auto &op: probeBfFinalizeSingleNode) {
      op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
    }
    for (const auto &op: probeBroadcastSingleNode) {
      if (op != nullptr) {
        op->setCollPredTransCSMetrics(buildPTCSMetricsInfo);
      }
    }
#endif
  }
  // connect "probeBfFinalizes" to "probeBroadcasts" when not on the same node
  for (int i = 0; i < numNodes; ++i) {
    for (int group = 0; group < numNodes; ++group) {
      if (i == group) {
        continue;
      }
      bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(
              probeBfFinalizes[i][group], {}, {probeBroadcasts[group][i]});
    }
  }

  /// step 3: filter join key vals of the build table using the received exchanged shuffled BFs from the probe table,
  /// need to keep filtered data grouped by incoming nodes
  std::vector<std::vector<POpVec>> buildBfUses;
  for (int i = 0; i < numNodes; ++i) {
    // Broadcast to duplicate join key vals of the build table so that each copy is filtered by
    // a BF from an incoming node
    POpVec buildBroadcastSingleNode;
    for (int j = 0; j < bfParallelPerGroup; ++j) {
      int opId = j * numNodes + i;
      buildBroadcastSingleNode.emplace_back(std::make_shared<broadcast::BroadcastPOp>(
              fmt::format("Broadcast({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*unused*/,
              i));
    }
    newOps.insert(newOps.end(), buildBroadcastSingleNode.begin(), buildBroadcastSingleNode.end());
    PrePToPTransformerUtil::connectOneToMany(buildSplitsBE[i], buildBroadcastSingleNode);
    // BF use in each group
    std::vector<POpVec> buildBfUseSingleNode;
    for (int group = 0; group < numNodes; ++group) {
      POpVec buildBfUseGroup;
      for (int j = 0; j < bfParallelPerGroup; ++j) {
        int opId = (j * numNodes + group) * numNodes + i;
        std::shared_ptr<PhysicalOp> bfUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
                std::vector<std::string>{} /*unused*/,
                i,
                buildColumns,
                1 /* here each BF is not used with other BFs together within a single BF use */);
        newOps.emplace_back(bfUse);
        buildBfUseGroup.emplace_back(bfUse);
      }
      buildBfUseSingleNode.emplace_back(buildBfUseGroup);
      // connect this group of bf to its input
      PrePToPTransformerUtil::connectOneToOne(buildBroadcastSingleNode, buildBfUseGroup);
      // connect either "probeBfFinalizes" (local) or "probeBroadcasts" (remote) to this group of bf
      if (i == group) {
        bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(probeBfFinalizes[i][i], buildBfUseGroup, {});
      } else {
        PrePToPTransformerUtil::connectOneToMany(probeBroadcasts[i][group], buildBfUseGroup);
      }
    }
    buildBfUses.emplace_back(buildBfUseSingleNode);
#if SHOW_DEBUG_METRICS == true
    for (const auto &op: buildBroadcastSingleNode) {
      op->setCollPredTransCSMetrics(probePTCSMetricsInfo);
    }
    for (const auto &opVec: buildBfUseSingleNode) {
      for (const auto &op: opVec) {
        op->setCollPredTransCSMetrics(probePTCSMetricsInfo);
      }
    }
#endif
  }

  /// step 4 and 5: post-probe
  /// construct BFs using reduced join key vals of the build table, then send them back to their original node, and
  /// use these BFs reduce the probe table, in each node, grouped by shuffling
  makeOnePairFilterOpsDistPtionPostProbeOneSideBf(numNodes, dirSbl, step, hashJoinPredicateStr,
                                                  buildColumns, probeColumns, filterProbe, newOps,
                                                  buildBfUses, bfParallelPerGroup, probeShuffles,
                                                  ptCSMetricsInfoBase);
}

void makeOnePairFilterOpsDistSemiJoinRed(
        int numNodes, int parallelDegree,
        const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
        POpVec& in, POpVec& out,
        std::vector<POpVec>&, std::vector<POpVec>& filterProbe,
        POpVec& newOps, const metrics::PredTransCSMetrics::PTCSMetricsInfo&,
        bool) {
  POpVec buildSplits;
  std::vector<POpVec> buildShuffles, probeShuffles;
  for (int i = 0; i < numNodes; ++i) {
    // project to collect join key vals of build table in each node
    std::vector<std::pair<std::string, std::string>> buildProjectColumnPairs;
    for (const auto &buildColumn: buildColumns) {
      buildProjectColumnPairs.emplace_back(std::make_pair(buildColumn, buildColumn));
    }
    std::shared_ptr<PhysicalOp> buildProject = std::make_shared<project::ProjectPOp>(
            fmt::format("Project({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            buildColumns,
            i,
            std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{},
            std::vector<std::string>{},
            buildProjectColumnPairs);
    in.emplace_back(buildProject);
    newOps.emplace_back(buildProject);
    // split for the build table after collecting join key vals from all projects in each node
    std::shared_ptr<PhysicalOp> buildSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    buildSplits.emplace_back(buildSplit);
    newOps.emplace_back(buildSplit);
    // split for the probe table in each node
    std::shared_ptr<PhysicalOp> probeSplit = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, i),
            std::vector<std::string>{} /*unused*/,
            i);
    out.emplace_back(probeSplit);
    newOps.emplace_back(probeSplit);
    // shuffle and hash join
    POpVec buildShuffleSingleNode, probeShuffleSingleNode, hashJoinSingleNode;
    for (int j = 0; j < parallelDegree; ++j) {
      int opId = j * numNodes + i;
      buildShuffleSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-build-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              buildColumns));
      probeShuffleSingleNode.emplace_back(std::make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle({})<{}>-{}-probe-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{}, /*unused*/
              i,
              probeColumns)
              );
      hashJoinSingleNode.emplace_back(std::make_shared<join::HashJoinArrowPOp>(
              fmt::format("HashJoinArrow({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, opId),
              std::vector<std::string>{} /*we set this later in caller `makeOnePairFilterOpsDist()`*/,
              i,
              join::HashJoinPredicate(buildColumns, probeColumns),
              JoinType::RIGHT_SEMI));
    }
    buildShuffles.emplace_back(buildShuffleSingleNode);
    probeShuffles.emplace_back(probeShuffleSingleNode);
    filterProbe.emplace_back(hashJoinSingleNode);
    newOps.insert(newOps.end(), buildShuffleSingleNode.begin(), buildShuffleSingleNode.end());
    newOps.insert(newOps.end(), probeShuffleSingleNode.begin(), probeShuffleSingleNode.end());
    newOps.insert(newOps.end(), hashJoinSingleNode.begin(), hashJoinSingleNode.end());
  }
  // connect `in_` (buildProject) to `buildSplits`
  PrePToPTransformerUtil::connectManyToMany(in, buildSplits);
  // connect `buildSplits` to `buildShuffles` and `out_` (probeSplit) to `probeShuffles`
  for (int i = 0; i < numNodes; ++i) {
    PrePToPTransformerUtil::connectOneToMany(buildSplits[i], buildShuffles[i]);
    PrePToPTransformerUtil::connectOneToMany(out[i], probeShuffles[i]);
  }
  // connect `buildShuffles` and `probeShuffles` to `filterProbe` (probeHashJoin)
  for (int i = 0; i < numNodes; ++i) {
    const auto &buildShuffleSingleNode = buildShuffles[i];
    const auto &probeShuffleSingleNode = probeShuffles[i];
    const auto &hashJoinSingleNode = filterProbe[i];
    for (int j = 0; j < parallelDegree; ++j) {
      for (int k = 0; k < parallelDegree; ++k) {
        std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoinSingleNode[k])
                ->addBuildProducer(buildShuffleSingleNode[j]);
        std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoinSingleNode[k])
                ->addProbeProducer(probeShuffleSingleNode[j]);
        buildShuffleSingleNode[j]->produce(hashJoinSingleNode[k]);
        probeShuffleSingleNode[j]->produce(hashJoinSingleNode[k]);
      }
    }
  }
}

void SmallToLargePredTransOrder::makeOnePairFilterOpsDist(
        const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
        POpVec& in, POpVec& out,
        std::vector<POpVec>& filterBuild, std::vector<POpVec>& filterProbe,
        POpVec& newOps,
        const std::shared_ptr<PredTransUnit> &leftPTUnit, const std::shared_ptr<PredTransUnit> &rightPTUnit) {
  std::function<void(int numNodes, int parallelDegree,
                     const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
                     const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
                     POpVec& in, POpVec& out,
                     std::vector<POpVec>& filterBuild, std::vector<POpVec>& filterProbe,
                     POpVec& newOps, const metrics::PredTransCSMetrics::PTCSMetricsInfo&,
                     bool)> makeOnePairFilterOpsFunc;
  DistPredTransType actualType = DIST_PRED_TRANS_TYPE;
  bool recordCard = false;
  if (actualType == DistPredTransType::ADAPT) {
    bool forward = (dirSbl == "F") ? true : false;
    auto expBuildCard = ((Executor*)(transformer_->executor_))->cardCache_
                          .consume(cache::PredTransCardCache::PredTransCardKey(step, forward, true, true));
    auto expProbeCard = ((Executor*)(transformer_->executor_))->cardCache_
                          .consume(cache::PredTransCardCache::PredTransCardKey(step, forward, false, true));
    auto expProbeOutCard = ((Executor*)(transformer_->executor_))->cardCache_
                             .consume(cache::PredTransCardCache::PredTransCardKey(step, forward, false, false));
    // distinguish real adapt and simluation with "USE_DOUBLE_EXEC_ADAPT
    if (USE_DOUBLE_EXEC_ADAPT) {
      // if cardinalities is recorded, then determine the dist-PT strategy based on them,
      // otherwise record them in this exec, now use "BCAST_VAL" to record.
      if (expBuildCard.has_value() && expProbeCard.has_value() && expProbeOutCard.has_value()) {
        int64_t numBuildRows = (*expBuildCard).card_;
        auto joinKeyLen = (*expBuildCard).joinKeyLen_;
        if (!joinKeyLen.has_value()) {
          throw std::runtime_error("Join key length not collected in adaptive dist-PT with 'USE_DOUBLE_EXEC_ADAPT' set.");
        }
        int64_t numProbeRows = (*expProbeCard).card_;
        int64_t numProbeRowsOut = (*expProbeOutCard).card_;
        actualType = DistPredTransTypeUtil::optimize(numBuildRows, numProbeRows, numProbeRowsOut,
                                                     *joinKeyLen, transformer_->numNodes_);
      } else {
        actualType = DistPredTransType::BCAST_VAL;
        recordCard = true;
      }
      // record selected dist-PT type if need to show it
      if (metrics::SHOW_DIST_PRED_TRANS_TYPE) {
        std::string typeSuffix = recordCard ? " (recordCard)" : "";
        adaptDistPTTypes_.selections_[{forward, step}] =
          {DistPredTransTypeUtil::toStepDigest(dirSbl, step, hashJoinPredicateStr),
           DistPredTransTypeUtil::toString(actualType) + typeSuffix};
      }
    } else {
      // require only build card, join key len and probe card,
      // since it's impossible to get probe out card before PT really happens.
      if (!expBuildCard.has_value()) {
        throw std::runtime_error(expBuildCard.error());
      }
      int64_t numBuildRows = (*expBuildCard).card_;
      auto joinKeyLen = (*expBuildCard).joinKeyLen_;
      if (!joinKeyLen.has_value()) {
        throw std::runtime_error("Join key length not collected in adaptive dist-PT.");
      }
      if (!expProbeCard.has_value()) {
        throw std::runtime_error(expProbeCard.error());
      }
      int64_t numProbeRows = (*expProbeCard).card_;
      actualType = DistPredTransTypeUtil::optimize(numBuildRows, numProbeRows, std::nullopt,
                                                   *joinKeyLen, transformer_->numNodes_);
    }
  }
  switch (actualType) {
    case DistPredTransType::BCAST_VAL: {
      makeOnePairFilterOpsFunc = makeOnePairFilterOpsDistBcastVal;
      break;
    }
    case DistPredTransType::BCAST_BF: {
      makeOnePairFilterOpsFunc = makeOnePairFilterOpsDistBcastBf;
      break;
    }
    case DistPredTransType::PTION_VAL: {
      makeOnePairFilterOpsFunc = makeOnePairFilterOpsDistPtionVal;
      break;
    }
    case DistPredTransType::PTION_SRC_BF_DST_VAL: {
      makeOnePairFilterOpsFunc = makeOnePairFilterOpsDistPtionSrcBfDstVal;
      break;
    }
    case DistPredTransType::PTION_SRC_VAL_DST_BF: {
      makeOnePairFilterOpsFunc = makeOnePairFilterOpsDistPtionSrcValDstBf;
      break;
    }
    case DistPredTransType::SEMI_JOIN_RED: {
      makeOnePairFilterOpsFunc = makeOnePairFilterOpsDistSemiJoinRed;
      break;
    }
    case DistPredTransType::ADAPT: {
      // handled above before this switch block
      break;
    }
  }
  metrics::PredTransCSMetrics::PTCSMetricsInfo pTCSMetricsInfoBase(
        dirSbl == "F", step,
        leftPTUnit->base_->prePOpId_, rightPTUnit->base_->prePOpId_,
        plan::prephysical::Util::getBaseTableDigest(leftPTUnit->prePOp_),
        plan::prephysical::Util::getBaseTableDigest(rightPTUnit->prePOp_),
        DistPredTransTypeUtil::toString(actualType),
        metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::UNKNOWN);
  makeOnePairFilterOpsFunc(transformer_->numNodes_, transformer_->parallelDegree_,
                           dirSbl, step, hashJoinPredicateStr,
                           buildColumns, probeColumns,
                           in, out, filterBuild, filterProbe,
                           newOps, pTCSMetricsInfoBase, recordCard);
  if (actualType == DistPredTransType::SEMI_JOIN_RED) {
    // set `projectColumnNames_` for filterProbe (`HashJoinArrowPOp`)
    const auto &probeProjectColumnSet = dirSbl == "F" ?
            rightPTUnit->prePOp_->getProjectColumnNames() :
            leftPTUnit->prePOp_->getProjectColumnNames();
    std::vector<std::string> probeProjectColumns(
            probeProjectColumnSet.begin(), probeProjectColumnSet.end());
    for (const auto &opVec: filterProbe) {
      for (const auto &op: opVec) {
        op->setProjectColumnNames(probeProjectColumns);
      }
    }
  }
}

}
