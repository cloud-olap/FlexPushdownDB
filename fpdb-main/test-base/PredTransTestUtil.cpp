//
// Created by Yifei Yang on 5/3/23.
//

#include "PredTransTestUtil.h"
#include "TestUtil.h"
#include "Globals.h"
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/plan/Globals.h>
#include <fpdb/util/Color.h>
#include <doctest/doctest.h>
#include <limits>

namespace fpdb::main::test {

void PredTransTestUtil::testPredTrans(const std::string &schemaName, const std::string &queryFileName,
                                      int parallelDegree, bool enablePredTrans,
                                      bool enableYannakakis, bool useHeuristicJoinOrdering) {
  // start local FPDB store server and set flags
  TestUtil::startFPDBStoreServer();
  bool oldEnablePredTrans = fpdb::plan::ENABLE_PRED_TRANS;
  bool oldEnableYannakakis = fpdb::executor::physical::ENABLE_YANNAKAKIS;
  PredTransOrderType oldPredTransOrderType = fpdb::executor::physical::PRED_TRANS_ORDER_TYPE;
  bool oldShowPredTransMetrics = fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS;
  fpdb::plan::ENABLE_PRED_TRANS = enablePredTrans;
  fpdb::executor::physical::ENABLE_YANNAKAKIS = enableYannakakis;
  if (enableYannakakis) {
    // Yannakakis only works on BFS pred-trans order
    fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = PredTransOrderType::BFS;
  }
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = true;

  // exec
  REQUIRE(TestUtil::e2eNoStartCalciteServerSingleNode(schemaName,
                                                      {queryFileName, queryFileName},
                                                      ObjStoreType::FPDB_STORE,
                                                      parallelDegree,
                                                      Mode::cachingOnlyMode(),
                                                      CachingPolicyType::LFU,
                                                      std::numeric_limits<size_t>::max(),
                                                      useHeuristicJoinOrdering));

  // stop local FPDB store server and reset flags
  TestUtil::stopFPDBStoreServer();
  fpdb::plan::ENABLE_PRED_TRANS = oldEnablePredTrans;
  fpdb::executor::physical::ENABLE_YANNAKAKIS = oldEnableYannakakis;
  if (enableYannakakis) {
    fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = oldPredTransOrderType;
  }
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = oldShowPredTransMetrics;
}

void PredTransTestUtil::testPredTransDist(const std::string &schemaName, const std::string &queryFileName,
                                          int parallelDegree, bool enablePredTrans,
                                          bool useHeuristicJoinOrdering) {
  // set flags
  bool oldEnablePredTrans = fpdb::plan::ENABLE_PRED_TRANS;
  bool oldEnableYannakakis = fpdb::executor::physical::ENABLE_YANNAKAKIS;
  bool oldShowPredTransMetrics = fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS;
  bool oldShowNetworkMetrics = fpdb::executor::metrics::SHOW_NETWORK_METRICS;
  auto oldPredTransOrderType = fpdb::executor::physical::PRED_TRANS_ORDER_TYPE;
  fpdb::plan::ENABLE_PRED_TRANS = enablePredTrans;
  fpdb::executor::physical::ENABLE_YANNAKAKIS = false;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = true;
  fpdb::executor::metrics::SHOW_NETWORK_METRICS = true;
  fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = PredTransOrderType::SMALL_TO_LARGE;

  // exec
  REQUIRE(TestUtil::e2eNoStartCalciteServer(schemaName,
                                            {queryFileName, queryFileName},
                                            parallelDegree,
                                            true,
                                            ObjStoreType::S3,
                                            Mode::cachingOnlyMode(),
                                            CachingPolicyType::LFU,
                                            std::numeric_limits<size_t>::max(),
                                            useHeuristicJoinOrdering));

  // reset flags
  fpdb::plan::ENABLE_PRED_TRANS = oldEnablePredTrans;
  fpdb::executor::physical::ENABLE_YANNAKAKIS = oldEnableYannakakis;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = oldShowPredTransMetrics;
  fpdb::executor::metrics::SHOW_NETWORK_METRICS = oldShowNetworkMetrics;
  fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = oldPredTransOrderType;
}

void PredTransTestUtil::testPredTransVaryThreads(
        const std::string &schemaName, const std::string &queryFileName,
        std::vector<int> numThreadsVec, bool enablePredTrans, bool useHeuristicJoinOrdering) {
  // start local FPDB store server and set flags
  TestUtil::startFPDBStoreServer();
  bool oldEnablePredTrans = fpdb::plan::ENABLE_PRED_TRANS;
  bool oldEnableYannakakis = fpdb::executor::physical::ENABLE_YANNAKAKIS;
  PredTransOrderType oldPredTransOrderType = fpdb::executor::physical::PRED_TRANS_ORDER_TYPE;
  bool oldShowOpTypeTime = fpdb::executor::metrics::SHOW_OP_TYPE_TIME;
  fpdb::plan::ENABLE_PRED_TRANS = enablePredTrans;
  fpdb::executor::physical::ENABLE_YANNAKAKIS = false;
  fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = PredTransOrderType::SMALL_TO_LARGE;
  fpdb::executor::metrics::SHOW_OP_TYPE_TIME = true;

  // create a cache content to be reused
  auto cache = SegmentCache::make(std::make_shared<LFUCachingPolicy>(std::numeric_limits<size_t>::max(), nullptr));

  // exec, add a caching exec at the beginning
  numThreadsVec.insert(numThreadsVec.begin(), std::thread::hardware_concurrency());
  for (uint i = 0; i < numThreadsVec.size(); ++i) {
    int numThreads = numThreadsVec[i];
    if (i == 0) {
      std::cout << BLUE << "Caching:" << RESET << std::endl;
    } else {
      std::cout << BLUE << fmt::format("Num threads: {}", numThreads) << RESET << std::endl;
    }
    TestUtil testUtil(schemaName, {queryFileName}, numThreads, false,
                      ObjStoreType::FPDB_STORE, Mode::cachingOnlyMode(),
                      CachingPolicyType::LFU /* unused */, std::numeric_limits<size_t>::max() /* unused */);
    testUtil.setNumThreads(numThreads);
    testUtil.setUseHeuristicJoinOrdering(useHeuristicJoinOrdering);
    testUtil.setCache(cache);
    REQUIRE_NOTHROW(testUtil.runTest());
  }

  // stop local FPDB store server and reset flags
  TestUtil::stopFPDBStoreServer();
  fpdb::plan::ENABLE_PRED_TRANS = oldEnablePredTrans;
  fpdb::executor::physical::ENABLE_YANNAKAKIS = oldEnableYannakakis;
  fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = oldPredTransOrderType;
  fpdb::executor::metrics::SHOW_OP_TYPE_TIME = oldShowOpTypeTime;
}

void PredTransTestUtil::testLIP(const std::string &schemaName, const std::string &queryFileName,
                                int parallelDegree, bool useHeuristicJoinOrdering) {
  // start local FPDB store server and set flags
  TestUtil::startFPDBStoreServer();
  bool oldEnablePredTrans = fpdb::plan::ENABLE_PRED_TRANS;
  PredTransOrderType oldPredTransOrderType = fpdb::executor::physical::PRED_TRANS_ORDER_TYPE;
  bool oldShowPredTransMetrics = fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS;
  fpdb::plan::ENABLE_PRED_TRANS = true;
  fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = PredTransOrderType::LIP;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = true;

  // exec
  REQUIRE(TestUtil::e2eNoStartCalciteServerSingleNode(schemaName,
                                                      {queryFileName, queryFileName},
                                                      ObjStoreType::FPDB_STORE,
                                                      parallelDegree,
                                                      Mode::cachingOnlyMode(),
                                                      CachingPolicyType::LFU,
                                                      std::numeric_limits<size_t>::max(),
                                                      useHeuristicJoinOrdering));

  // stop local FPDB store server and reset flags
  TestUtil::stopFPDBStoreServer();
  fpdb::plan::ENABLE_PRED_TRANS = oldEnablePredTrans;
  fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = oldPredTransOrderType;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = oldShowPredTransMetrics;
}

void PredTransTestUtil::testLIPDist(const std::string &schemaName, const std::string &queryFileName,
                                    int parallelDegree, bool useHeuristicJoinOrdering) {
  // set flags
  bool oldEnablePredTrans = fpdb::plan::ENABLE_PRED_TRANS;
  auto oldPredTransOrderType = fpdb::executor::physical::PRED_TRANS_ORDER_TYPE;
  bool oldShowPredTransMetrics = fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS;
  bool oldShowNetworkMetrics = fpdb::executor::metrics::SHOW_NETWORK_METRICS;
  fpdb::plan::ENABLE_PRED_TRANS = true;
  fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = PredTransOrderType::LIP;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = true;
  fpdb::executor::metrics::SHOW_NETWORK_METRICS = true;

  // exec
  REQUIRE(TestUtil::e2eNoStartCalciteServer(schemaName,
                                            {queryFileName, queryFileName},
                                            parallelDegree,
                                            true,
                                            ObjStoreType::S3,
                                            Mode::cachingOnlyMode(),
                                            CachingPolicyType::LFU,
                                            std::numeric_limits<size_t>::max(),
                                            useHeuristicJoinOrdering));

  // reset flags
  fpdb::plan::ENABLE_PRED_TRANS = oldEnablePredTrans;
  fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = oldPredTransOrderType;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = oldShowPredTransMetrics;
  fpdb::executor::metrics::SHOW_NETWORK_METRICS = oldShowNetworkMetrics;
}

std::vector<int> PredTransTestUtil::genNumThreadsVec(int start) {
  std::vector<int> vec;
  for (int i = start; i <= (int) std::thread::hardware_concurrency(); ++i) {
    vec.emplace_back(i);
  }
  return vec;
}

}
