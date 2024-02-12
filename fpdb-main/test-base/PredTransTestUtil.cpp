//
// Created by Yifei Yang on 5/3/23.
//

#include "PredTransTestUtil.h"
#include "TestUtil.h"
#include "Globals.h"
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/plan/Globals.h>
#include <doctest/doctest.h>
#include <limits>

namespace fpdb::main::test {

void PredTransTestUtil::testPredTrans(const std::string &schemaName, const std::string &queryFileName,
                                      bool enablePredTrans, bool enableYannakakis,
                                      bool useHeuristicJoinOrdering) {
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

  REQUIRE(TestUtil::e2eNoStartCalciteServerSingleThread(schemaName,
                                                        {queryFileName, queryFileName},
                                                        PARALLEL_PRED_TRANS,
                                                        false,
                                                        ObjStoreType::FPDB_STORE,
                                                        Mode::cachingOnlyMode(),
                                                        CachingPolicyType::LFU,
                                                        std::numeric_limits<size_t>::max(),
                                                        useHeuristicJoinOrdering));

  TestUtil::stopFPDBStoreServer();
  fpdb::plan::ENABLE_PRED_TRANS = oldEnablePredTrans;
  fpdb::executor::physical::ENABLE_YANNAKAKIS = oldEnableYannakakis;
  if (enableYannakakis) {
    fpdb::executor::physical::PRED_TRANS_ORDER_TYPE = oldPredTransOrderType;
  }
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = oldShowPredTransMetrics;
}

}
