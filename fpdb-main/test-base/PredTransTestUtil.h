//
// Created by Yifei Yang on 5/3/23.
//

#ifndef FPDB_FPDB_MAIN_TEST_BASE_PREDTRANSTESTUTIL_H
#define FPDB_FPDB_MAIN_TEST_BASE_PREDTRANSTESTUTIL_H

#include <string>
#include <vector>

namespace fpdb::main::test {

class PredTransTestUtil {

public:
  /**
   * Single-node execution
   * Run twice with "caching-only" mode, measure the second run where tables are all in memory cache
   * if "enablePredTrans" is set to false, then it's measuring the baseline
   * Currently do not support Yannakakis on parallel exec (BFS ordering not supported)
   */
  static void testPredTrans(const std::string &schemaName, const std::string &queryFileName,
                            int parallelDegree, bool enablePredTrans = true,
                            bool enableYannakakis = false, bool useHeuristicJoinOrdering = true);
  /**
   * Distributed execution
   * Run twice with "caching-only" mode, measure the second run where tables are all in memory cache
   * if "enablePredTrans" is set to false, then it's measuring the baseline
   * Currently do not support Yannakakis (BFS ordering not supported)
   */
  static void testPredTransDist(const std::string &schemaName, const std::string &queryFileName,
                                int parallelDegree, bool enablePredTrans = true,
                                bool useHeuristicJoinOrdering = true);

  /**
   * Run single-node parallel execution of predicate transfer with different num threads used
   */
  static void testPredTransVaryThreads(const std::string &schemaName, const std::string &queryFileName,
                                       std::vector<int> numThreadsVec, bool enablePredTrans = true,
                                       bool useHeuristicJoinOrdering = true);
  /**
   * Single-node execution for LIP-style pred-trans
   * Run twice with "caching-only" mode, measure the second run where tables are all in memory cache
   */
  static void testLIP(const std::string &schemaName, const std::string &queryFileName,
                      int parallelDegree, bool useHeuristicJoinOrdering = true);

  /**
   * Distributed execution
   * Run twice with "caching-only" mode, measure the second run where tables are all in memory cache
   */
  static void testLIPDist(const std::string &schemaName, const std::string &queryFileName,
                          int parallelDegree, bool useHeuristicJoinOrdering = true);

  /**
   * Generate a vector from 1 to max_threads
   */
  static std::vector<int> genNumThreadsVec(int start = 1);
};

}


#endif //FPDB_FPDB_MAIN_TEST_BASE_PREDTRANSTESTUTIL_H
