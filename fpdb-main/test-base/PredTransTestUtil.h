//
// Created by Yifei Yang on 5/3/23.
//

#ifndef FPDB_FPDB_MAIN_TEST_BASE_PREDTRANSTESTUTIL_H
#define FPDB_FPDB_MAIN_TEST_BASE_PREDTRANSTESTUTIL_H

#include <string>

namespace fpdb::main::test {

class PredTransTestUtil {

public:
  /**
   * Single thread execution
   * Run twice with "caching-only" mode, measure the second run where tables are all in memory cache
   * if "enablePredTrans" is set to false, then it's measuring the baseline
   */
  static void testPredTrans(const std::string &schemaName, const std::string &queryFileName,
                            bool enablePredTrans = true, bool enableYannakakis = false,
                            bool useHeuristicJoinOrdering = true);
};

}


#endif //FPDB_FPDB_MAIN_TEST_BASE_PREDTRANSTESTUTIL_H
