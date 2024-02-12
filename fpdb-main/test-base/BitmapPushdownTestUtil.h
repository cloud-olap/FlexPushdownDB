//
// Created by Yifei Yang on 12/6/22.
//

#ifndef FPDB_FPDB_MAIN_TEST_BASE_BITMAPPUSHDOWNTESTUTIL_H
#define FPDB_FPDB_MAIN_TEST_BASE_BITMAPPUSHDOWNTESTUTIL_H

#include <unordered_map>
#include <vector>
#include <string>

namespace fpdb::main::test {

class BitmapPushdownTestUtil {

public:
  static void run_bitmap_pushdown_benchmark_query(const std::string &cachingQuery,
                                                  const std::vector<std::string> &testQueries,
                                                  const std::vector<std::string> &testQueryFileNames,
                                                  bool isSsb,
                                                  const std::string &sf,
                                                  int parallelDegree,
                                                  bool startFPDBStore);

private:
  static void set_pushdown_flags(std::unordered_map<std::string, bool> &flags);
  static void reset_pushdown_flags(std::unordered_map<std::string, bool> &flags);
};

}


#endif //FPDB_FPDB_MAIN_TEST_BASE_BITMAPPUSHDOWNTESTUTIL_H
