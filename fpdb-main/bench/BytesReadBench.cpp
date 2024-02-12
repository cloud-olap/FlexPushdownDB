//
// Created by Yifei Yang on 3/14/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"
#include <fpdb/util/Util.h>
#include <cstdio>

using namespace fpdb::util;

/**
 * Test number of bytes read from parquet for pullup and pushdown-only, varying selectivity
 * Using the same setup as TPCHFPDBStoreDiffNodeTest
 * Start Calcite server and FPDB store server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("bytes-read-bench" * doctest::skip(SKIP_SUITE)) {

constexpr std::string_view bytesReadTestQueryFileNameBase = "bytes_read_test_{}.sql";

void writeQueryToFileBytesReadTest(const std::string queryFileName, double l_discount) {
  std::string query = fmt::format("select\n"
                                  "    l_returnflag,\n"
                                  "    l_linestatus,\n"
                                  "    l_quantity,\n"
                                  "    l_extendedprice\n"
                                  "from\n"
                                  "\tlineitem\n"
                                  "where\n"
                                  "\tl_discount <= {}\n"
                                  "order by\n"
                                  "    l_returnflag,\n"
                                  "    l_linestatus\n"
                                  "limit 10",
                                  l_discount);
  TestUtil::writeQueryToFile(queryFileName, query);
}

TEST_CASE ("bytes-read-tpch-sf10-fpdb-store-diff-node" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<std::shared_ptr<Mode>> modes{Mode::pullupMode(), Mode::pushdownOnlyMode()};

  for (const auto &mode: modes) {
    for (double l_discount = 0.00; l_discount <= 0.1; l_discount += 0.01) {
      std::stringstream ss;
      ss << std::fixed << std::setprecision(2) << l_discount;
      std::string queryFileName = fmt::format(bytesReadTestQueryFileNameBase, ss.str());
      writeQueryToFileBytesReadTest(queryFileName, l_discount);
      REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                                {std::string(queryFileName)},
                                                PARALLEL_FPDB_STORE_DIFF_NODE,
                                                false,
                                                ObjStoreType::FPDB_STORE,
                                                mode));
      TestUtil::removeQueryFile(queryFileName);
    }
  }
}

}

}
