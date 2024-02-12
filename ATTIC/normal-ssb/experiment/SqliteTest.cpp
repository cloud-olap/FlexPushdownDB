//
// Created by Yifei Yang on 8/6/20.
//

//#include <normal/ssb/Globals.h>
//#include <doctest/doctest.h>
//#include <experimental/filesystem>
//#include <normal/ssb/TestUtil.h>
//
//#define SKIP_SUITE true
//
//using namespace normal::ssb;
//
//TEST_SUITE ("SqliteTest" * doctest::skip(SKIP_SUITE)) {
//
//TEST_CASE ("JoinTest" * doctest::skip(false || SKIP_SUITE)) {
//  std::string sql = fmt::format(
//          "select sum(lo_revenue), d_year, p_brand1"
//          " from lineorder,\n"
//          "     date,\n"
//          "     part,\n"
//          "     supplier\n"
//          " where lo_orderdate = d_datekey\n"
//          "  and lo_partkey = p_partkey\n"
//          "  and lo_suppkey = s_suppkey\n"
//          "  and p_category = 'MFGR#12'\n"
//          "  and s_region = 'AMERICA'\n"
//          " group by d_year, p_brand1",
//          " temp");
//  std::string dataDir = "data/ssb-sf0.01";
//  std::vector<std::string> dataFiles = {std::experimental::filesystem::absolute(dataDir + "/date.tbl"),
//                std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl"),
//                std::experimental::filesystem::absolute(dataDir + "/part.tbl"),
//                std::experimental::filesystem::absolute(dataDir + "/supplier.tbl"),
//                std::experimental::filesystem::absolute(dataDir + "/customer.tbl")};
//  auto expected = TestUtil::executeSQLite(sql, dataFiles);
//  std::string strRes;
//  for (const auto &strVec: *expected) {
//    for (const auto &strPair: strVec) {
//      strRes += strPair.first + " " + strPair.second + "\t";
//    }
//    strRes += "\n";
//  }
//  SPDLOG_INFO("Result: \n{}", strRes);
//}
//}