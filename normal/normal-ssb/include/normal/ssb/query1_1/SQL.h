//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_SQL_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_SQL_H

#include <normal/core/OperatorManager.h>
#include <normal/tuple/TupleSet.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/filter/Filter.h>
#include <normal/pushdown/Collate.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <memory>

using namespace normal::core;
using namespace normal::pushdown;
using namespace normal::pushdown::filter;
using namespace normal::pushdown::join;

namespace normal::ssb::query1_1 {

/**
 * SQL queries for SSB query 1.1
 *
 * Includes partial queries
 */
class SQL {

public:
  static std::string query1_1SQLite(short year, short discount, short quantity, const std::string &catalogue);
  static std::string query1_1LineOrderScanSQLite(const std::string &catalogue);
  static std::string query1_1DateFilterSQLite(short year, const std::string &catalogue);
  static std::string query1_1LineOrderFilterSQLite(short discount, short quantity, const std::string &catalogue);
  static std::string query1_1JoinSQLite(short year, short discount, short quantity, const std::string &catalogue);

};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_SQL_H
