//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H

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

using namespace normal::core;
using namespace normal::pushdown;
using namespace normal::pushdown::filter;
using namespace normal::pushdown::join;

namespace normal::ssb::query1_1 {

class LocalFileSystemQueries {

public:

  static std::shared_ptr<OperatorManager> dateScanQuery(const std::string &dataDir,
															 int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> lineOrderScanQuery(const std::string &dataDir,
															 int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> dateFilterQuery(const std::string &dataDir,
														  short year,
														  int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> lineOrderFilterQuery(const std::string &dataDir,
															   short discount,
															   short quantity,
															   int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> joinQuery(const std::string &dataDir,
													short year,
													short discount,
													short quantity,
													int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> fullQuery(const std::string &dataDir,
													short year,
													short discount,
													short quantity,
													int numConcurrentUnits);
};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H
