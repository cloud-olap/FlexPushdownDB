//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H

#include <memory>

#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>

using namespace normal::core;
using namespace normal::core::graph;

namespace normal::ssb::query1_1 {

/**
 * Partial and full query definitions for SSB query 1.1 as Normal execution plans using local file system data sources
 */
class LocalFileSystemQueries {

public:

  static std::shared_ptr<OperatorManager> dateScan(const std::string &dataDir,
												   int numConcurrentUnits);

  static std::shared_ptr<OperatorGraph> dateScan2(const std::string &dataDir,
												   int numConcurrentUnits,
												  std::shared_ptr<OperatorManager>);

  static std::shared_ptr<OperatorManager> lineOrderScan(const std::string &dataDir,
														int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> dateFilter(const std::string &dataDir,
													 short year,
													 int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> lineOrderFilter(const std::string &dataDir,
														  short discount,
														  short quantity,
														  int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> join(const std::string &dataDir,
											   short year,
											   short discount,
											   short quantity,
											   int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> full(const std::string &dataDir,
											   short year,
											   short discount,
											   short quantity,
											   int numConcurrentUnits);
};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H
