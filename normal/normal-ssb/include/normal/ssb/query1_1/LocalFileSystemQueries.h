//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H


#include <memory>

#include <normal/core/OperatorManager.h>

using namespace normal::core;


namespace normal::ssb::query1_1 {

/**
 * Partial and full query definitions for SSB query 1.1 as Normal execution plans
 */
class LocalFileSystemQueries {

public:

  static std::shared_ptr<OperatorManager> dateScan(const std::string &dataDir,
												   int numConcurrentUnits);

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
