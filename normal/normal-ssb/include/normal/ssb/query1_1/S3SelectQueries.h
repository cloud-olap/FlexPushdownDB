//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_S3SELECTQUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_S3SELECTQUERIES_H

#include <memory>

#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/AWSClient.h>

using namespace normal::core;
using namespace normal::core::graph;
using namespace normal::pushdown;

namespace normal::ssb::query1_1 {

/**
 * Partial and full query definitions for SSB query 1.1 as Normal execution plans using S3 data sources
 */
class S3SelectQueries {

public:

  static std::shared_ptr<OperatorGraph> dateScanPullUp(const std::string &s3Bucket,
														 const std::string &s3ObjectDir,
														 int numConcurrentUnits,
														 AWSClient &client,
														 const std::shared_ptr<OperatorManager> &mgr);

  static std::shared_ptr<OperatorGraph> lineOrderScanPullUp(const std::string &s3Bucket,
															  const std::string &s3ObjectDir,
															  int numConcurrentUnits,
															  AWSClient &client,
															  const std::shared_ptr<OperatorManager> &mgr);

  static std::shared_ptr<OperatorGraph> dateFilterPullUp(const std::string &s3Bucket,
														   const std::string &s3ObjectDir,
														   short year,
														   int numConcurrentUnits,
														   AWSClient &client,
														   const std::shared_ptr<OperatorManager> &mgr);

  static std::shared_ptr<OperatorGraph> lineOrderFilterPullUp(const std::string &s3Bucket,
																const std::string &s3ObjectDir,
																short discount,
																short quantity,
																int numConcurrentUnits,
																AWSClient &client,
																const std::shared_ptr<OperatorManager> &mgr);

  static std::shared_ptr<OperatorGraph> joinPullUp(const std::string &s3Bucket,
													 const std::string &s3ObjectDir,
													 short year,
													 short discount,
													 short quantity,
													 int numConcurrentUnits,
													 AWSClient &client,
													 const std::shared_ptr<OperatorManager> &mgr);

  static std::shared_ptr<OperatorGraph> fullPullUp(const std::string &s3Bucket,
													 const std::string &s3ObjectDir,
													 short year,
													 short discount,
													 short quantity,
													 int numConcurrentUnits,
													 AWSClient &client,
													 const std::shared_ptr<OperatorManager> &mgr);

  static std::shared_ptr<OperatorGraph> fullPushDown(const std::string &s3Bucket,
													   const std::string &s3ObjectDir,
													   short year,
													   short discount,
													   short quantity,
													   int numPartitions,
													   AWSClient &client,
													   const std::shared_ptr<OperatorManager> &mgr);

  static std::shared_ptr<OperatorGraph> fullHybrid(const std::string &s3Bucket,
													 const std::string &s3ObjectDir,
													 short year,
													 short discount,
													 short quantity,
													 int numPartitions,
													 AWSClient &client,
													 const std::shared_ptr<OperatorManager> &mgr);
};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_S3SELECTQUERIES_H
