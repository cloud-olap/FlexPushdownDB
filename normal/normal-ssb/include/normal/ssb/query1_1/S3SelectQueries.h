//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_S3SELECTQUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_S3SELECTQUERIES_H

#include <memory>

#include <normal/core/OperatorManager.h>
#include <normal/pushdown/AWSClient.h>

using namespace normal::core;
using namespace normal::pushdown;

namespace normal::ssb::query1_1 {

/**
 * Partial and full query definitions for SSB query 1.1 as Normal execution plans using S3 data sources
 */
class S3SelectQueries {

public:

  static std::shared_ptr<OperatorManager> dateScanPullUp(const std::string &s3Bucket,
														 const std::string &s3ObjectDir,
														 int numConcurrentUnits,
														 AWSClient &client);

  static std::shared_ptr<OperatorManager> lineOrderScanPullUp(const std::string &s3Bucket,
															  const std::string &s3ObjectDir,
															  int numConcurrentUnits,
															  AWSClient &client);

  static std::shared_ptr<OperatorManager> dateFilterPullUp(const std::string &s3Bucket,
														   const std::string &s3ObjectDir,
														   short year,
														   int numConcurrentUnits,
														   AWSClient &client);

  static std::shared_ptr<OperatorManager> lineOrderFilterPullUp(const std::string &s3Bucket,
																const std::string &s3ObjectDir,
																short discount,
																short quantity,
																int numConcurrentUnits,
																AWSClient &client);

  static std::shared_ptr<OperatorManager> joinPullUp(const std::string &s3Bucket,
													 const std::string &s3ObjectDir,
													 short year,
													 short discount,
													 short quantity,
													 int numConcurrentUnits,
													 AWSClient &client);

  static std::shared_ptr<OperatorManager> fullPullUp(const std::string &s3Bucket,
													 const std::string &s3ObjectDir,
													 short year,
													 short discount,
													 short quantity,
													 int numConcurrentUnits,
													 AWSClient &client);

  static std::shared_ptr<OperatorManager> fullPushDown(const std::string &s3Bucket,
													   const std::string &s3ObjectDir,
													   short year,
													   short discount,
													   short quantity,
													   int numPartitions,
													   AWSClient &client);

  static std::shared_ptr<OperatorManager> fullHybrid(const std::string &s3Bucket,
													 const std::string &s3ObjectDir,
													 short year,
													 short discount,
													 short quantity,
													 int numPartitions,
													 AWSClient &client);
};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_S3SELECTQUERIES_H
