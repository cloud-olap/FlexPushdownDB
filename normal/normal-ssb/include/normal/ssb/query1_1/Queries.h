//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_QUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_QUERIES_H

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

/**
 * Normal execution plans for SSB query 1.1
 *
 * Includes partial queries run against file data sources and s3 using pull up and push down strategies
 */
class Queries {

public:



  static std::shared_ptr<OperatorManager> lineOrderScanS3PullUp(const std::string &s3Bucket,
																const std::string &s3ObjectDir,
																int numConcurrentUnits,
																AWSClient &client);



  static std::shared_ptr<OperatorManager> dateFilterS3PullUp(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 int numConcurrentUnits,
															 AWSClient &client);



  static std::shared_ptr<OperatorManager> query1_1LineOrderFilterS3PullUp(const std::string &s3Bucket,
																		  const std::string &s3ObjectDir,
																		  short discount,
																		  short quantity,
																		  int numConcurrentUnits,
																		  AWSClient &client);





  static std::shared_ptr<OperatorManager> query1_1S3PullUp(const std::string &s3Bucket,
														   const std::string &s3ObjectDir,
														   short year,
														   short discount,
														   short quantity,
														   int numConcurrentUnits,
														   AWSClient &client);

  static std::shared_ptr<OperatorManager> query1_1S3PushDownParallel(const std::string &s3Bucket,
																	 const std::string &s3ObjectDir,
																	 short year,
																	 short discount,
																	 short quantity,
																	 int numPartitions,
																	 AWSClient &client);

  static std::shared_ptr<OperatorManager> query1_1S3HybridParallel(const std::string &s3Bucket,
																   const std::string &s3ObjectDir,
																   short year,
																   short discount,
																   short quantity,
																   int numPartitions,
																   AWSClient &client);
};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_QUERIES_H
