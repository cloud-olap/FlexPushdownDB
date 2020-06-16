//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H

#include <memory>

#include <normal/core/OperatorManager.h>
#include <normal/tuple/TupleSet.h>
#include <normal/pushdown/AWSClient.h>

using namespace normal::core;
using namespace normal::pushdown;

/**
 * SSB query factories
 */
class Queries {

public:
  static std::string query01(short year, short discount, short quantity);

  static std::shared_ptr<OperatorManager> query1_1FilePullUp(const std::string &dataDir,
															 short year,
															 short discount,
															 short quantity);

  static std::shared_ptr<OperatorManager> query1_1FilePullUpParallel(const std::string &dataDir,
																	 short year,
																	 short discount,
																	 short quantity,
																	 int numPartitions);

  static std::shared_ptr<OperatorManager> query1_1S3PullUp(const std::string &s3Bucket,
														   const std::string &s3ObjectDir,
														   short year,
														   short discount,
														   short quantity,
														   AWSClient &client);

  static std::shared_ptr<OperatorManager> query1_1S3PushDown(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 AWSClient &client);
};

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H
