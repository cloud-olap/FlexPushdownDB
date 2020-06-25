//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H

#include <memory>

#include <normal/core/OperatorManager.h>
#include <normal/tuple/TupleSet.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/filter/Filter.h>
#include <normal/pushdown/Collate.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>

using namespace normal::core;
using namespace normal::pushdown;
using namespace normal::pushdown::filter;
using namespace normal::pushdown::join;

/**
 * SSB query factories
 */
class Queries {

public:
  static std::string query1_1SQLite(short year, short discount, short quantity, const std::string& catalogue);
  static std::string query1_1DateFilterSQLite(short year, const std::string& catalogue);
  static std::string query1_1LineOrderFilterSQLite(short discount, short quantity, const std::string& catalogue);
  static std::string query1_1JoinSQLite(short year, short discount, short quantity, const std::string& catalogue);

  static std::shared_ptr<FileScan> makeDateFileScan(const std::string &dataDir);
  static std::shared_ptr<Filter> makeDateFilter(short year);
  static std::shared_ptr<FileScan> makeLineOrderFileScan(const std::string &dataDir);
  static std::shared_ptr<Filter> makeLineOrderFilter(short discount, short quantity);
  static std::shared_ptr<HashJoinBuild> makeHashJoinBuild();
  static std::shared_ptr<HashJoinProbe> makeHashJoinProbe();
  static std::shared_ptr<Collate> makeCollate();

  static std::shared_ptr<OperatorManager> query1_1DateFilterFilePullUp(const std::string &dataDir,
																	   short year);

  static std::shared_ptr<OperatorManager> query1_1LineOrderFilterFilePullUp(const std::string &dataDir,
																		short discount,
																		short quantity);

  static std::shared_ptr<OperatorManager> query1_1JoinFilePullUp(const std::string &dataDir,
															 short year,
															 short discount,
															 short quantity);

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

  static std::shared_ptr<OperatorManager> query1_1S3PullUpParallel(const std::string &s3Bucket,
																   const std::string &s3ObjectDir,
																   short year,
																   short discount,
																   short quantity,
																   int numPartitions,
																   AWSClient &client);

  static std::shared_ptr<OperatorManager> query1_1S3PushDown(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
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

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H
