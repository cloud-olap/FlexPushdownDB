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
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/s3/S3SelectScan.h>

using namespace normal::core;
using namespace normal::pushdown;
using namespace normal::pushdown::filter;
using namespace normal::pushdown::join;

/**
 * SSB query factories
 */
class Queries {

public:

  /*
   * SQLite queries
   */
  static std::string query1_1LineOrderScanSQLite(const std::string &catalogue);
  static std::string query1_1DateFilterSQLite(short year, const std::string &catalogue);
  static std::string query1_1LineOrderFilterSQLite(short discount, short quantity, const std::string &catalogue);
  static std::string query1_1JoinSQLite(short year, short discount, short quantity, const std::string &catalogue);
  static std::string query1_1SQLite(short year, short discount, short quantity, const std::string &catalogue);

  /*
   * Normal operators
   */
  static std::vector<std::shared_ptr<FileScan>>
  makeDateFileScanOperators(const std::string &dataDir, int numConcurrentUnits);

  static std::vector<std::shared_ptr<S3SelectScan>>
  makeDateS3SelectScanOperators(const std::string &s3ObjectDir,
								const std::string &s3Bucket,
								int numConcurrentUnits,
								std::unordered_map<std::string, long> partitionMap,
								AWSClient &client);

  static std::vector<std::shared_ptr<FileScan>>
  makeLineOrderFileScanOperators(const std::string &dataDir, int numConcurrentUnits);

  static std::vector<std::shared_ptr<S3SelectScan>>
  makeLineOrderS3SelectScanOperators(const std::string &s3ObjectDir,
									 const std::string &s3Bucket,
									 int numConcurrentUnits,
									 std::unordered_map<std::string, long> partitionMap,
									 AWSClient &client);

  static std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
  makeDateFilterOperators(short year, int numConcurrentUnits);

  static std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
  makeLineOrderFilterOperators(short discount, short quantity, int numConcurrentUnits);

  static std::shared_ptr<HashJoinBuild> makeHashJoinBuildOperators();
  static std::shared_ptr<HashJoinProbe> makeHashJoinProbeOperators();
  static std::shared_ptr<Aggregate> makeAggregateOperators();
  static std::shared_ptr<Collate> makeCollate();

  /*
   * Normal queries
   */
  static std::shared_ptr<OperatorManager> query1_1LineOrderScanS3PullUp(const std::string &s3Bucket,
																		const std::string &s3ObjectDir,
																		int numConcurrentUnits,
																		AWSClient &client);

  static std::shared_ptr<OperatorManager> query1_1DateFilterFilePullUp(const std::string &dataDir,
																	   short year,
																	   int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> query1_1DateFilterS3PullUp(const std::string &s3Bucket,
																	 const std::string &s3ObjectDir,
																	 short year,
																	 int numConcurrentUnits,
																	 AWSClient &client);

  static std::shared_ptr<OperatorManager> query1_1LineOrderFilterFilePullUp(const std::string &dataDir,
																			short discount,
																			short quantity,
																			int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> query1_1LineOrderFilterS3PullUp(const std::string &s3Bucket,
																		  const std::string &s3ObjectDir,
																		  short discount,
																		  short quantity,
																		  int numConcurrentUnits,
																		  AWSClient &client);

  static std::shared_ptr<OperatorManager> query1_1JoinFilePullUp(const std::string &dataDir,
																 short year,
																 short discount,
																 short quantity,
																 int numConcurrentUnits);

  static std::shared_ptr<OperatorManager> query1_1FilePullUp(const std::string &dataDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits);

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

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H
