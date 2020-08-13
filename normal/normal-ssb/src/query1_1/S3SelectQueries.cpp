//
// Created by matt on 26/6/20.
//

#include "normal/ssb/query1_1/S3SelectQueries.h"

#include <normal/ssb/Globals.h>
#include <normal/ssb/common/Operators.h>
#include <normal/ssb/query1_1/Operators.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>

using namespace normal::ssb::query1_1;
using namespace normal::pushdown::aggregate;
using namespace normal::core::type;
using namespace normal::expression::gandiva;
using namespace normal::connector::s3;

std::shared_ptr<OperatorGraph>
S3SelectQueries::dateScanPullUp(const std::string &s3Bucket,
								const std::string &s3ObjectDir,
								int numConcurrentUnits,
								AWSClient &client,
								const std::shared_ptr<Normal>& n) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto dateScans = Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectToOne(dateScans, collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::dateFilterPullUp(const std::string &s3Bucket,
																   const std::string &s3ObjectDir,
																   short year,
																   int numConcurrentUnits,
																   AWSClient &client,
																 const std::shared_ptr<Normal>& n) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, true, numConcurrentUnits, g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectToEach(dateScans, dateFilters);
  common::Operators::connectToOne(dateFilters, collate);

  return g;
}

std::shared_ptr<OperatorGraph>
S3SelectQueries::dateFilterHybrid(const std::string &s3Bucket,
								  const std::string &s3ObjectDir,
								  short year,
								  int numConcurrentUnits,
								  AWSClient &client,
								  const std::shared_ptr<Normal>& n) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto cacheLoads =
	  Operators::makeDateS3SelectCacheLoadOperators(s3ObjectDir, s3Bucket,
													   numConcurrentUnits, partitionMap,
													   client, g);

  auto dateScans =
	  Operators::makeDateS3SelectScanPushDownOperators(s3ObjectDir, s3Bucket,
													   year,
													   numConcurrentUnits, partitionMap,
													   client, g);
  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, true, numConcurrentUnits, g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectHitsToEach(cacheLoads, dateMerges);
  common::Operators::connectMissesToEach(cacheLoads, dateScans);
  common::Operators::connectToEach(dateScans, dateMerges);

  common::Operators::connectToEach(dateMerges, dateFilters);

  common::Operators::connectToOne(dateFilters, collate);

  return g;
}

std::shared_ptr<OperatorGraph>
S3SelectQueries::lineOrderScanPullUp(const std::string &s3Bucket,
									 const std::string &s3ObjectDir,
									 int numConcurrentUnits,
									 AWSClient &client,
									 const std::shared_ptr<Normal>& n) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto s3Objects = std::vector{lineOrderFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto lineOrderScans =
	  Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectToOne(lineOrderScans, collate);

  return g;
}

std::shared_ptr<OperatorGraph>
S3SelectQueries::lineOrderFilterPullUp(const std::string &s3Bucket,
									   const std::string &s3ObjectDir,
									   short discount,
									   short quantity,
									   int numConcurrentUnits,
									   AWSClient &client,
									   const std::shared_ptr<Normal>& n) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto s3Objects = std::vector{lineOrderFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto lineOrderScans =
	  Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, true, numConcurrentUnits, g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectToEach(lineOrderScans, lineOrderFilters);
  common::Operators::connectToOne(lineOrderFilters, collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::joinPullUp(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client,
														   const std::shared_ptr<Normal>& n) {
  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket,
																	  numConcurrentUnits, partitionMap, client, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, true, numConcurrentUnits, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, true, numConcurrentUnits, g);
  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
  auto joinBuild = common::Operators::makeHashJoinBuildOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
  auto joinProbe = common::Operators::makeHashJoinProbeOperators("lo_orderdate", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectToEach(dateScans, dateFilters);
  common::Operators::connectToEach(lineOrderScans, lineOrderFilters);

  common::Operators::connectToEach(dateFilters, dateShuffles);
  common::Operators::connectToEach(lineOrderFilters, lineOrderShuffles);

  common::Operators::connectToAll(dateShuffles, joinBuild);
  common::Operators::connectToAll(lineOrderShuffles, joinProbe);
  common::Operators::connectToEach(joinBuild, joinProbe);

  common::Operators::connectToOne(joinProbe, collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::fullPullUp(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client,
														   const std::shared_ptr<Normal>& n) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket,
																	  numConcurrentUnits, partitionMap, client, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, true, numConcurrentUnits, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, true, numConcurrentUnits, g);
  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
  auto joinBuild = common::Operators::makeHashJoinBuildOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
  auto joinProbe = common::Operators::makeHashJoinProbeOperators("lo_orderdate", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectToEach(dateScans, dateFilters);
  common::Operators::connectToEach(lineOrderScans, lineOrderFilters);

  common::Operators::connectToEach(dateFilters, dateShuffles);
  common::Operators::connectToEach(lineOrderFilters, lineOrderShuffles);

  common::Operators::connectToAll(dateShuffles, joinBuild);
  common::Operators::connectToAll(lineOrderShuffles, joinProbe);
  common::Operators::connectToEach(joinBuild, joinProbe);

  common::Operators::connectToEach(joinProbe, aggregates);
  common::Operators::connectToOne(aggregates, aggregateReduce);

  common::Operators::connectToOne(aggregateReduce, collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::fullPushDown(const std::string &s3Bucket,
															   const std::string &s3ObjectDir,
															   short year,
															   short discount,
															   short quantity,
															   int numConcurrentUnits,
															   AWSClient &client,
															 const std::shared_ptr<Normal>& n) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto dateScans =
	  Operators::makeDateS3SelectScanPushDownOperators(s3ObjectDir, s3Bucket,
													   year,
													   numConcurrentUnits, partitionMap,
													   client, g);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanPushdownOperators(s3ObjectDir, s3Bucket,
																			  discount, quantity,
																			  numConcurrentUnits, partitionMap,
																			  client, g);
  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
  auto joinBuild = common::Operators::makeHashJoinBuildOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
  auto joinProbe = common::Operators::makeHashJoinProbeOperators("lo_orderdate", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectToEach(dateScans, dateShuffles);
  common::Operators::connectToEach(lineOrderScans, lineOrderShuffles);

  common::Operators::connectToAll(dateShuffles, joinBuild);
  common::Operators::connectToAll(lineOrderShuffles, joinProbe);
  common::Operators::connectToEach(joinBuild, joinProbe);

  common::Operators::connectToEach(joinProbe, aggregates);
  common::Operators::connectToOne(aggregates, aggregateReduce);

  common::Operators::connectToOne(aggregateReduce, collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::fullHybrid(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client,
														   const std::shared_ptr<Normal>& n) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = n->createQuery();

  auto dateScans =
	  Operators::makeDateS3SelectScanPushDownOperators(s3ObjectDir, s3Bucket,
													   year,
													   numConcurrentUnits, partitionMap,
													   client, g);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanPushdownOperators(s3ObjectDir, s3Bucket,
																			  discount, quantity,
																			  numConcurrentUnits, partitionMap,
																			  client, g);
  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
  auto joinBuild = common::Operators::makeHashJoinBuildOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
  auto joinProbe = common::Operators::makeHashJoinProbeOperators("lo_orderdate", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectToEach(dateScans, dateShuffles);
  common::Operators::connectToEach(lineOrderScans, lineOrderShuffles);

  common::Operators::connectToAll(dateShuffles, joinBuild);
  common::Operators::connectToAll(lineOrderShuffles, joinProbe);
  common::Operators::connectToEach(joinBuild, joinProbe);

  common::Operators::connectToEach(joinProbe, aggregates);
  common::Operators::connectToOne(aggregates, aggregateReduce);

  common::Operators::connectToOne(aggregateReduce, collate);

  return g;
}


