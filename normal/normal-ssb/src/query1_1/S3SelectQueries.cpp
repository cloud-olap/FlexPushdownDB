//
// Created by matt on 26/6/20.
//

#include "normal/ssb/query1_1/S3SelectQueries.h"

#include <normal/ssb/Globals.h>
#include <normal/ssb/query1_1/Operators.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>

using namespace normal::ssb::query1_1;
using namespace normal::pushdown::aggregate;
using namespace normal::core::type;
using namespace normal::expression::gandiva;
using namespace normal::connector::s3;

std::shared_ptr<OperatorManager>
S3SelectQueries::dateScanPullUp(const std::string &s3Bucket,
								const std::string &s3ObjectDir,
								int numConcurrentUnits,
								AWSClient &client) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(collate);
	collate->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager> S3SelectQueries::dateFilterPullUp(const std::string &s3Bucket,
																   const std::string &s3ObjectDir,
																   short year,
																   int numConcurrentUnits,
																   AWSClient &client) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits);
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateFilters[u]->produce(collate);
	collate->consume(dateFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager>
S3SelectQueries::lineOrderScanPullUp(const std::string &s3Bucket,
									 const std::string &s3ObjectDir,
									 int numConcurrentUnits,
									 AWSClient &client) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto s3Objects = std::vector{lineOrderFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto mgr = std::make_shared<OperatorManager>();

  auto lineOrderScans =
	  Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(collate);
	collate->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager>
S3SelectQueries::lineOrderFilterPullUp(const std::string &s3Bucket,
									   const std::string &s3ObjectDir,
									   short discount,
									   short quantity,
									   int numConcurrentUnits,
									   AWSClient &client) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto s3Objects = std::vector{lineOrderFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto mgr = std::make_shared<OperatorManager>();

  auto lineOrderScans =
	  Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(collate);
	collate->consume(lineOrderFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager> S3SelectQueries::joinPullUp(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client) {
  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket,
																	  numConcurrentUnits, partitionMap, client);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits);
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateFilters[u]->produce(dateShuffles[u]);
	dateShuffles[u]->consume(dateFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(lineOrderShuffles[u]);
	lineOrderShuffles[u]->consume(lineOrderFilters[u]);
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  dateShuffles[u1]->produce(joinBuild[u2]);
	  joinBuild[u2]->consume(dateShuffles[u1]);
	}
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  lineOrderShuffles[u1]->produce(joinProbe[u2]);
	  joinProbe[u2]->consume(lineOrderShuffles[u1]);
	}
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinBuild[u]->produce(joinProbe[u]);
	joinProbe[u]->consume(joinBuild[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinProbe[u]->produce(collate);
	collate->consume(joinProbe[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinProbe[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager> S3SelectQueries::fullPullUp(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket,
																	  numConcurrentUnits, partitionMap, client);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits);
  auto aggregateReduce = Operators::makeAggregateReduceOperator();
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateFilters[u]->produce(dateShuffles[u]);
	dateShuffles[u]->consume(dateFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(lineOrderShuffles[u]);
	lineOrderShuffles[u]->consume(lineOrderFilters[u]);
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  dateShuffles[u1]->produce(joinBuild[u2]);
	  joinBuild[u2]->consume(dateShuffles[u1]);
	}
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  lineOrderShuffles[u1]->produce(joinProbe[u2]);
	  joinProbe[u2]->consume(lineOrderShuffles[u1]);
	}
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinBuild[u]->produce(joinProbe[u]);
	joinProbe[u]->consume(joinBuild[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinProbe[u]->produce(aggregates[u]);
	aggregates[u]->consume(joinProbe[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	aggregates[u]->produce(aggregateReduce);
	aggregateReduce->consume(aggregates[u]);
  }

  aggregateReduce->produce(collate);
  collate->consume(aggregateReduce);

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinProbe[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(aggregates[u]);
  mgr->put(aggregateReduce);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager> S3SelectQueries::fullPushDown(const std::string &s3Bucket,
															   const std::string &s3ObjectDir,
															   short year,
															   short discount,
															   short quantity,
															   int numConcurrentUnits,
															   AWSClient &client) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans =
	  Operators::makeDateS3SelectScanPushDownOperators(s3ObjectDir, s3Bucket,
													   year,
													   numConcurrentUnits, partitionMap,
													   client);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanPushdownOperators(s3ObjectDir, s3Bucket,
																			  discount, quantity,
																			  numConcurrentUnits, partitionMap,
																			  client);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits);
  auto aggregateReduce = Operators::makeAggregateReduceOperator();
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateShuffles[u]);
	dateShuffles[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderShuffles[u]);
	lineOrderShuffles[u]->consume(lineOrderScans[u]);
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  dateShuffles[u1]->produce(joinBuild[u2]);
	  joinBuild[u2]->consume(dateShuffles[u1]);
	}
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  lineOrderShuffles[u1]->produce(joinProbe[u2]);
	  joinProbe[u2]->consume(lineOrderShuffles[u1]);
	}
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinBuild[u]->produce(joinProbe[u]);
	joinProbe[u]->consume(joinBuild[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinProbe[u]->produce(aggregates[u]);
	aggregates[u]->consume(joinProbe[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	aggregates[u]->produce(aggregateReduce);
	aggregateReduce->consume(aggregates[u]);
  }

  aggregateReduce->produce(collate);
  collate->consume(aggregateReduce);

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinProbe[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(aggregates[u]);
  mgr->put(aggregateReduce);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager> S3SelectQueries::fullHybrid(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans =
	  Operators::makeDateS3SelectScanPushDownOperators(s3ObjectDir, s3Bucket,
													   year,
													   numConcurrentUnits, partitionMap,
													   client);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanPushdownOperators(s3ObjectDir, s3Bucket,
																			  discount, quantity,
																			  numConcurrentUnits, partitionMap,
																			  client);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits);
  auto aggregateReduce = Operators::makeAggregateReduceOperator();
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateShuffles[u]);
	dateShuffles[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderShuffles[u]);
	lineOrderShuffles[u]->consume(lineOrderScans[u]);
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  dateShuffles[u1]->produce(joinBuild[u2]);
	  joinBuild[u2]->consume(dateShuffles[u1]);
	}
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  lineOrderShuffles[u1]->produce(joinProbe[u2]);
	  joinProbe[u2]->consume(lineOrderShuffles[u1]);
	}
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinBuild[u]->produce(joinProbe[u]);
	joinProbe[u]->consume(joinBuild[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinProbe[u]->produce(aggregates[u]);
	aggregates[u]->consume(joinProbe[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	aggregates[u]->produce(aggregateReduce);
	aggregateReduce->consume(aggregates[u]);
  }

  aggregateReduce->produce(collate);
  collate->consume(aggregateReduce);

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinProbe[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(aggregates[u]);
  mgr->put(aggregateReduce);
  mgr->put(collate);

  return mgr;
}


