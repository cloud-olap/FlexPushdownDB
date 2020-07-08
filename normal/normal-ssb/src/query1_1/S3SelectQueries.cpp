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

std::shared_ptr<OperatorGraph>
S3SelectQueries::dateScanPullUp(const std::string &s3Bucket,
								const std::string &s3ObjectDir,
								int numConcurrentUnits,
								AWSClient &client,
								const std::shared_ptr<OperatorManager> &mgr) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = OperatorGraph::make(mgr);

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto collate = Operators::makeCollateOperator(g);

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(collate);
	collate->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateScans[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::dateFilterPullUp(const std::string &s3Bucket,
																   const std::string &s3ObjectDir,
																   short year,
																   int numConcurrentUnits,
																   AWSClient &client,
																   const std::shared_ptr<OperatorManager> &mgr) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = OperatorGraph::make(mgr);

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits, g);
  auto collate = Operators::makeCollateOperator(g);

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
	g->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateFilters[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph>
S3SelectQueries::lineOrderScanPullUp(const std::string &s3Bucket,
									 const std::string &s3ObjectDir,
									 int numConcurrentUnits,
									 AWSClient &client,
									 const std::shared_ptr<OperatorManager> &mgr) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto s3Objects = std::vector{lineOrderFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = OperatorGraph::make(mgr);

  auto lineOrderScans =
	  Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto collate = Operators::makeCollateOperator(g);

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(collate);
	collate->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScans[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph>
S3SelectQueries::lineOrderFilterPullUp(const std::string &s3Bucket,
									   const std::string &s3ObjectDir,
									   short discount,
									   short quantity,
									   int numConcurrentUnits,
									   AWSClient &client,
									   const std::shared_ptr<OperatorManager> &mgr) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto s3Objects = std::vector{lineOrderFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = OperatorGraph::make(mgr);

  auto lineOrderScans =
	  Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits, g);
  auto collate = Operators::makeCollateOperator(g);

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
	g->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderFilters[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::joinPullUp(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client,
															 const std::shared_ptr<OperatorManager> &mgr) {
  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = OperatorGraph::make(mgr);

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket,
																	  numConcurrentUnits, partitionMap, client, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits, g);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits, g);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits, g);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits, g);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits, g);
  auto collate = Operators::makeCollateOperator(g);

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
	g->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinProbe[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::fullPullUp(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client,
															 const std::shared_ptr<OperatorManager> &mgr) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = OperatorGraph::make(mgr);

  auto dateScans =
	  Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client, g);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket,
																	  numConcurrentUnits, partitionMap, client, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits, g);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits, g);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits, g);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits, g);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits, g);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
  auto collate = Operators::makeCollateOperator(g);

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
	g->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinProbe[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(aggregates[u]);
  g->put(aggregateReduce);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::fullPushDown(const std::string &s3Bucket,
															   const std::string &s3ObjectDir,
															   short year,
															   short discount,
															   short quantity,
															   int numConcurrentUnits,
															   AWSClient &client,
															   const std::shared_ptr<OperatorManager> &mgr) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = OperatorGraph::make(mgr);

  auto dateScans =
	  Operators::makeDateS3SelectScanPushDownOperators(s3ObjectDir, s3Bucket,
													   year,
													   numConcurrentUnits, partitionMap,
													   client, g);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanPushdownOperators(s3ObjectDir, s3Bucket,
																			  discount, quantity,
																			  numConcurrentUnits, partitionMap,
																			  client, g);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits, g);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits, g);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits, g);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits, g);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
  auto collate = Operators::makeCollateOperator(g);

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
	g->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinProbe[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(aggregates[u]);
  g->put(aggregateReduce);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph> S3SelectQueries::fullHybrid(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numConcurrentUnits,
															 AWSClient &client,
															 const std::shared_ptr<OperatorManager> &mgr) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto s3Objects = std::vector{lineOrderFile, dateFile};

  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  auto g = OperatorGraph::make(mgr);

  auto dateScans =
	  Operators::makeDateS3SelectScanPushDownOperators(s3ObjectDir, s3Bucket,
													   year,
													   numConcurrentUnits, partitionMap,
													   client, g);
  auto lineOrderScans = Operators::makeLineOrderS3SelectScanPushdownOperators(s3ObjectDir, s3Bucket,
																			  discount, quantity,
																			  numConcurrentUnits, partitionMap,
																			  client, g);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits, g);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits, g);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits, g);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits, g);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
  auto collate = Operators::makeCollateOperator(g);

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
	g->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinProbe[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(aggregates[u]);
  g->put(aggregateReduce);
  g->put(collate);

  return g;
}


