//
// Created by matt on 26/6/20.
//

#include "normal/ssb/query1_1/S3SelectQueries.h"

#include <normal/ssb/Globals.h>
#include <normal/ssb/query1_1/Operators.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/Literal.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/And.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>

using namespace normal::ssb::query1_1;
using namespace normal::pushdown::aggregate;
using namespace normal::core::type;
using namespace normal::expression::gandiva;
using namespace normal::connector::s3;


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

  auto dateScans = Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
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

  auto dateScans = Operators::makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
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

//std::shared_ptr<OperatorManager> S3SelectQueries::query1_1S3PushDown(const std::string &s3Bucket,
//															 const std::string &s3ObjectDir,
//															 short year,
//															 short discount,
//															 short quantity,
//															 AWSClient &client) {
//
//  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
//  auto dateFile = s3ObjectDir + "/date.tbl";
//  auto s3Objects = std::vector{lineOrderFile, dateFile};
//
//  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//
//  SPDLOG_DEBUG("Discovered partitions");
//  for (auto &partition : partitionMap) {
//	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
//  }
//  auto mgr = std::make_shared<OperatorManager>();
//
//  /**
//   * Scan
//   * lineorder.tbl
//   * date.tbl
//   */
//  std::vector<std::string> lineOrderColumns =
//	  {"LO_ORDERDATE", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_DISCOUNT", "LO_REVENUE"};
//  auto numBytesLineOrderFile = partitionMap.find(lineOrderFile)->second;
//  int discountLower = discount - 1;
//  int discountUpper = discount + 1;
//  auto lineOrderScan = S3SelectScan::make(
//	  "lineOrderScan",
//	  s3Bucket,
//	  lineOrderFile,
//	  fmt::format(
//		  "select LO_ORDERDATE, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE from s3Object where cast(LO_DISCOUNT as int) between {} and {} and cast(LO_QUANTITY as int) < {}",
//		  discountLower,
//		  discountUpper,
//		  quantity),
//	  lineOrderColumns,
//	  0,
//	  numBytesLineOrderFile,
//	  S3SelectCSVParseOptions(",", "\n"),
//	  client.defaultS3Client());
//
//  std::vector<std::string> dateColumns =
//	  {"D_DATEKEY", "D_YEAR",};
//  auto numBytesDateFile = partitionMap.find(dateFile)->second;
//  auto dateScan = S3SelectScan::make(
//	  "dateScan",
//	  s3Bucket,
//	  dateFile,
//	  fmt::format("select D_DATEKEY, D_YEAR from s3Object where cast(D_YEAR as int) = {}", year),
//	  dateColumns,
//	  0,
//	  numBytesDateFile,
//	  S3SelectCSVParseOptions(",", "\n"),
//	  client.defaultS3Client());
//
//  /**
//   * Join
//   * lo_orderdate (f5) = d_datekey (f0)
//   */
//  auto joinBuild = HashJoinBuild::create("join-build", "d_datekey");
//  auto joinProbe = std::make_shared<HashJoinProbe>("join-probe",
//												   JoinPredicate::create("d_datekey", "lo_orderdate"));
//
//  /**
//   * Aggregate
//   * sum(lo_extendedprice (f9) * lo_discount (f11))
//   */
//  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
//  aggregateFunctions->
//	  emplace_back(std::make_shared<Sum>("revenue", times(cast(col("lo_extendedprice"), float64Type()),
//														  cast(col("lo_discount"), float64Type()))
//  ));
//  auto aggregate = std::make_shared<Aggregate>("aggregate", aggregateFunctions);
//
//  /**
//   * Collate
//   */
//  auto collate = makeCollate();
//
//  // Wire up
//  dateScan->produce(joinBuild);
//  joinBuild->consume(dateScan);
//
//  joinBuild->produce(joinProbe);
//  joinProbe->consume(joinBuild);
//
//  lineOrderScan->produce(joinProbe);
//  joinProbe->consume(lineOrderScan);
//
//  joinProbe->produce(aggregate);
//  aggregate->consume(joinProbe);
//
//  aggregate->produce(collate);
//  collate->consume(aggregate);
//
//  mgr->put(lineOrderScan);
//  mgr->put(dateScan);
//  mgr->put(joinBuild);
//  mgr->put(joinProbe);
//  mgr->put(aggregate);
//  mgr->put(collate);
//
//  return mgr;
//}

std::shared_ptr<OperatorManager> S3SelectQueries::fullPushDown(const std::string &s3Bucket,
															   const std::string &s3ObjectDir,
															   short year,
															   short discount,
															   short quantity,
															   int numPartitions,
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

  /**
   * Scan
   * date.tbl
   */
  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_YEAR",};

  std::vector<std::shared_ptr<Operator>> dateScanOperators;
  auto dateScanRanges = Util::ranges<long>(0, partitionMap.find(dateFile)->second, numPartitions);
  for (int p = 0; p < numPartitions; ++p) {
	auto dateScan = S3SelectScan::make(
		fmt::format("dateScan-{}", p),
		s3Bucket,
		dateFile,
		fmt::format("select D_DATEKEY, D_YEAR from s3Object where cast(D_YEAR as int) = {}", year),
		dateColumns,
		dateScanRanges[p].first,
		dateScanRanges[p].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	dateScanOperators.push_back(dateScan);
  }


  /**
   * Scan
   * lineorder.tbl
   */
  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERDATE", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_DISCOUNT", "LO_REVENUE"};
  int discountLower = discount - 1;
  int discountUpper = discount + 1;

  std::vector<std::shared_ptr<Operator>> lineOrderScanOperators;
  auto lineOrderScanRanges = Util::ranges<long>(0, partitionMap.find(lineOrderFile)->second, numPartitions);
  for (int p = 0; p < numPartitions; ++p) {
	auto lineOrderScan = S3SelectScan::make(
		fmt::format("lineOrderScan-{}", p),
		s3Bucket,
		lineOrderFile,
		fmt::format(
			"select LO_ORDERDATE, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE from s3Object where cast(LO_DISCOUNT as int) between {} and {} and cast(LO_QUANTITY as int) < {}",
			discountLower,
			discountUpper,
			quantity),
		lineOrderColumns,
		lineOrderScanRanges[p].first,
		lineOrderScanRanges[p].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	lineOrderScanOperators.push_back(lineOrderScan);
  }
  /**
   * Join
   * lo_orderdate (f5) = d_datekey (f0)
   */
  auto joinBuild = HashJoinBuild::create("join-build", "d_datekey");
  auto joinProbe = std::make_shared<HashJoinProbe>("join-probe",
												   JoinPredicate::create("d_datekey", "lo_orderdate"));

  /**
   * Aggregate
   * sum(lo_extendedprice (f9) * lo_discount (f11))
   */
  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  aggregateFunctions->
	  emplace_back(std::make_shared<Sum>("revenue", times(cast(col("lo_extendedprice"), float64Type()),
														  cast(col("lo_discount"), float64Type()))
  ));
  auto aggregate = std::make_shared<Aggregate>("aggregate", aggregateFunctions);

  /**
   * Collate
   */
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int p = 0; p < numPartitions; ++p) {
	dateScanOperators[p]->produce(joinBuild);
	joinBuild->consume(dateScanOperators[p]);
  }

  for (int p = 0; p < numPartitions; ++p) {
	lineOrderScanOperators[p]->produce(joinProbe);
	joinProbe->consume(lineOrderScanOperators[p]);
  }

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  joinProbe->produce(aggregate);
  aggregate->consume(joinProbe);

  aggregate->produce(collate);
  collate->consume(aggregate);

  for (int p = 0; p < numPartitions; ++p)
	mgr->put(dateScanOperators[p]);
  for (int p = 0; p < numPartitions; ++p)
	mgr->put(lineOrderScanOperators[p]);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(aggregate);
  mgr->put(collate);

  return mgr;
}
std::shared_ptr<OperatorManager> S3SelectQueries::fullHybrid(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 int numPartitions,
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

  /**
   * Scan
   * date.tbl
   */
  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_YEAR",};

  std::vector<std::shared_ptr<Operator>> dateScanOperators;
  auto dateScanRanges = Util::ranges<long>(0, partitionMap.find(dateFile)->second, numPartitions);
  for (int p = 0; p < numPartitions; ++p) {
	auto dateScan = S3SelectScan::make(
		fmt::format("dateScan-{}", p),
		s3Bucket,
		dateFile,
		fmt::format("select D_DATEKEY, D_YEAR from s3Object where cast(D_YEAR as int) = {}", year),
		dateColumns,
		dateScanRanges[p].first,
		dateScanRanges[p].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	dateScanOperators.push_back(dateScan);
  }


  /**
   * Scan
   * lineorder.tbl
   */
  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERDATE", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_DISCOUNT", "LO_REVENUE"};
  int discountLower = discount - 1;
  int discountUpper = discount + 1;

  std::vector<std::shared_ptr<Operator>> lineOrderScanOperators;
  auto lineOrderScanRanges = Util::ranges<long>(0, partitionMap.find(lineOrderFile)->second, numPartitions);
  for (int p = 0; p < numPartitions; ++p) {
	auto lineOrderScan = S3SelectScan::make(
		fmt::format("lineOrderScan-{}", p),
		s3Bucket,
		lineOrderFile,
		fmt::format(
			"select LO_ORDERDATE, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE from s3Object where cast(LO_DISCOUNT as int) between {} and {} and cast(LO_QUANTITY as int) < {}",
			discountLower,
			discountUpper,
			quantity),
		lineOrderColumns,
		lineOrderScanRanges[p].first,
		lineOrderScanRanges[p].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	lineOrderScanOperators.push_back(lineOrderScan);
  }
  /**
	* Filter
	* d_year (f4) = 1993
	*/
  std::vector<std::shared_ptr<Operator>> dateFilterOperators;
  for (int p = 0; p < numPartitions; ++p) {
	auto dateFilter = normal::pushdown::filter::Filter::make(
		fmt::format("dateFilter-{}", p),
		FilterPredicate::make(
			eq(cast(col("d_year"), integer32Type()), lit<::arrow::Int32Type>(year))));
	dateFilterOperators.push_back(dateFilter);
  }

  /**
   * Filter
   * lo_discount (f11) between 1 and 3
   * and lo_quantity (f8) < 25
   */

  std::vector<std::shared_ptr<Operator>> lineOrderFilterOperators;
  for (int p = 0; p < numPartitions; ++p) {
	auto lineOrderFilter = normal::pushdown::filter::Filter::make(
		fmt::format("lineOrderFilter-{}", p),
		FilterPredicate::make(
			and_(
				and_(
					gte(cast(col("lo_discount"), integer32Type()), lit<::arrow::Int32Type>(discountLower)),
					lte(cast(col("lo_discount"), integer32Type()), lit<::arrow::Int32Type>(discountUpper))
				),
				lt(cast(col("lo_quantity"), integer32Type()), lit<::arrow::Int32Type>(quantity))
			)
		)
	);
	lineOrderFilterOperators.push_back(lineOrderFilter);
  }

  /**
   * Join
   * lo_orderdate (f5) = d_datekey (f0)
   */
  auto joinBuild = HashJoinBuild::create("join-build", "d_datekey");
  auto joinProbe = std::make_shared<HashJoinProbe>("join-probe",
												   JoinPredicate::create("d_datekey", "lo_orderdate"));

  /**
   * Aggregate
   * sum(lo_extendedprice (f9) * lo_discount (f11))
   */
  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  aggregateFunctions->
	  emplace_back(std::make_shared<Sum>("revenue", times(cast(col("lo_extendedprice"), float64Type()),
														  cast(col("lo_discount"), float64Type()))
  ));
  auto aggregate = std::make_shared<Aggregate>("aggregate", aggregateFunctions);

  /**
   * Collate
   */
  auto collate = Operators::makeCollateOperator();

  // Wire up
  for (int p = 0; p < numPartitions; ++p) {
	dateScanOperators[p]->produce(joinBuild);
	joinBuild->consume(dateScanOperators[p]);
  }

  for (int p = 0; p < numPartitions; ++p) {
	lineOrderScanOperators[p]->produce(joinProbe);
	joinProbe->consume(lineOrderScanOperators[p]);
  }
  for (int p = 0; p < numPartitions; ++p) {
	dateScanOperators[p]->produce(dateFilterOperators[p]);
	dateFilterOperators[p]->consume(dateScanOperators[p]);
  }

  for (int p = 0; p < numPartitions; ++p) {
	lineOrderScanOperators[p]->produce(lineOrderFilterOperators[p]);
	lineOrderFilterOperators[p]->consume(lineOrderScanOperators[p]);
  }

  for (int p = 0; p < numPartitions; ++p) {
	dateFilterOperators[p]->produce(joinBuild);
	joinBuild->consume(dateFilterOperators[p]);
  }
  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  for (int p = 0; p < numPartitions; ++p) {
	lineOrderFilterOperators[p]->produce(joinProbe);
	joinProbe->consume(lineOrderFilterOperators[p]);
  }

  joinProbe->produce(aggregate);
  aggregate->consume(joinProbe);

  aggregate->produce(collate);
  collate->consume(aggregate);

  for (int p = 0; p < numPartitions; ++p)
	mgr->put(dateScanOperators[p]);
  for (int p = 0; p < numPartitions; ++p)
	mgr->put(lineOrderScanOperators[p]);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(aggregate);
  mgr->put(collate);

  return mgr;

}
