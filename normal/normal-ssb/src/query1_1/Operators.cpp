//
// Created by matt on 26/6/20.
//

#include "normal/ssb/query1_1/Operators.h"

#include <experimental/filesystem>

#include <normal/ssb/Globals.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/Literal.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/And.h>
#include <normal/pushdown/Util.h>

using namespace normal::ssb::query1_1;
using namespace std::experimental;
using namespace normal::pushdown::aggregate;
using namespace normal::core::type;
using namespace normal::core::graph;
using namespace normal::expression::gandiva;


std::vector<std::shared_ptr<FileScan>>
Operators::makeDateFileScanOperators(const std::string &dataDir, int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {

  auto dateFile = filesystem::absolute(dataDir + "/date.tbl");
  auto numBytesDateFile = filesystem::file_size(dateFile);

  /**
   * Scan
   * date.tbl
   */
  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK",
	   "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON",
	   "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL"};

  std::vector<std::shared_ptr<FileScan>> dateScanOperators;
  auto dateScanRanges = Util::ranges<int>(0, numBytesDateFile, numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto dateScan = FileScan::make(fmt::format("/query-{}/date-scan-{}", g->getId(), u),
								   dateFile,
								   dateColumns,
								   dateScanRanges[u].first,
								   dateScanRanges[u].second,
								   g->getId());
	dateScanOperators.push_back(dateScan);
  }

  return dateScanOperators;
}

std::vector<std::shared_ptr<S3SelectScan>>
Operators::makeDateS3SelectScanOperators(const std::string &s3ObjectDir,
										 const std::string &s3Bucket,
										 int numConcurrentUnits,
										 std::unordered_map<std::string, long> partitionMap,
										 AWSClient &client, const std::shared_ptr<OperatorGraph>& g) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto numBytesDateFile = partitionMap.find(dateFile)->second;

  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK",
	   "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON",
	   "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL"};

  std::vector<std::shared_ptr<S3SelectScan>> dateScanOperators;
  auto dateScanRanges = Util::ranges<long>(0, numBytesDateFile, numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto dateScan = S3SelectScan::make(
		fmt::format("/query-{}/date-scan-{}", g->getId(), u),
		s3Bucket,
		dateFile,
		"select * from s3object",
		dateColumns,
		dateScanRanges[u].first,
		dateScanRanges[u].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	dateScanOperators.push_back(dateScan);
  }

  return dateScanOperators;
}

std::vector<std::shared_ptr<S3SelectScan>>
Operators::makeDateS3SelectScanPushDownOperators(const std::string &s3ObjectDir,
									  const std::string &s3Bucket,
									  short year,
									  int numConcurrentUnits,
									  std::unordered_map<std::string, long> partitionMap,
									  AWSClient &client, const std::shared_ptr<OperatorGraph>& g){
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto numBytesDateFile = partitionMap.find(dateFile)->second;

  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_YEAR"};

  std::vector<std::shared_ptr<S3SelectScan>> dateScanOperators;
  auto dateScanRanges = Util::ranges<long>(0, numBytesDateFile, numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto dateScan = S3SelectScan::make(
		fmt::format("/query-{}/date-scan-{}", g->getId(), u),
		s3Bucket,
		dateFile,
		fmt::format("select D_DATEKEY, D_YEAR from s3Object where cast(D_YEAR as int) = {}", year),
		dateColumns,
		dateScanRanges[u].first,
		dateScanRanges[u].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	dateScanOperators.push_back(dateScan);
  }

  return dateScanOperators;
}

std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
Operators::makeDateFilterOperators(short year, int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {

  std::vector<std::shared_ptr<normal::pushdown::filter::Filter>> dateFilterOperators;
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto dateFilter = normal::pushdown::filter::Filter::make(
		fmt::format("/query-{}/date-filter-{}", g->getId(), u),
		FilterPredicate::make(
			eq(cast(col("d_year"), integer32Type()), lit<::arrow::Int32Type>(year))));
	dateFilterOperators.push_back(dateFilter);
  }

  return dateFilterOperators;
}

std::vector<std::shared_ptr<Shuffle>>
Operators::makeDateShuffleOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {

  std::vector<std::shared_ptr<Shuffle>> shuffleOperators;
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto shuffle = Shuffle::make(fmt::format("/query-{}/date-shuffle-{}", g->getId(), u), "d_datekey");
	shuffleOperators.emplace_back(shuffle);
  }

  return shuffleOperators;
}

std::vector<std::shared_ptr<Shuffle>>
Operators::makeLineOrderShuffleOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {

  std::vector<std::shared_ptr<Shuffle>> shuffleOperators;
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto shuffle = Shuffle::make(fmt::format("/query-{}/lineorder-shuffle-{}", g->getId(), u), "lo_orderdate");
	shuffleOperators.emplace_back(shuffle);
  }

  return shuffleOperators;
}

std::vector<std::shared_ptr<FileScan>>
Operators::makeLineOrderFileScanOperators(const std::string &dataDir, int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {

  auto lineOrderFile = filesystem::absolute(dataDir + "/lineorder.tbl");
  auto numBytesLineOrderFile = filesystem::file_size(lineOrderFile);
//  auto numBytesLineOrderFile = 2973819;

  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY",
	   "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE",
	   "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE"};

  std::vector<std::shared_ptr<FileScan>> lineOrderScanOperators;
  auto lineOrderScanRanges = Util::ranges<int>(0, numBytesLineOrderFile, numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto lineOrderScan = FileScan::make(fmt::format("/query-{}/lineorder-scan-{}", g->getId(), u),
										lineOrderFile,
										lineOrderColumns,
										lineOrderScanRanges[u].first,
										lineOrderScanRanges[u].second,
										g->getId());
	lineOrderScanOperators.push_back(lineOrderScan);
  }

  return lineOrderScanOperators;
}

std::vector<std::shared_ptr<S3SelectScan>>
Operators::makeLineOrderS3SelectScanOperators(const std::string &s3ObjectDir,
											  const std::string &s3Bucket,
											  int numConcurrentUnits,
											  std::unordered_map<std::string, long> partitionMap,
											  AWSClient &client, const std::shared_ptr<OperatorGraph>& g) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto numBytesLineOrderFile = partitionMap.find(lineOrderFile)->second;

  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY",
	   "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE",
	   "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE"};

  std::vector<std::shared_ptr<S3SelectScan>> lineOrderScanOperators;
  auto lineOrderScanRanges = Util::ranges<long>(0, numBytesLineOrderFile, numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto lineOrderScan = S3SelectScan::make(
		fmt::format("/query-{}/lineorder-scan-{}", g->getId(), u),
		s3Bucket,
		lineOrderFile,
		"select * from s3object",
		lineOrderColumns,
		lineOrderScanRanges[u].first,
		lineOrderScanRanges[u].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	lineOrderScanOperators.push_back(lineOrderScan);
  }

  return lineOrderScanOperators;
}

std::vector<std::shared_ptr<S3SelectScan>>
Operators::makeLineOrderS3SelectScanPushdownOperators(const std::string &s3ObjectDir,
										   const std::string &s3Bucket,
										   short discount, short quantity,
										   int numConcurrentUnits,
										   std::unordered_map<std::string, long> partitionMap,
										   AWSClient &client, const std::shared_ptr<OperatorGraph>& g){

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto numBytesLineOrderFile = partitionMap.find(lineOrderFile)->second;

  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERDATE", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_DISCOUNT", "LO_REVENUE"};
  int discountLower = discount - 1;
  int discountUpper = discount + 1;

  std::vector<std::shared_ptr<S3SelectScan>> lineOrderScanOperators;
  auto lineOrderScanRanges = Util::ranges<long>(0, numBytesLineOrderFile, numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto lineOrderScan = S3SelectScan::make(
		fmt::format("/query-{}/lineorder-scan-{}", g->getId(), u),
		s3Bucket,
		lineOrderFile,
		fmt::format(
			"select LO_ORDERDATE, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE from s3Object where cast(LO_DISCOUNT as int) between {} and {} and cast(LO_QUANTITY as int) < {}",
			discountLower,
			discountUpper,
			quantity),
		lineOrderColumns,
		lineOrderScanRanges[u].first,
		lineOrderScanRanges[u].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	lineOrderScanOperators.push_back(lineOrderScan);
  }

  return lineOrderScanOperators;
}

std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
Operators::makeLineOrderFilterOperators(short discount, short quantity, int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {

  /**
   * Filter
   * lo_discount (f11) between 1 and 3
   * and lo_quantity (f8) < 25
   */
  int discountLower = discount - 1;
  int discountUpper = discount + 1;

  std::vector<std::shared_ptr<normal::pushdown::filter::Filter>> lineOrderFilterOperators;
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto lineOrderFilter = normal::pushdown::filter::Filter::make(
		fmt::format("/query-{}/lineorder-filter-{}", g->getId(), u),
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

  return lineOrderFilterOperators;
}

std::vector<std::shared_ptr<HashJoinBuild>>
Operators::makeHashJoinBuildOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {
  std::vector<std::shared_ptr<HashJoinBuild>> hashJoinBuildOperators;
  hashJoinBuildOperators.reserve(numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	hashJoinBuildOperators.emplace_back(HashJoinBuild::create(fmt::format("/query-{}/join-build-{}", g->getId(), u), "d_datekey"));
  }
  return hashJoinBuildOperators;
}

std::vector<std::shared_ptr<HashJoinProbe>> Operators::makeHashJoinProbeOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {
  std::vector<std::shared_ptr<HashJoinProbe>> hashJoinProbeOperators;
  hashJoinProbeOperators.reserve(numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	hashJoinProbeOperators.emplace_back(std::make_shared<HashJoinProbe>(fmt::format("/query-{}/join-probe-{}", g->getId(), u),
																		JoinPredicate::create("d_datekey",
																							  "lo_orderdate")));
  }
  return hashJoinProbeOperators;
}

std::vector<std::shared_ptr<Aggregate>>
Operators::makeAggregateOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g) {

  /**
   * Aggregate
   * sum(lo_extendedprice (f9) * lo_discount (f11))
   */
  std::vector<std::shared_ptr<Aggregate>> aggregateOperators;
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
	aggregateFunctions->
		emplace_back(std::make_shared<Sum>("revenue", times(cast(col("lo_extendedprice"), float64Type()),
															cast(col("lo_discount"), float64Type()))
	));
	auto aggregate = std::make_shared<Aggregate>(fmt::format("/query-{}/aggregate-{}", g->getId(), u), aggregateFunctions);
	aggregateOperators.emplace_back(aggregate);
  }

  return aggregateOperators;
}

std::shared_ptr<Aggregate> Operators::makeAggregateReduceOperator(const std::shared_ptr<OperatorGraph>& g) {

  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  aggregateFunctions->
	  emplace_back(std::make_shared<Sum>("revenue", col("revenue"))
  );
  auto aggregateReduce = std::make_shared<Aggregate>(fmt::format("/query-{}/aggregate-reduce", g->getId()), aggregateFunctions);

  return aggregateReduce;
}

std::shared_ptr<Collate> Operators::makeCollateOperator(const std::shared_ptr<OperatorGraph>& g) {
  return std::make_shared<Collate>(fmt::format("/query-{}/collate", g->getId()), g->getId());
}