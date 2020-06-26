//
// Created by matt on 28/4/20.
//

#include <normal/ssb/Queries.h>

#include <experimental/filesystem>

#include <normal/ssb/Globals.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Collate.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/core/type/Integer64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/pushdown/filter/Filter.h>
#include <normal/pushdown/filter/FilterPredicate.h>
#include <normal/expression/gandiva/Literal.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/And.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/AWSClient.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>

using namespace std::experimental;
using namespace normal::pushdown;
using namespace normal::pushdown::aggregate;
using namespace normal::pushdown::filter;
using namespace normal::core;
using namespace normal::core::type;
using namespace normal::expression;
using namespace normal::expression::gandiva;
using namespace normal::pushdown::join;
using namespace normal::connector::s3;

std::string Queries::query1_1SQLite(short year, short discount, short quantity, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "sum(lo_extendedprice * lo_discount) as revenue "
	  "from "
	  "{0}.lineorder, {0}.date "
	  "where "
	  "lo_orderdate = d_datekey "
	  "and cast(d_year as integer) = {1} "
	  "and cast(lo_discount as integer) between {2} and {3} "
	  "and cast(lo_quantity as integer) < {4}; ",
	  catalogue,
	  year,
	  discount - 1,
	  discount + 1,
	  quantity
  );

  return sql;
}

std::string Queries::query1_1LineOrderScanSQLite(const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder;",
	  catalogue
  );

  return sql;
}

std::string Queries::query1_1DateFilterSQLite(short year, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.date "
	  "where "
	  "cast(d_year as integer) = {1} ",
	  catalogue,
	  year
  );

  return sql;
}

std::string Queries::query1_1LineOrderFilterSQLite(short discount, short quantity, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder "
	  "where "
	  "cast(lo_discount as integer) between {1} and {2} "
	  "and cast(lo_quantity as integer) < {3};",
	  catalogue,
	  discount - 1,
	  discount + 1,
	  quantity
  );

  return sql;
}

std::string Queries::query1_1JoinSQLite(short year, short discount, short quantity, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder, {0}.date "
	  "where "
	  "lo_orderdate = d_datekey "
	  "and cast(d_year as integer) = {1} "
	  "and cast(lo_discount as integer) between {2} and {3} "
	  "and cast(lo_quantity as integer) < {4}; ",
	  catalogue,
	  year,
	  discount - 1,
	  discount + 1,
	  quantity
  );

  return sql;
}

std::vector<std::shared_ptr<FileScan>>
Queries::makeDateFileScanOperators(const std::string &dataDir, int numConcurrentUnits) {

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
	auto dateScan = FileScan::make(fmt::format("dateScan-{}", u),
								   dateFile,
								   dateColumns,
								   dateScanRanges[u].first,
								   dateScanRanges[u].second);
	dateScanOperators.push_back(dateScan);
  }

  return dateScanOperators;
}

std::vector<std::shared_ptr<S3SelectScan>>
Queries::makeDateS3SelectScanOperators(const std::string &s3ObjectDir,
									   const std::string &s3Bucket,
									   int numConcurrentUnits,
									   std::unordered_map<std::string, long> partitionMap,
									   AWSClient &client) {

  auto dateFile = s3ObjectDir + "/date.tbl";
  auto numBytesDateFile = partitionMap.find(dateFile)->second;

  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK",
	   "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON",
	   "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL"};

  std::vector<std::shared_ptr<S3SelectScan>> dateScanOperators;
  auto dateScanRanges = Util::ranges<long>(0, numBytesDateFile, numConcurrentUnits);
  for (int p = 0; p < numConcurrentUnits; ++p) {
	auto dateScan = S3SelectScan::make(
		fmt::format("dateScan-{}", p),
		s3Bucket,
		dateFile,
		"select * from s3object",
		dateColumns,
		dateScanRanges[p].first,
		dateScanRanges[p].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	dateScanOperators.push_back(dateScan);
  }

  return dateScanOperators;
}

std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
Queries::makeDateFilterOperators(short year, int numConcurrentUnits) {

  std::vector<std::shared_ptr<normal::pushdown::filter::Filter>> dateFilterOperators;
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto dateFilter = normal::pushdown::filter::Filter::make(
		fmt::format("dateFilter-{}", u),
		FilterPredicate::make(
			eq(cast(col("d_year"), integer32Type()), lit<::arrow::Int32Type>(year))));
	dateFilterOperators.push_back(dateFilter);
  }

  return dateFilterOperators;
}

std::vector<std::shared_ptr<FileScan>>
Queries::makeLineOrderFileScanOperators(const std::string &dataDir, int numConcurrentUnits) {

  auto lineOrderFile = filesystem::absolute(dataDir + "/lineorder.tbl");
  auto numBytesLineOrderFile = filesystem::file_size(lineOrderFile);

  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY",
	   "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE",
	   "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE"};

  std::vector<std::shared_ptr<FileScan>> lineOrderScanOperators;
  auto lineOrderScanRanges = Util::ranges<int>(0, numBytesLineOrderFile, numConcurrentUnits);
  for (int u = 0; u < numConcurrentUnits; ++u) {
	auto lineOrderScan = FileScan::make(fmt::format("lineOrderScan-{}", u),
										lineOrderFile,
										lineOrderColumns,
										lineOrderScanRanges[u].first,
										lineOrderScanRanges[u].second);
	lineOrderScanOperators.push_back(lineOrderScan);
  }

  return lineOrderScanOperators;
}

std::vector<std::shared_ptr<S3SelectScan>>
Queries::makeLineOrderS3SelectScanOperators(const std::string &s3ObjectDir,
											const std::string &s3Bucket,
											int numConcurrentUnits,
											std::unordered_map<std::string, long> partitionMap,
											AWSClient &client) {

  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto numBytesLineOrderFile = partitionMap.find(lineOrderFile)->second;

  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY",
	   "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE",
	   "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE"};

  std::vector<std::shared_ptr<S3SelectScan>> lineOrderScanOperators;
  auto lineOrderScanRanges = Util::ranges<long>(0, numBytesLineOrderFile, numConcurrentUnits);
  for (int p = 0; p < numConcurrentUnits; ++p) {
	auto lineOrderScan = S3SelectScan::make(
		fmt::format("lineOrderScan-{}", p),
		s3Bucket,
		lineOrderFile,
		"select * from s3object",
		lineOrderColumns,
		lineOrderScanRanges[p].first,
		lineOrderScanRanges[p].second,
		S3SelectCSVParseOptions(",", "\n"),
		client.defaultS3Client());
	lineOrderScanOperators.push_back(lineOrderScan);
  }

  return lineOrderScanOperators;
}

std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
Queries::makeLineOrderFilterOperators(short discount, short quantity, int numConcurrentUnits) {

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
		fmt::format("lineOrderFilter-{}", u),
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

std::shared_ptr<HashJoinBuild> Queries::makeHashJoinBuildOperators() {
  return HashJoinBuild::create("join-build", "d_datekey");
}

std::shared_ptr<HashJoinProbe> Queries::makeHashJoinProbeOperators() {
  return std::make_shared<HashJoinProbe>("join-probe",
										 JoinPredicate::create("d_datekey", "lo_orderdate"));
}

std::shared_ptr<Aggregate> Queries::makeAggregateOperators() {

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

  return aggregate;
}

std::shared_ptr<Collate> Queries::makeCollate() { return std::make_shared<Collate>("collate"); }

std::shared_ptr<OperatorManager>
Queries::query1_1DateFilterFilePullUp(const std::string &dataDir, short year, int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans = makeDateFileScanOperators(dataDir, numConcurrentUnits);
  auto dateFilters = makeDateFilterOperators(year, numConcurrentUnits);
  auto collate = makeCollate();

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

std::shared_ptr<OperatorManager> Queries::query1_1DateFilterS3PullUp(const std::string &s3Bucket,
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

  auto dateScans = makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto dateFilters = makeDateFilterOperators(year, numConcurrentUnits);
  auto collate = makeCollate();

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
Queries::query1_1LineOrderScanS3PullUp(const std::string &s3Bucket,
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
	  makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto collate = makeCollate();

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
Queries::query1_1LineOrderFilterFilePullUp(const std::string &dataDir,
										   short discount,
										   short quantity,
										   int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto lineOrderScans = makeLineOrderFileScanOperators(dataDir, numConcurrentUnits);
  auto lineOrderFilters = makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto collate = makeCollate();

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

std::shared_ptr<OperatorManager>
Queries::query1_1LineOrderFilterS3PullUp(const std::string &s3Bucket,
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
	  makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto lineOrderFilters = makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto collate = makeCollate();

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

std::shared_ptr<OperatorManager>
Queries::query1_1JoinFilePullUp(const std::string &dataDir,
								short year,
								short discount,
								short quantity,
								int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans = makeDateFileScanOperators(dataDir, numConcurrentUnits);
  auto lineOrderScans = makeLineOrderFileScanOperators(dataDir, numConcurrentUnits);
  auto dateFilters = makeDateFilterOperators(year, numConcurrentUnits);
  auto lineOrderFilters = makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto joinBuild = makeHashJoinBuildOperators();
  auto joinProbe = makeHashJoinProbeOperators();
  auto collate = makeCollate();

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
	dateFilters[u]->produce(joinBuild);
	joinBuild->consume(dateFilters[u]);
  }

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(joinProbe);
	joinProbe->consume(lineOrderFilters[u]);
  }

  joinProbe->produce(collate);
  collate->consume(joinProbe);

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager>
Queries::query1_1FilePullUp(const std::string &dataDir,
							short year,
							short discount,
							short quantity,
							int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans = makeDateFileScanOperators(dataDir, numConcurrentUnits);
  auto lineOrderScans = makeLineOrderFileScanOperators(dataDir, numConcurrentUnits);
  auto dateFilters = makeDateFilterOperators(year, numConcurrentUnits);
  auto lineOrderFilters = makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto joinBuild = makeHashJoinBuildOperators();
  auto joinProbe = makeHashJoinProbeOperators();
  auto aggregate = makeAggregateOperators();
  auto collate = makeCollate();

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
	dateFilters[u]->produce(joinBuild);
	joinBuild->consume(dateFilters[u]);
  }

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(joinProbe);
	joinProbe->consume(lineOrderFilters[u]);
  }

  joinProbe->produce(aggregate);
  aggregate->consume(joinProbe);

  aggregate->produce(collate);
  collate->consume(aggregate);

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(aggregate);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager> Queries::query1_1S3PullUp(const std::string &s3Bucket,
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

  auto dateScans = makeDateS3SelectScanOperators(s3ObjectDir, s3Bucket, numConcurrentUnits, partitionMap, client);
  auto lineOrderScans = makeLineOrderS3SelectScanOperators(s3ObjectDir, s3Bucket,
														   numConcurrentUnits, partitionMap, client);
  auto dateFilters = makeDateFilterOperators(year, numConcurrentUnits);
  auto lineOrderFilters = makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto joinBuild = makeHashJoinBuildOperators();
  auto joinProbe = makeHashJoinProbeOperators();
  auto aggregate = makeAggregateOperators();
  auto collate = makeCollate();

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
	dateFilters[u]->produce(joinBuild);
	joinBuild->consume(dateFilters[u]);
  }

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(joinProbe);
	joinProbe->consume(lineOrderFilters[u]);
  }

  joinProbe->produce(aggregate);
  aggregate->consume(joinProbe);

  aggregate->produce(collate);
  collate->consume(aggregate);

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(aggregate);
  mgr->put(collate);

  return mgr;
}

//std::shared_ptr<OperatorManager> Queries::query1_1S3PushDown(const std::string &s3Bucket,
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

std::shared_ptr<OperatorManager> Queries::query1_1S3PushDownParallel(const std::string &s3Bucket,
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
  auto collate = makeCollate();

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
std::shared_ptr<OperatorManager> Queries::query1_1S3HybridParallel(const std::string &s3Bucket,
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
  auto collate = makeCollate();

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
