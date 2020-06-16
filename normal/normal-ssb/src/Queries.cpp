//
// Created by matt on 28/4/20.
//

#include <normal/ssb/Queries.h>

#include <experimental/filesystem>

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

std::string Queries::query01(short year, short discount, short quantity) {

  // FIXME: using catalogies until default catalogue implemented
  auto sql = fmt::format(
	  "select "
	  "sum(lo_extendedprice * lo_discount) as revenue "
	  "from "
	  "local_fs.lineorder, local_fs.date "
	  "where "
	  "lo_orderdate = d_datekey "
	  "and d_year = {0} -- Specific values below "
	  "and lo_discount between {1} - 1 "
	  "and {1} + 1 and lo_quantity < {2}; ",
	  year,
	  discount,
	  quantity
  );

  return sql;
}

std::shared_ptr<OperatorManager> Queries::query1_1FilePullUp(const std::string &dataDir,
															 short year,
															 short discount,
															 short quantity) {

  auto mgr = std::make_shared<OperatorManager>();

  /**
   * Scan
   * lineorder.tbl
   * date.tbl
   */
  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY",
	   "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE",
	   "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE"};
  auto lineOrderFile = filesystem::absolute(dataDir + "/lineorder.tbl");
  auto numBytesLineOrderFile = filesystem::file_size(lineOrderFile);
  auto lineOrderScan = FileScan::make("lineOrderScan", lineOrderFile, lineOrderColumns, 0, numBytesLineOrderFile);
  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK",
	   "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON",
	   "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL"};
  auto dateFile = filesystem::absolute(dataDir + "/date.tbl");
  auto numBytesDateFile = filesystem::file_size(dateFile);
  auto dateScan = FileScan::make("dateScan", dateFile, dateColumns, 0, numBytesDateFile);

  /**
   * Filter
   * d_year (f4) = 1993
   * and lo_discount (f11) between 1 and 3
   * and lo_quantity (f8) < 25
   */
  auto dateFilter = normal::pushdown::filter::Filter::make(
	  "dateFilter",
	  FilterPredicate::make(
		  eq(cast(col("d_year"), integer32Type()), lit<::arrow::Int32Type>(year))));

  int discountLower = discount - 1;
  int discountUpper = discount + 1;

  auto lineOrderFilter = normal::pushdown::filter::Filter::make(
	  "lineOrderFilter",
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
  auto collate = std::make_shared<Collate>("collate");

  // Wire up
  dateScan->produce(dateFilter);
  dateFilter->consume(dateScan);

  lineOrderScan->produce(lineOrderFilter);
  lineOrderFilter->consume(lineOrderScan);

  dateFilter->produce(joinBuild);
  joinBuild->consume(dateFilter);

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  lineOrderFilter->produce(joinProbe);
  joinProbe->consume(lineOrderFilter);

  joinProbe->produce(aggregate);
  aggregate->consume(joinProbe);

  aggregate->produce(collate);
  collate->consume(aggregate);

  mgr->put(lineOrderScan);
  mgr->put(dateScan);
  mgr->put(lineOrderFilter);
  mgr->put(dateFilter);
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
   * lineorder.tbl
   * date.tbl
   */
  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY",
	   "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE",
	   "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE"};
  auto numBytesLineOrderFile = partitionMap.find(lineOrderFile)->second;
  auto lineOrderScan = S3SelectScan::make(
	  "lineOrderScan",
	  s3Bucket,
	  lineOrderFile,
	  "select * from s3object",
	  lineOrderColumns,
	  0,
	  numBytesLineOrderFile,
	  S3SelectCSVParseOptions(",", "\n"),
	  client.defaultS3Client());

  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK",
	   "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON",
	   "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL"};

  auto numBytesDateFile = partitionMap.find(dateFile)->second;
  auto dateScan = S3SelectScan::make(
	  "dateScan",
	  s3Bucket,
	  dateFile,
	  "select * from s3object",
	  dateColumns,
	  0,
	  numBytesDateFile,
	  S3SelectCSVParseOptions(",", "\n"),
	  client.defaultS3Client());

  /**
   * Filter
   * d_year (f4) = 1993
   * and lo_discount (f11) between 1 and 3
   * and lo_quantity (f8) < 25
   */
  auto dateFilter = normal::pushdown::filter::Filter::make(
	  "dateFilter",
	  FilterPredicate::make(
		  eq(cast(col("d_year"), integer32Type()), lit<::arrow::Int32Type>(year))));

  int discountLower = discount - 1;
  int discountUpper = discount + 1;

  auto lineOrderFilter = normal::pushdown::filter::Filter::make(
	  "lineOrderFilter",
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
  auto collate = std::make_shared<Collate>("collate");

  // Wire up
  dateScan->produce(dateFilter);
  dateFilter->consume(dateScan);

  lineOrderScan->produce(lineOrderFilter);
  lineOrderFilter->consume(lineOrderScan);

  dateFilter->produce(joinBuild);
  joinBuild->consume(dateFilter);

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  lineOrderFilter->produce(joinProbe);
  joinProbe->consume(lineOrderFilter);

  joinProbe->produce(aggregate);
  aggregate->consume(joinProbe);

  aggregate->produce(collate);
  collate->consume(aggregate);

  mgr->put(lineOrderScan);
  mgr->put(dateScan);
  mgr->put(lineOrderFilter);
  mgr->put(dateFilter);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(aggregate);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager> Queries::query1_1S3PushDown(const std::string &s3Bucket,
															 const std::string &s3ObjectDir,
															 short year,
															 short discount,
															 short quantity,
															 AWSClient &client) {

  auto mgr = std::make_shared<OperatorManager>();

  /**
   * Scan
   * lineorder.tbl
   * date.tbl
   */
  std::vector<std::string> lineOrderColumns =
	  {"LO_ORDERDATE", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_DISCOUNT", "LO_REVENUE"};
  auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
  auto numBytesLineOrderFile = filesystem::file_size(lineOrderFile);
  int discountLower = discount - 1;
  int discountUpper = discount + 1;
  auto lineOrderScan = S3SelectScan::make(
	  "lineOrderScan",
	  s3Bucket,
	  lineOrderFile,
	  fmt::format(
		  "select LO_ORDERDATE, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE from s3Object where cast(LO_DISCOUNT as int) between {} and {} and cast(LO_QUANTITY as int) < {}",
		  discountLower,
		  discountUpper,
		  quantity),
	  lineOrderColumns,
	  0,
	  numBytesLineOrderFile,
	  S3SelectCSVParseOptions(",", "\n"),
	  client.defaultS3Client());

  std::vector<std::string> dateColumns =
	  {"D_DATEKEY", "D_YEAR",};
  auto dateFile = s3ObjectDir + "/date.tbl";
  auto numBytesDateFile = filesystem::file_size(dateFile);
  auto dateScan = S3SelectScan::make(
	  "dateScan",
	  s3Bucket,
	  dateFile,
	  fmt::format("select D_DATEKEY, D_YEAR from s3Object where d_datekey = '{}'", year),
	  dateColumns,
	  0,
	  numBytesDateFile,
	  S3SelectCSVParseOptions(",", "\n"),
	  client.defaultS3Client());

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
  auto collate = std::make_shared<Collate>("collate");

  // Wire up
  dateScan->produce(joinBuild);
  joinBuild->consume(dateScan);

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  lineOrderScan->produce(joinProbe);
  joinProbe->consume(lineOrderScan);

  joinProbe->produce(aggregate);
  aggregate->consume(joinProbe);

  aggregate->produce(collate);
  collate->consume(aggregate);

  mgr->put(lineOrderScan);
  mgr->put(dateScan);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(aggregate);
  mgr->put(collate);

  return mgr;
}
