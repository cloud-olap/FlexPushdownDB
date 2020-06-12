//
// Created by matt on 24/4/20.
//

#include <vector>

#include <doctest/doctest.h>

#include <normal/ssb/TestUtil.h>
#include <normal/sql/Interpreter.h>
#include <normal/connector/local-fs/LocalFileSystemConnector.h>
#include <normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h>
#include <normal/connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
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
#include "normal/ssb/Queries.h"

using namespace normal::ssb;
using namespace normal::pushdown;
using namespace normal::pushdown::aggregate;
using namespace normal::pushdown::filter;
using namespace normal::core::type;
using namespace normal::expression;
using namespace normal::expression::gandiva;
using namespace normal::pushdown::join;

void configureLocalConnector(normal::sql::Interpreter &i) {

  auto conn = std::make_shared<normal::connector::local_fs::LocalFileSystemConnector>("local_fs");

  auto cat = std::make_shared<normal::connector::Catalogue>("local_fs", conn);

  auto partitioningScheme1 = std::make_shared<LocalFileExplicitPartitioningScheme>();
  partitioningScheme1->add(std::make_shared<LocalFilePartition>("data/lineorder.csv"));
  cat->put(std::make_shared<normal::connector::local_fs::LocalFileSystemCatalogueEntry>("lineorder",
																						partitioningScheme1,
																						cat));

  auto partitioningScheme2 = std::make_shared<LocalFileExplicitPartitioningScheme>();
  partitioningScheme2->add(std::make_shared<LocalFilePartition>("data/date.csv"));
  cat->put(std::make_shared<normal::connector::local_fs::LocalFileSystemCatalogueEntry>("date",
																						partitioningScheme2,
																						cat));

  i.put(cat);
}

void configureS3Connector(normal::sql::Interpreter &i) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  auto partitioningScheme1 = std::make_shared<S3SelectExplicitPartitioningScheme>();
  partitioningScheme1->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer.csv"));
  cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>("customer", partitioningScheme1, cat));

  // FIXME: Don't think these are the actual partitions, need to look them up
  auto partitioningScheme2 = std::make_shared<S3SelectExplicitPartitioningScheme>();
  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_01.csv"));
  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_02.csv"));
  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_03.csv"));
  cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>("customer_partitioned",
																		   partitioningScheme2,
																		   cat));

  i.put(cat);
}

auto execute(normal::sql::Interpreter &i) {
  i.getOperatorManager()->boot();
  i.getOperatorManager()->start();
  i.getOperatorManager()->join();

  std::shared_ptr<normal::pushdown::Collate>
	  collate = std::static_pointer_cast<normal::pushdown::Collate>(i.getOperatorManager()->getOperator("collate"));

  auto tuples = collate->tuples();

  SPDLOG_DEBUG("Output:\n{}", tuples->toString());

  return tuples;
}

auto executeSQLTest(const std::string &sql) {

  SPDLOG_INFO("SQL:\n{}", sql);

  normal::sql::Interpreter i;

  configureLocalConnector(i);
  configureS3Connector(i);

  i.parse(sql);

  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
  TestUtil::writeExecutionPlan(*i.getOperatorManager());

  auto tuples = execute(i);

  i.getOperatorManager()->stop();

  SPDLOG_INFO("Metrics:\n{}", i.getOperatorManager()->showMetrics());

  return tuples;
}

#define SKIP_SUITE false

TEST_SUITE ("ssb" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-benchmark-sql-query01" * doctest::skip(true || SKIP_SUITE)) {

  short year = 1993;
  short discount = 2;
  short quantity = 24;

  auto sql = Queries::query01(year, discount, quantity);

  auto tuples = executeSQLTest(sql);
}

TEST_CASE ("ssb-benchmark-ep-query01" * doctest::skip(false || SKIP_SUITE)) {

  short year = 1992;
  short discount = 2;
  short quantity = 24;
  std::string dataDir = "data/ssb-sf0.01"; // NOTE: Need to generate data in this dir first

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}", dataDir, year, discount, quantity);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

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

  short discountLower = discount - 1;
  short discountUpper = discount + 1;

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
  lineOrderScan->produce(lineOrderFilter);
  lineOrderFilter->consume(lineOrderScan);

  dateScan->produce(dateFilter);
  dateFilter->consume(dateScan);

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

  TestUtil::writeExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  mgr->stop();

  auto tupleSet = TupleSet2::create(tuples);

  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

}
