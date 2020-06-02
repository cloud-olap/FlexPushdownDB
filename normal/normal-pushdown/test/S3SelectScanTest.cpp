//
// Created by matt on 5/3/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>

#include "TestUtil.h"

using namespace normal::pushdown;
using namespace normal::pushdown::test;
using namespace normal::core;
using namespace normal::core::type;
using namespace normal::expression;
using namespace normal::expression::gandiva;

#define SKIP_SUITE true

TEST_SUITE ("s3" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("s3selectscan-sum-collate" * doctest::skip(false || SKIP_SUITE)) {

  normal::pushdown::AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();

  std::vector<std::string> columns = {"c_acctbal"};

  auto s3selectScan = std::make_shared<S3SelectScan>("s3SelectScan",
													 "s3filter",
													 "tpch-sf1/customer.csv",
													 "select c_acctbal from S3Object",
													 columns,
													 0,
													 1023,
													 client.defaultS3Client());

  auto sumExpr = std::make_shared<aggregate::Sum>("sum", cast(col("c_acctbal"), float64Type()));
  auto expressions2 =
	  std::make_shared<std::vector<std::shared_ptr<aggregate::AggregationFunction>>>();
  expressions2->push_back(sumExpr);

  auto aggregate = std::make_shared<Aggregate>("aggregate", expressions2);
  auto collate = std::make_shared<Collate>("collate");

  s3selectScan->produce(aggregate);
  aggregate->consume(s3selectScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  mgr->put(s3selectScan);
  mgr->put(aggregate);
  mgr->put(collate);

  TestUtil::writeExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = tuples->value<arrow::DoubleType>("sum", 0);

	  CHECK(tuples->numRows() == 1);
	  CHECK(tuples->numColumns() == 1);
	  CHECK(val.value() == 29194.0);

  mgr->stop();

  client.shutdown();
}

}
