//
// Created by matt on 5/3/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <normal/pushdown/S3SelectScan.h>
#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/core/expression/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/core/expression/Cast.h>

#include <normal/test/TestUtil.h>

using namespace normal::core::type;
using namespace normal::core::expression;

TEST_CASE ("S3SelectScan -> Sum -> Collate"
* doctest::skip(false)) {

  normal::pushdown::AWSClient client;
  client.init();

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto s3selectScan = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan",
                                                                       "s3filter",
                                                                       "tpch-sf1/customer.csv",
                                                                       "select * from S3Object limit 1000",
                                                                       "N/A",
                                                                       "N/A",
                                                                       client.defaultS3Client());

  auto sumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f5"), float64Type()));
  auto expressions2 =
      std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions2->push_back(sumExpr);

  auto aggregate = std::make_shared<normal::pushdown::Aggregate>("aggregate", expressions2);
  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  s3selectScan->produce(aggregate);
  aggregate->consume(s3selectScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  mgr->put(s3selectScan);
  mgr->put(aggregate);
  mgr->put(collate);

  normal::test::TestUtil::writeLogicalExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = tuples->value<arrow::DoubleType>("sum", 0);

      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
      CHECK(val.value() == 4400247.21);

  mgr->stop();

  client.shutdown();
}
