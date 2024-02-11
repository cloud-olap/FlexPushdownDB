//
// Created by matt on 5/3/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <normal/pushdown/collate/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/aggregate/Aggregate.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/pushdown/project/Project.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>

#include "TestUtil.h"

using namespace normal::core::type;
using namespace normal::expression;
using namespace normal::expression::gandiva;
using namespace normal::pushdown::aggregate;
using namespace normal::pushdown::test;
using namespace normal::core::graph;

#define SKIP_SUITE true

TEST_SUITE ("filescan" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("filescan-sum-collate" * doctest::skip(true || SKIP_SUITE)) {

  auto testFile = filesystem::absolute("data/filescan/single-partition/test.csv");
  auto numBytesTestFile = filesystem::file_size(testFile);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto g = OperatorGraph::make(mgr);

  auto fileScan = normal::pushdown::file::FileScan::make("fileScan", "data/filescan/single-partition/test.csv", std::vector<std::string>{"A"}, 0, numBytesTestFile, g->getId());

  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();

  // FIXME: Why does col need to be fully classified?

  aggregateFunctions->emplace_back(std::make_shared<Sum>("Sum",
														 cast(normal::expression::gandiva::col("A"), float64Type())));

  auto aggregate = std::make_shared<normal::pushdown::aggregate::Aggregate>("aggregate", aggregateFunctions);
  auto collate = std::make_shared<normal::pushdown::collate::Collate>("collate", g->getId());

  fileScan->produce(aggregate);
  aggregate->consume(fileScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  g->put(fileScan);
  g->put(aggregate);
  g->put(collate);

  TestUtil::writeExecutionPlan(*g);

  mgr->boot();

  mgr->start();
//  mgr->join();
//
//  auto tuples = collate->tuples();
//
//  auto val = tuples->value<arrow::DoubleType>("Sum", 0);
//
//	  CHECK(tuples->numRows() == 1);
//	  CHECK(tuples->numColumns() == 1);
//	  CHECK(val == 12.0);

  mgr->stop();
}

TEST_CASE ("filescan-project-collate" * doctest::skip(false || SKIP_SUITE)) {

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto g = OperatorGraph::make(mgr);

  std::vector<std::string> columnNames = {"a","b","c"};
  auto fileScan = normal::pushdown::file::FileScan::make("fileScan", "data/filescan/single-partition/test.csv", columnNames , 0, 18, g->getId());
  auto expressions = {
	  cast(normal::expression::gandiva::col("A"), float64Type()),
	  normal::expression::gandiva::col("B")
  };
  auto project = std::make_shared<normal::pushdown::project::Project>("project", expressions);
  auto collate = std::make_shared<normal::pushdown::collate::Collate>("collate", g->getId());

  fileScan->produce(project);
  project->consume(fileScan);

  project->produce(collate);
  collate->consume(project);

  g->put(fileScan);
  g->put(project);
  g->put(collate);

  TestUtil::writeExecutionPlan(*g);

  mgr->boot();

  mgr->start();
//  mgr->join();
//
//  auto tuples = collate->tuples();
//
//  SPDLOG_DEBUG("Output:\n{}", tuples->toString());
//
//	  CHECK(tuples->numRows() == 3);
//	  CHECK(tuples->numColumns() == 2);
//
//  auto val_a_0 = tuples->value<arrow::DoubleType>("A", 0).value();
//	  CHECK(val_a_0 == 1);
//  auto val_a_1 = tuples->value<arrow::DoubleType>("A", 1).value();
//	  CHECK(val_a_1 == 4);
//  auto val_a_2 = tuples->value<arrow::DoubleType>("A", 2).value();
//	  CHECK(val_a_2 == 7);
//
//  auto val_b_0 = tuples->getString("B", 0).value();
//	  CHECK(val_b_0 == "2");
//  auto val_b_1 = tuples->getString("B", 1).value();
//	  CHECK(val_b_1 == "5");
//  auto val_b_2 = tuples->getString("B", 2).value();
//	  CHECK(val_b_2 == "8");

  mgr->stop();
}

TEST_CASE ("filescan-sum-collate-parallel" * doctest::skip(true || SKIP_SUITE)) {

  auto test01File = filesystem::absolute("data/filescan/multi-partition/test01.csv");
  auto numBytesTest01File = filesystem::file_size(test01File);

  auto test02File = filesystem::absolute("data/filescan/multi-partition/test02.csv");
  auto numBytesTest02File = filesystem::file_size(test02File);

  auto test03File = filesystem::absolute("data/filescan/multi-partition/test03.csv");
  auto numBytesTest03File = filesystem::file_size(test03File);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto g = OperatorGraph::make(mgr);

  auto fileScan01 = normal::pushdown::file::FileScan::make("fileScan01", "data/filescan/multi-partition/test01.csv", std::vector<std::string>{"A"}, 0, numBytesTest01File, g->getId());
  auto fileScan02 = normal::pushdown::file::FileScan::make("fileScan02", "data/filescan/multi-partition/test02.csv", std::vector<std::string>{"A"}, 0, numBytesTest02File, g->getId());
  auto fileScan03 = normal::pushdown::file::FileScan::make("fileScan03", "data/filescan/multi-partition/test03.csv", std::vector<std::string>{"A"}, 0, numBytesTest03File, g->getId());

  auto expressions01 = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  expressions01->emplace_back(std::make_shared<Sum>("sum(A)",
													cast(col("A"), float64Type())));
  auto aggregate01 = std::make_shared<normal::pushdown::aggregate::Aggregate>("aggregate01", expressions01);

  auto expressions02 = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  expressions02->emplace_back(std::make_shared<Sum>("sum(A)",
													cast(col("A"), float64Type())));
  auto aggregate02 = std::make_shared<normal::pushdown::aggregate::Aggregate>("aggregate02", expressions02);

  auto expressions03 = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  expressions03->emplace_back(std::make_shared<Sum>("sum(A)",
													cast(col("A"), float64Type())));
  auto aggregate03 = std::make_shared<normal::pushdown::aggregate::Aggregate>("aggregate03", expressions03);

  auto reduceAggregateExpressions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  reduceAggregateExpressions->emplace_back(std::make_shared<Sum>("sum(A)", col("sum(A)")));
  auto reduceAggregate = std::make_shared<normal::pushdown::aggregate::Aggregate>("reduceAggregate", reduceAggregateExpressions);

  auto collate = std::make_shared<normal::pushdown::collate::Collate>("collate", g->getId());

  fileScan01->produce(aggregate01);
  aggregate01->consume(fileScan01);

  fileScan02->produce(aggregate02);
  aggregate02->consume(fileScan02);

  fileScan03->produce(aggregate03);
  aggregate03->consume(fileScan03);

  aggregate01->produce(reduceAggregate);
  reduceAggregate->consume(aggregate01);

  aggregate02->produce(reduceAggregate);
  reduceAggregate->consume(aggregate02);

  aggregate03->produce(reduceAggregate);
  reduceAggregate->consume(aggregate03);

  reduceAggregate->produce(collate);
  collate->consume(reduceAggregate);

  g->put(fileScan01);
  g->put(fileScan02);
  g->put(fileScan03);
  g->put(aggregate01);
  g->put(aggregate02);
  g->put(aggregate03);
  g->put(reduceAggregate);
  g->put(collate);

  TestUtil::writeExecutionPlan(*g);

  mgr->boot();

  mgr->start();
//  mgr->join();
//
//  auto tuples = collate->tuples();
//
//  auto val = tuples->value<arrow::DoubleType>("sum(A)", 0);
//
//	  CHECK(tuples->numRows() == 1);
//	  CHECK(tuples->numColumns() == 1);
//	  CHECK(val == 36.0);

  mgr->stop();

}

}
