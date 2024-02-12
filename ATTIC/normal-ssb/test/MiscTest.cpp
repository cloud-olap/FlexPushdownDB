//
// Created by matt on 23/7/20.
//
//
//#include <memory>
//#include <experimental/filesystem>
//
//#include <doctest/doctest.h>
//
//#include <normal/pushdown/collate/Collate.h>
//#include <normal/core/OperatorManager.h>
//#include <normal/core/graph/OperatorGraph.h>
//#include <normal/pushdown/file/FileScan.h>
//#include <normal/tuple/TupleSet2.h>
//#include <normal/pushdown/aggregate/Sum.h>
//#include <normal/expression/gandiva/Column.h>
//#include <normal/core/type/Float64Type.h>
//#include <normal/expression/gandiva/Cast.h>
//#include <normal/pushdown/aggregate/Aggregate.h>
//#include <normal/ssb/TestUtil.h>
//#include <normal/pushdown/project/Project.h>
//#include <normal/connector/MiniCatalogue.h>
//
//using namespace normal::pushdown;
//using namespace normal::pushdown::aggregate;
//using namespace normal::tuple;
//using namespace normal::core::type;
//using namespace normal::core::graph;
//using namespace normal::expression;
//using namespace normal::expression::gandiva;
//using namespace std::experimental;
//using namespace normal::ssb;
//using namespace normal::connector;
//
//TEST_SUITE ("aggregate" * doctest::skip(false)) {
//
///**
// * Tests aggregation over large lineorder file with a project operator that ensures tuples are delivered in batches
// */
//TEST_CASE ("large" * doctest::skip(false)) {
//
//  auto aFile = filesystem::absolute("data/ssb-sf1/lineorder.tbl");
//  auto numBytesAFile = filesystem::file_size(aFile);
//
//  auto mgr = std::make_shared<OperatorManager>();
//  mgr->boot();
//  mgr->start();
//
//  auto g = OperatorGraph::make(mgr);
//
//  auto fileScan = file::FileScan::make("fileScan",
//								 aFile,
//								 std::vector<std::string>{"LO_EXTENDEDPRICE"},
//								 0,
//								 numBytesAFile,
//								 g->getId(),
//								 true);
//  auto project = std::make_shared<project::Project>("project",
//										   std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
//											   col("LO_EXTENDEDPRICE")});
//  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
//  aggregateFunctions->
//	  emplace_back(std::make_shared<Sum>("sum", cast(col("LO_EXTENDEDPRICE"), float64Type()))
//  );
//  auto aggregate = std::make_shared<Aggregate>("aggregate", aggregateFunctions);
//  auto collate = std::make_shared<Collate>("collate", g->getId());
//
//  fileScan->produce(project);
//  project->consume(fileScan);
//
//  project->produce(aggregate);
//  aggregate->consume(project);
//
//  aggregate->produce(collate);
//  collate->consume(aggregate);
//
//  g->put(fileScan);
//  g->put(project);
//  g->put(aggregate);
//  g->put(collate);
//
//  TestUtil::writeExecutionPlan2(*g);
//
//  g->boot();
//
//  g->start();
//  g->join();
//
//  auto tuples = collate->tuples();
//
//  auto val = tuples->value<arrow::DoubleType>("Sum", 0);
//
//	  CHECK(tuples->numRows() == 1);
//	  CHECK(tuples->numColumns() == 1);
//	  CHECK_EQ(22952182236037.0, val.value());
//
//  mgr->stop();
//
//}
//
//TEST_CASE ("misc-mini-catalogue" * doctest::skip(false)) {
//  auto cat = MiniCatalogue::defaultMiniCatalogue("pushdowndb", "ssb-sf1-sortlineorder");
//  REQUIRE_EQ(cat->getSchema("date")->GetFieldByName("d_datekey")->type()->id(), arrow::StringType::type_id);
//}
//
//}