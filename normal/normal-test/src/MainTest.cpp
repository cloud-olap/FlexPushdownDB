//
// Created by matt on 4/12/19.
//

#include <string>
#include <memory>
#include <vector>

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <arrow/array/builder_binary.h>           // for StringBuilder
#include <arrow/table.h>                          // for Table
#include <arrow/type.h>                           // for field, schema, Schema
#include <arrow/type_fwd.h>                       // for default_memory_pool
#include <bits/shared_ptr.h>                      // for shared_ptr, make_sh...
#include <cstdio>                                // for FILENAME_MAX
#include <unistd.h>                               // for getcwd
#include <iostream>                               // for cout

#include "normal/pushdown/S3SelectScan.h"
#include "normal/pushdown/Collate.h"
#include "normal/core/OperatorContext.h"
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/FileScan.h>
#include "normal/core/TupleSet.h"                 // for TupleSet
#include "normal/pushdown/AggregateExpression.h"  // for AggregateExpression

namespace arrow { class Array; }
namespace arrow { class MemoryPool; }
namespace arrow { class StringArray; }

//TEST_CASE ("Operator lifecycle") {
//
//  auto mgr = std::make_shared<OperatorManager>();
//  auto s3selectScan = std::make_shared<S3SelectScan>("s3selectScan");
//  auto ctx = std::make_shared<OperatorContext>(s3selectScan, mgr);
//
//  s3selectScan->create(ctx);
//  s3selectScan->start();
//      CHECK(s3selectScan->running());
//
//  s3selectScan->stop();
//      CHECK(!s3selectScan->running());
//}
//
//TEST_CASE ("OperatorManager lifecycle") {
//
//  auto s3selectScan1 = std::make_shared<S3SelectScan>("s3selectScan1");
//  auto s3selectScan2 = std::make_shared<S3SelectScan>("s3selectScan2");
//  auto s3selectScan3 = std::make_shared<S3SelectScan>("s3selectScan3");
//  auto mgr = std::make_shared<OperatorManager>();
//
//  mgr->put(s3selectScan1);
//  mgr->put(s3selectScan2);
//  mgr->put(s3selectScan3);
//
//  mgr->start();
//  mgr->stop();
//}
//
//TEST_CASE ("OperatorManager execution") {
//
//  spdlog::info("Test");
//
//  auto s3selectScan1 = std::make_shared<S3SelectScan>("s3selectScan1");
//  auto s3selectScan2 = std::make_shared<S3SelectScan>("s3selectScan2");
//  auto s3selectScan3 = std::make_shared<S3SelectScan>("s3selectScan3");
//  auto collate = std::make_shared<Collate>("collate");
//
//  s3selectScan1->produce(s3selectScan2);
//  s3selectScan2->consume(s3selectScan1);
//
//  s3selectScan2->produce(s3selectScan3);
//  s3selectScan3->consume(s3selectScan2);
//
//  s3selectScan3->produce(collate);
//  collate->consume(s3selectScan3);
//
//  auto mgr = std::make_shared<OperatorManager>();
//
//  mgr->put(s3selectScan1);
//  mgr->put(s3selectScan2);
//  mgr->put(s3selectScan3);
//  mgr->put(collate);
//
//  mgr->start();
//  mgr->stop();
//
//  collate->show();
//}

//TEST_CASE ("File Scan -> Sum -> Collate") {
//
//  char buff[FILENAME_MAX];
//  getcwd(buff, FILENAME_MAX);
//  std::string current_working_dir(buff);
//
//  std::cout << current_working_dir;
//
//  auto fn = [](std::shared_ptr<TupleSet> tupleSet) -> std::shared_ptr<TupleSet> {
//
//    long sum = 0;
//
//    auto fieldIndex = tupleSet->getTable()->schema()->GetFieldIndex("A");
//
//    for (int r = 0; r < tupleSet->numRows(); ++r) {
//      auto s = tupleSet->getValue(fieldIndex, r);
//      sum += std::stol(s);
//    }
//
//    auto s = std::to_string(sum);
//    std::vector<std::shared_ptr<std::string>> data;
//    data.push_back(std::make_shared<std::string>(s));
//
//    std::shared_ptr<arrow::Schema> schema;
//
//    std::shared_ptr<arrow::Field> field;
//    field = arrow::field("sum(A)", arrow::utf8());
//
//    schema = arrow::schema({field});
//
//    spdlog::info("\n" + schema->ToString());
//
//    arrow::MemoryPool *pool = arrow::default_memory_pool();
//    arrow::StringBuilder colBuilder(pool);
//
//    colBuilder.Append(s);
//
//    std::shared_ptr<arrow::StringArray> col;
//    colBuilder.Finish(&col);
//
//    auto columns = std::vector<std::shared_ptr<arrow::Array>>{col};
//
//    std::shared_ptr<arrow::Table> table;
//    table = arrow::Table::Make(schema, columns);
//
//    std::shared_ptr<TupleSet> outTupleSet = TupleSet::make(table);
//
//    return outTupleSet;
//  };
//
//  auto aggregateExpression = std::make_unique<AggregateExpression>(fn);
//  auto aggregateExpressions = std::vector<std::unique_ptr<AggregateExpression>>();
//  aggregateExpressions.push_back(std::move(aggregateExpression));
//
//  auto s3selectScan = std::make_shared<FileScan>(std::string("fileScan"), std::string("data/test.csv"));
//  auto aggregate = std::make_shared<Aggregate>("aggregate", std::move(aggregateExpressions));
//  auto collate = std::make_shared<Collate>("collate");
//
//  s3selectScan->produce(aggregate);
//  aggregate->consume(s3selectScan);
//
//  aggregate->produce(collate);
//  collate->consume(aggregate);
//
//  auto mgr = std::make_shared<OperatorManager>();
//
//  mgr->put(s3selectScan);
//  mgr->put(aggregate);
//  mgr->put(collate);
//
//  mgr->start();
//  mgr->stop();
//
//  collate->show();
//}

TEST_CASE ("S3SelectScan -> Sum -> Collate") {

  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  std::string current_working_dir(buff);

  std::cout << current_working_dir;

  auto fn = [](std::shared_ptr<TupleSet> dataTupleSet, std::shared_ptr<TupleSet> aggregateTupleSet) -> std::shared_ptr<TupleSet> {

    // Compute sum for current data
    long sum = 0;

    spdlog::info("Data:\n{}", dataTupleSet->toString());

    auto fieldIndex = dataTupleSet->getTable()->schema()->GetFieldIndex("f0");

    for (int r = 0; r < dataTupleSet->numRows(); ++r) {
      auto s = dataTupleSet->getValue(fieldIndex, r);

      spdlog::info("Row:\n{}", s);

      long l = std::stol(s);
      sum += l;
    }

    // Get current sum
    long currentSum = 0;

    if(aggregateTupleSet->getTable()->num_rows() > 0) {
      auto sumFieldIndex = aggregateTupleSet->getTable()->schema()->GetFieldIndex("sum(f0)");
      auto currentSumString = aggregateTupleSet->getValue(sumFieldIndex, 0);
      currentSum = std::stol(currentSumString);
    }

    long newCurrentSum = currentSum + sum;

    if(aggregateTupleSet->getTable()->num_rows() == 0) {

      std::vector<std::shared_ptr<long>> data;
      data.push_back(std::make_shared<long>(newCurrentSum));

      std::shared_ptr<arrow::Schema> schema;

      std::shared_ptr<arrow::Field> field;
      field = arrow::field("sum(f0)", arrow::int64());

      schema = arrow::schema({field});

      spdlog::info("\n" + schema->ToString());

      arrow::MemoryPool *pool = arrow::default_memory_pool();
      arrow::Int64Builder colBuilder(pool);

      colBuilder.Append(sum);

      std::shared_ptr<arrow::Int64Array> col;
      colBuilder.Finish(&col);

      auto columns = std::vector<std::shared_ptr<arrow::Array>>{col};

      std::shared_ptr<arrow::Table> table;
      table = arrow::Table::Make(schema, columns);

      std::shared_ptr<TupleSet> aggregateTupleSet = TupleSet::make(table);
    }

    return aggregateTupleSet;
  };

  auto aggregateExpression = std::make_unique<AggregateExpression>(fn);
  auto aggregateExpressions = std::vector<std::unique_ptr<AggregateExpression>>();
  aggregateExpressions.push_back(std::move(aggregateExpression));

  auto s3selectScan = std::make_shared<S3SelectScan>("s3SelectScan",
                                                     "s3filter",
                                                     "tpch-sf1/customer.csv",
                                                     "select * from S3Object limit 1000");
  auto aggregate = std::make_shared<Aggregate>("aggregate", std::move(aggregateExpressions));
  auto collate = std::make_shared<Collate>("collate");

  s3selectScan->produce(aggregate);
  aggregate->consume(s3selectScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  auto mgr = std::make_shared<OperatorManager>();

  mgr->put(s3selectScan);
  mgr->put(aggregate);
  mgr->put(collate);

  mgr->start();
  mgr->stop();

  collate->show();

//  11250075000
}