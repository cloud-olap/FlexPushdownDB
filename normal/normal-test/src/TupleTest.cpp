//
// Created by matt on 2/4/20.
//

#include <doctest/doctest.h>

#include <normal/core/arrow/Arrays.h>
#include <normal/core/TupleSet.h>

#include "gandiva/projector.h"
#include "gandiva/tree_expr_builder.h"
#include "gandiva/arrow.h"
#include "gandiva/configuration.h"

#include "Globals.h"

TEST_CASE ("Make" * doctest::skip(false)) {

  auto column1 = std::vector{"1", "2", "3"};
  auto column2 = std::vector{"4", "5", "6"};
  auto column3 = std::vector{"7", "8", "9"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto arrowColumn1 = Arrays::make<arrow::StringType>(column1);
  auto arrowColumn2 = Arrays::make<arrow::StringType>(column2);
  auto arrowColumn3 = Arrays::make<arrow::StringType>(column3);

  auto tuples = normal::core::TupleSet::make(schema, {arrowColumn1, arrowColumn2, arrowColumn3});

  SPDLOG_DEBUG("Output:\n{}", tuples->toString());
}

TEST_CASE ("Expression" * doctest::skip(false)) {

  auto column1 = std::vector{"1", "2", "3"};
  auto column2 = std::vector{"4", "5", "6"};
  auto column3 = std::vector{"7", "8", "9"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto arrowColumn1 = Arrays::make<arrow::StringType>(column1);
  auto arrowColumn2 = Arrays::make<arrow::StringType>(column2);
  auto arrowColumn3 = Arrays::make<arrow::StringType>(column3);

  auto tuples = normal::core::TupleSet::make(schema, {arrowColumn1, arrowColumn2, arrowColumn3});

  SPDLOG_DEBUG("Input:\n{}", tuples->toString());

  auto exprs = std::vector<gandiva::ExpressionPtr>{
      gandiva::TreeExprBuilder::MakeExpression("castDECIMAL", {fieldA},
                                               field("a",
                                                     arrow::decimal(5, 2))),
      gandiva::TreeExprBuilder::MakeExpression("castDECIMAL", {fieldB},
                                               field("b",
                                                     arrow::decimal(5, 2))),
      gandiva::TreeExprBuilder::MakeExpression("castDECIMAL", {fieldC},
                                               field("c",
                                                     arrow::decimal(5, 2)))
  };

  // Build a projector for the expression.
  std::shared_ptr<gandiva::Projector> projector;
  auto status = gandiva::Projector::Make(schema,
                                         exprs,
                                         gandiva::ConfigurationBuilder::DefaultConfiguration(),
                                         &projector);
  assert(status.ok());

  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::TableBatchReader reader(*tuples->table());
  reader.set_chunksize(10);
  auto res = reader.ReadNext(&batch);

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*batch, arrow::default_memory_pool(), &outputs);
  assert(status.ok());

  auto resultFieldA = field("a", outputs[0]->type());
  auto resultFieldB = field("b", outputs[1]->type());
  auto resultFieldC = field("c", outputs[2]->type());
  auto resultSchema = arrow::schema({resultFieldA, resultFieldB, resultFieldC});

  auto result = normal::core::TupleSet::make(resultSchema, outputs);
  SPDLOG_DEBUG("Output:\n{}", result->toString());
}
