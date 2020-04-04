//
// Created by matt on 5/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONS_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONS_H

#include <memory>
#include <arrow/array.h>
#include <gandiva/tree_expr_builder.h>
#include <gandiva/configuration.h>
#include <gandiva/projector.h>
#include "Expression.h"

/**
 * Utility methods for working with expressions and Arrow
 */
class Expressions {
public:
  /**
   * Evaluats the given expressions against an Arrow record batch
   * @param expressions
   * @param recordBatch
   * @return
   */
  static std::shared_ptr<arrow::ArrayVector> evaluate(
      const std::vector<std::shared_ptr<normal::core::expression::Expression>>& expressions,
      const arrow::RecordBatch& recordBatch) {

    // Prepare a schema for the results
    auto resultFields = std::vector<std::shared_ptr<arrow::Field>>();
    for (const auto &e: expressions) {
      resultFields.emplace_back(arrow::field(e->name(),e->resultType(recordBatch.schema())));
    }
    auto resultSchema = arrow::schema(resultFields);

    // Create gandiva expressions
    std::vector<gandiva::ExpressionPtr> gandivaExpressions;
    gandivaExpressions.reserve(expressions.size());
    for (const auto &e: expressions) {
      gandivaExpressions.emplace_back(gandiva::TreeExprBuilder::MakeExpression(e->buildGandivaExpression(recordBatch.schema()),
                                                                               field(e->name(),
                                                                                     e->resultType(recordBatch.schema()))));
    }

    // Build a projector for the expression.
    std::shared_ptr<gandiva::Projector> projector;
    auto status = gandiva::Projector::Make(recordBatch.schema(),
                                           gandivaExpressions,
                                           gandiva::ConfigurationBuilder::DefaultConfiguration(),
                                           &projector);
    assert(status.ok());

    // Evaluate the expressions
    auto outputs = std::make_shared<arrow::ArrayVector>();
    status = projector->Evaluate(recordBatch, arrow::default_memory_pool(), &*outputs);
    assert(status.ok());

    return outputs;
  }
};

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONS_H
