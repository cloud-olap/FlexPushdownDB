//
// Created by matt on 6/5/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_FILTER_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_FILTER_H

#include <fpdb/expression/gandiva/Expression.h>
#include <gandiva/expression.h>
#include <gandiva/filter.h>
#include <fpdb/expression/Filter.h>
#include "Projector.h"
#include <map>

namespace fpdb::expression::gandiva {

class Filter : public fpdb::expression::Filter {

public:
  explicit Filter(std::shared_ptr<Expression> Pred);

  static std::shared_ptr<Filter> make(const std::shared_ptr<Expression> &Pred);

  /**
   * Evaluate filter on recordBatch using selectionVector, static function
   * @param recordBatch
   * @param selectionVector
   * @return
   */
  static tl::expected<arrow::ArrayVector, std::string>
  evaluateBySelectionVectorStatic(const arrow::RecordBatch &recordBatch,
                                  const std::shared_ptr<::gandiva::SelectionVector> &selectionVector);

  /**
   * Evaluate filter on tupleSet completely
   * @param TupleSet
   * @return
   */
  tl::expected<std::shared_ptr<fpdb::tuple::TupleSet>, std::string>
  evaluate(const fpdb::tuple::TupleSet &TupleSet) override;

  /**
   * Evaluate filter on recordBatch completely
   * @param recordBatch
   * @return
   */
  tl::expected<arrow::ArrayVector, std::string> evaluate(const arrow::RecordBatch &recordBatch) override;

  /**
   * Compute selectionVector on recordBatch
   * @param recordBatch
   * @return
   */
  tl::expected<std::shared_ptr<::gandiva::SelectionVector>, std::string>
  computeSelectionVector(const arrow::RecordBatch &recordBatch);

  /**
   * Evaluate filter on recordBatch using selectionVector
   * @param recordBatch
   * @param selectionVector
   * @return
   */
  tl::expected<arrow::ArrayVector, std::string>
  evaluateBySelectionVector(const arrow::RecordBatch &recordBatch,
                            const std::shared_ptr<::gandiva::SelectionVector> &selectionVector);

  /**
   * Compile filter, i.e. generate gandivaFilter_ and gandivaProjector_
   * @param inputSchema
   * @param outputSchema
   * @return
   */
  tl::expected<void, std::string> compile(const std::shared_ptr<arrow::Schema> &inputSchema,
                                          const std::shared_ptr<arrow::Schema> &outputSchema) override;

private:
  std::shared_ptr<Expression> pred_;
  std::shared_ptr<arrow::Schema> inputSchema_;
  std::shared_ptr<arrow::Schema> outputSchema_;
  std::shared_ptr<::gandiva::Filter> gandivaFilter_;
  std::shared_ptr<::gandiva::Projector> gandivaProjector_;
};

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_FILTER_H
