//
// Created by matt on 5/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONS_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONS_H

#include <memory>
#include <arrow/api.h>
#include <gandiva/tree_expr_builder.h>
#include <gandiva/configuration.h>
#include <gandiva/projector.h>
#include "Expression.h"
#include "Projector.h"
#include "IProjector.h"

namespace normal::expression{

/**
 * Utility methods for working with expressions and Arrow
 */
class Expressions {
public:

  /**
   * Evaluates the given expressions against an Arrow record batch
   * @param projector
   * @param recordBatch
   * @return
   */
  static std::shared_ptr<arrow::ArrayVector> evaluate(
	  const std::shared_ptr<IProjector> &projector,
	  const arrow::RecordBatch &recordBatch);

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONS_H
