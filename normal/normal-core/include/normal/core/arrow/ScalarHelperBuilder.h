//
// Created by matt on 10/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPERBUILDER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPERBUILDER_H

#include <tl/expected.hpp>
#include <memory>
#include <arrow/scalar.h>
#include "ScalarHelperImpl.h"
#include "ScalarHelper.h"

class ScalarHelperBuilder {

public:

  static tl::expected<std::shared_ptr<ScalarHelper>, std::string> make(std::shared_ptr<arrow::Scalar> &scalar) {
    if (scalar->type->id() == arrow::DoubleType::type_id) {
      std::shared_ptr<arrow::DoubleScalar> doubleScalar = std::static_pointer_cast<arrow::DoubleScalar>(scalar);
      auto helper = std::make_shared<ScalarHelperImpl<arrow::DoubleType, double>>(doubleScalar);
      return std::static_pointer_cast<ScalarHelper>(helper);
    } else {
      return tl::unexpected("Unrecognized type " + scalar->type->name());
    }
  }
};

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPERBUILDER_H
