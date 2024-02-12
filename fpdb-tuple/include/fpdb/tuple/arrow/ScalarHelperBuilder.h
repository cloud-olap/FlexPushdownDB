//
// Created by matt on 10/4/20.
//

#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCALARHELPERBUILDER_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCALARHELPERBUILDER_H

#include <tl/expected.hpp>
#include <memory>
#include <arrow/scalar.h>
#include "ScalarHelperImpl.h"
#include "fpdb/tuple/arrow/ScalarHelper.h"

class ScalarHelperBuilder {

public:

  static tl::expected<std::shared_ptr<ScalarHelper>, std::string> make(std::shared_ptr<arrow::Scalar> &scalar) {
    if (scalar->type->id() == arrow::Int32Type::type_id) {
      auto int32Scalar = std::static_pointer_cast<arrow::Int32Scalar>(scalar);
      return std::make_shared<ScalarHelperImpl<arrow::Int32Type, arrow::Int32Type::c_type>>(int32Scalar);
    } else if (scalar->type->id() == arrow::Int64Type::type_id) {
      auto int64Scalar = std::static_pointer_cast<arrow::Int64Scalar>(scalar);
      return std::make_shared<ScalarHelperImpl<arrow::Int64Type, arrow::Int64Type::c_type>>(int64Scalar);
    } else if (scalar->type->id() == arrow::DoubleType::type_id) {
      auto doubleScalar = std::static_pointer_cast<arrow::DoubleScalar>(scalar);
      return std::make_shared<ScalarHelperImpl<arrow::DoubleType, arrow::DoubleType::c_type>>(doubleScalar);
    } else {
      return tl::unexpected("Unrecognized type " + scalar->type->name() + " for ScalarHelperBuilder");
    }
  }
};

#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCALARHELPERBUILDER_H
