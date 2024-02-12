//
// Created by matt on 10/4/20.
//

#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCALARHELPER_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCALARHELPER_H

class ScalarHelper {
public:
  virtual ~ScalarHelper() = default;

  virtual ScalarHelper &operator+=(const std::shared_ptr<ScalarHelper> &rhs) = 0;

  virtual std::shared_ptr<arrow::Scalar> asScalar() = 0;

  virtual std::string toString() = 0;
};

#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCALARHELPER_H
