//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCALAR_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCALAR_H

#include <memory>
#include <arrow/scalar.h>

namespace normal::tuple {

class Scalar {

public:
  explicit Scalar(std::shared_ptr<::arrow::Scalar> scalar);

  std::shared_ptr<::arrow::DataType> type();

  std::string showString();

private:
  std::shared_ptr<::arrow::Scalar> scalar_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCALAR_H
