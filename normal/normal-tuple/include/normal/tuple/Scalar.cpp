//
// Created by matt on 1/5/20.
//

#include "Scalar.h"

#include <utility>

using namespace normal::tuple;

Scalar::Scalar(std::shared_ptr<::arrow::Scalar> scalar) : scalar_(std::move(scalar)) {}

std::shared_ptr<::arrow::DataType> Scalar::type() {
  return scalar_->type;
}

std::string Scalar::showString() {
  return scalar_->ToString();
}

