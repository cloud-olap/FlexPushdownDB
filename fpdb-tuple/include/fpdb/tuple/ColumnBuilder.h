//
// Created by matt on 8/5/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNBUILDER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNBUILDER_H

#include "Column.h"
#include <arrow/array/builder_base.h>
#include <arrow/builder.h>
#include <tl/expected.hpp>
#include <memory>
#include <utility>

namespace fpdb::tuple {

class ColumnBuilder {

public:
  ColumnBuilder(std::string name, const std::shared_ptr<::arrow::DataType>& type);

  static std::shared_ptr<ColumnBuilder> make(const std::string& name, const std::shared_ptr<::arrow::DataType>& type);

  tl::expected<void, std::string> append(const std::shared_ptr<Scalar>& scalar);
  tl::expected<void, std::string> appendNulls(int64_t length);

  tl::expected<std::shared_ptr<Column>, std::string> finalize();
  tl::expected<std::shared_ptr<arrow::Array>, std::string> finalizeToArray();

private:
  std::string name_;
  std::unique_ptr<::arrow::ArrayBuilder> arrowBuilder_;
  std::shared_ptr<::arrow::Array> array_;

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNBUILDER_H
