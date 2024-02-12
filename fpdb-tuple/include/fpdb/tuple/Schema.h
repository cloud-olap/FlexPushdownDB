//
// Created by matt on 1/5/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SCHEMA_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SCHEMA_H

#include <arrow/type.h>

#include "Column.h"

namespace fpdb::tuple {

class Schema {

public:
  explicit Schema(std::shared_ptr<::arrow::Schema> Schema);

  static std::shared_ptr<Schema> make(const std::shared_ptr<::arrow::Schema> &schema);
  static std::shared_ptr<Schema> concatenate(const std::vector<std::shared_ptr<Schema>>& schemas);

  [[nodiscard]] const std::shared_ptr<::arrow::Schema> &getSchema() const;
  [[nodiscard]] const std::vector<std::shared_ptr<::arrow::Field>> &fields() const;

  int getFieldIndexByName(const std::string& name);

  std::vector<std::shared_ptr<Column>> makeColumns();

  std::string showString();

private:
  std::shared_ptr<::arrow::Schema> schema_;

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SCHEMA_H
