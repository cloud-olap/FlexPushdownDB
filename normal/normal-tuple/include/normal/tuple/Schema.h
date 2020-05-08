//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCHEMA_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCHEMA_H

#include <arrow/type.h>

#include "Column.h"

namespace normal::tuple {

class Schema {

public:
  explicit Schema(std::shared_ptr<::arrow::Schema> Schema);

  static std::shared_ptr<Schema> concatenate(const std::vector<std::shared_ptr<Schema>>& schemas);

  [[nodiscard]] const std::shared_ptr<::arrow::Schema> &getSchema() const;
  [[nodiscard]] const std::vector<std::shared_ptr<::arrow::Field>> &fields() const{
    return schema_->fields();
  };

  std::vector<std::shared_ptr<Column>> makeColumns(){
	std::vector<std::shared_ptr<Column>> columns;
	columns.reserve(schema_->fields().size());
    for(const auto &field: schema_->fields()){
      auto column = Column::make(field->name(), field->type());
	  columns.emplace_back(column);
    }
    return columns;
  }

  std::string showString();

private:
  std::shared_ptr<::arrow::Schema> schema_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCHEMA_H
