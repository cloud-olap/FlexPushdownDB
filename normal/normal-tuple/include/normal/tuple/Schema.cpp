//
// Created by matt on 1/5/20.
//

#include "Schema.h"

using namespace normal::tuple;

Schema::Schema(const std::shared_ptr<::arrow::Schema> &schema) : schema_(schema) {}

std::shared_ptr<Schema> Schema::concatenate(const std::vector<std::shared_ptr<Schema>> &schemas) {

  std::vector<std::shared_ptr<::arrow::Field>> fields;
  for(const auto &schema: schemas){
	fields.insert(fields.end(), schema->schema_->fields().begin(), schema->schema_->fields().end());
  }

  auto schema = ::arrow::schema(fields);

  return std::make_shared<Schema>(schema);
}

std::string Schema::showString() {
  return schema_->ToString();
}

const std::shared_ptr<::arrow::Schema> &Schema::getSchema() const {
  return schema_;
}
