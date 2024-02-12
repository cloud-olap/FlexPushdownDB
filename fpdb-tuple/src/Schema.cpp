//
// Created by matt on 1/5/20.
//

#include "fpdb/tuple/Schema.h"

#include <utility>

using namespace fpdb::tuple;

Schema::Schema(std::shared_ptr<::arrow::Schema> schema) : schema_(std::move(schema)) {}

std::shared_ptr<Schema> Schema::make(const std::shared_ptr<::arrow::Schema> &schema) {
  return std::make_shared<Schema>(schema);
}

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

std::vector<std::shared_ptr<Column>> Schema::makeColumns() {
  std::vector<std::shared_ptr<Column>> columns;
  columns.reserve(schema_->fields().size());
  for(const auto &field: schema_->fields()){
	auto column = Column::make(field->name(), field->type());
	columns.emplace_back(column);
  }
  return columns;
}

const std::vector<std::shared_ptr<::arrow::Field>> &Schema::fields() const {
  return schema_->fields();
}

int Schema::getFieldIndexByName(const std::string& name) {
  return schema_->GetFieldIndex(name);
}
