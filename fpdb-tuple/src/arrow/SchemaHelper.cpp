//
// Created by matt on 30/4/20.
//

#include "fpdb/tuple/arrow/SchemaHelper.h"

using namespace fpdb::tuple::arrow;

std::shared_ptr<::arrow::Schema> SchemaHelper::concatenate(const std::vector<std::shared_ptr<::arrow::Schema>>& schemas) {

  std::vector<std::shared_ptr<::arrow::Field>> fields;
  for(const auto &schema: schemas){
	fields.insert(fields.end(), schema->fields().begin(), schema->fields().end());
  }

  auto schema = ::arrow::schema(fields);

  return schema;
}
