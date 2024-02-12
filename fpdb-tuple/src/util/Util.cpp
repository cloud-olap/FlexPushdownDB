//
// Created by Yifei Yang on 1/18/22.
//

#include <fpdb/tuple/util/Util.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <fmt/format.h>

namespace fpdb::tuple::util {

tl::expected<std::shared_ptr<arrow::Array>, std::string>
Util::makeEmptyArray(const std::shared_ptr<arrow::DataType> &type) {
  auto expAppender = ArrayAppenderBuilder::make(type);
  if (!expAppender.has_value()) {
    return tl::make_unexpected(expAppender.error());
  }
  const auto &appender = *expAppender;

  auto expArray = appender->finalize();
  if (!expArray.has_value()) {
    return tl::make_unexpected(expArray.error());
  }
  return *expArray;
}

tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
Util::makeEmptyRecordBatch(const std::shared_ptr<arrow::Schema> &schema) {
  arrow::ArrayVector arrayVec;
  for (const auto &field: schema->fields()) {
    auto expArray = makeEmptyArray(field->type());
    if (!expArray.has_value()) {
      return tl::make_unexpected(expArray.error());
    }
    arrayVec.emplace_back(*expArray);
  }
  return arrow::RecordBatch::Make(schema, 0, arrayVec);
}

tl::expected<::arrow::RecordBatchVector, std::string>
Util::table_to_record_batches(const std::shared_ptr<arrow::Table> &table) {
  if (table == nullptr) {
    return tl::make_unexpected("Cannot make record batches from null table");
  }

  ::arrow::RecordBatchVector batches;
  if (table->num_rows() > 0) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ::arrow::TableBatchReader tbl_reader(*table);
    tbl_reader.set_chunksize(fpdb::tuple::DefaultChunkSize);
    auto status = tbl_reader.ReadAll(&batches);
    if (!status.ok()) {
      return tl::make_unexpected(status.message());
    }
  } else {
    auto expRecordBatch = makeEmptyRecordBatch(table->schema());
    if (!expRecordBatch.has_value()) {
      return tl::make_unexpected(expRecordBatch.error());
    }
    batches.emplace_back(*expRecordBatch);
  }
  return batches;
}

int64_t Util::getSize(const std::shared_ptr<arrow::RecordBatch> &recordBatch) {
  int64_t size = 0;
  for (const auto &array: recordBatch->columns()) {
    for (const auto &buffer: array->data()->buffers) {
      if (buffer) {
        size += buffer->size();
      }
    }
  }
  return size;
}

tl::expected<std::shared_ptr<arrow::Table>, std::string>
Util::calibrateSchema(const std::shared_ptr<arrow::Table> &table, const std::shared_ptr<arrow::Schema> &targetSchema) {
  // schemas are identical, just return
  if (table->schema()->Equals(targetSchema)) {
    return table;
  }

  // different number of fields
  if (table->schema()->num_fields() != targetSchema->num_fields()) {
    return tl::make_unexpected(fmt::format("Cannot calibrate schema, num fields are different, "
                                           "got '{}' but expected '{}'",
                                           table->schema()->num_fields(), targetSchema->num_fields()));
  }

  // schemas are not identical, try if we can rearrange columns to make them same
  std::unordered_map<std::string, std::shared_ptr<arrow::Field>> targetFieldMap;
  std::unordered_map<std::string, int> targetFieldIdMap;
  for (int i = 0; i < targetSchema->num_fields(); ++i) {
    const auto &field = targetSchema->field(i);
    targetFieldMap.emplace(field->name(), field);
    targetFieldIdMap.emplace(field->name(), i);
  }
  arrow::FieldVector rearrangedFields{(size_t) table->num_columns()};
  arrow::ChunkedArrayVector rearrangedColumns{(size_t) table->num_columns()};
  for (int i = 0; i < table->num_columns(); ++i) {
    const auto &field = table->schema()->field(i);
    const auto &column = table->column(i);
    // try to get field id
    auto targetFieldIt = targetFieldMap.find(field->name());
    if (targetFieldIt == targetFieldMap.end()) {
      return tl::make_unexpected(
              fmt::format("Cannot calibrate schema, field '{}' not exist in target schema", field->name()));
    }
    const auto &targetField = targetFieldIt->second;
    if (field->type()->id() != targetField->type()->id()) {
      return tl::make_unexpected(fmt::format("Cannot calibrate schema, field '{}' has different type in target schema, "
                                             "got '{}' but expected '{}'",
                                             field->name(), field->type()->name(), targetField->type()->name()));
    }
    int fieldId = targetFieldIdMap[field->name()];
    rearrangedFields[fieldId] = field;
    rearrangedColumns[fieldId] = column;
  }

  // rearrange is successful
  return arrow::Table::Make(arrow::schema(rearrangedFields), rearrangedColumns);
}

}
