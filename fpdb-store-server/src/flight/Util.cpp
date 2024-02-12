//
// Created by Yifei Yang on 10/10/22.
//

#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

tl::expected<std::shared_ptr<arrow::Table>, std::string> Util::getEndTable() {
  auto schema = ::arrow::schema({{field(EndTableColumnName.data(), ::arrow::utf8())}});
  auto builder = std::make_shared<arrow::StringBuilder>();
  auto status = builder->Append(EndTableRowValue.data());
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  auto expArray = builder->Finish();
  if (!expArray.ok()) {
    return tl::make_unexpected(expArray.status().message());
  }
  return arrow::Table::Make(schema, arrow::ArrayVector{*expArray});
}

bool Util::isEndTable(const std::shared_ptr<arrow::Table> &table) {
  if (table == nullptr || table->num_rows() != 1 || table->num_columns() != 1) {
    return false;
  }
  auto column = table->GetColumnByName(EndTableColumnName.data());
  if (column == nullptr || column->type()->id() != arrow::StringType::type_id) {
    return false;
  }
  auto value = std::static_pointer_cast<arrow::StringArray>(column->chunk(0))->GetString(0);
  return value == EndTableRowValue.data();
}

tl::expected<std::shared_ptr<arrow::RecordBatch>, arrow::Status>
Util::makeTableLengthBatch(const std::vector<int64_t> &lengths) {
  size_t num_lengths = lengths.size();
  auto exp_length_buffer = arrow::AllocateBuffer(sizeof(int64_t) * num_lengths);
  if (!exp_length_buffer.ok()) {
    return tl::make_unexpected(exp_length_buffer.status());
  }
  std::shared_ptr<arrow::Buffer> length_buffer = std::move(*exp_length_buffer);
  auto length_buffer_data = length_buffer->mutable_data();
  memcpy(length_buffer_data, lengths.data(), sizeof(int64_t) * num_lengths);
  auto length_array = std::make_shared<arrow::Int64Array>(
          arrow::ArrayData::Make(arrow::int64(), num_lengths, {nullptr, length_buffer}));
  return arrow::RecordBatch::Make(arrow::schema({{field(TableLengthColumnName.data(), arrow::int64())}}),
                                  num_lengths, {length_array});
}

tl::expected<void, std::string> Util::readTableLengthBatch(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                                           std::vector<int64_t> *lengths) {
  if (recordBatch->num_columns() != 1 || recordBatch->schema()->field(0)->name() != TableLengthColumnName) {
    return tl::make_unexpected(fmt::format("Table length record batch should only have one column: '{}'",
                               TableLengthColumnName));
  }
  const auto &lengthBuffer = recordBatch->column(0)->data()->buffers[1];
  memcpy(lengths->data(), lengthBuffer->data(), sizeof(int64_t) * recordBatch->num_rows());
  return {};
}

}
