//
// Created by Yifei Yang on 1/12/22.
//

#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fmt/format.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

namespace fpdb::tuple {

std::shared_ptr<arrow::Table> ArrowSerializer::bytes_to_table(const std::vector<std::uint8_t>& bytes_vec,
                                                              bool copy_view) {
  if (bytes_vec.empty()) {
    return nullptr;
  }

  arrow::Status status;

  // Create a view over the given byte vector, but then get a copy because the vector ref eventually disappears
  auto buffer_view = ::arrow::Buffer::Wrap(bytes_vec);
  if (copy_view) {
    auto maybe_buffer = buffer_view->CopySlice(0, buffer_view->size(), arrow::default_memory_pool());
    if (!maybe_buffer.ok())
      throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", maybe_buffer.status().message()));
    buffer_view = *maybe_buffer;
  }

  // Get a reader over the buffer
  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(buffer_view);

  // Get a record batch reader over that
  auto maybe_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
  if (!maybe_reader.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", maybe_reader.status().message()));

  // Read the table
  std::shared_ptr<arrow::Table> table;
  status = (*maybe_reader)->ReadAll(&table);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", status.message()));

  return table;
}

std::vector<std::uint8_t> ArrowSerializer::table_to_bytes(const std::shared_ptr<arrow::Table>& table) {
  if (!table) {
    return {};
  }

  arrow::Status status;

  auto maybe_output_stream = arrow::io::BufferOutputStream::Create();
  if (!maybe_output_stream.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", maybe_output_stream.status().message()));

  auto maybe_writer = arrow::ipc::MakeStreamWriter((*maybe_output_stream).get(), table->schema());
  if (!maybe_writer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", maybe_writer.status().message()));

  status = (*maybe_writer)->WriteTable(*table);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  status = (*maybe_writer)->Close();
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  auto maybe_buffer = (*maybe_output_stream)->Finish();
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  auto data = (*maybe_buffer)->data();
  auto length = (*maybe_buffer)->size();

  std::vector<std::uint8_t> bytes_vec(data, data + length);

  return bytes_vec;
}

std::shared_ptr<arrow::Table> ArrowSerializer::align_table_by_copy(const std::shared_ptr<arrow::Table>& table) {
  if (!table) {
    return nullptr;
  }

  arrow::Status status;

  // table -> buffer
  auto maybe_output_stream = arrow::io::BufferOutputStream::Create();
  if (!maybe_output_stream.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", maybe_output_stream.status().message()));

  auto maybe_writer = arrow::ipc::MakeStreamWriter((*maybe_output_stream).get(), table->schema());
  if (!maybe_writer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", maybe_writer.status().message()));

  status = (*maybe_writer)->WriteTable(*table);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  status = (*maybe_writer)->Close();
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  auto maybe_buffer = (*maybe_output_stream)->Finish();
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  // buffer -> table
  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(*maybe_buffer);

  auto maybe_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
  if (!maybe_reader.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", maybe_reader.status().message()));

  std::shared_ptr<arrow::Table> new_table;
  status = (*maybe_reader)->ReadAll(&new_table);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", status.message()));

  return new_table;
}

std::shared_ptr<arrow::RecordBatch> ArrowSerializer::bytes_to_recordBatch(const std::vector<std::uint8_t>& bytes_vec) {
  if (bytes_vec.empty()) {
    return nullptr;
  }

  arrow::Status status;

  // Create a view over the given byte vector, but then get a copy because the vector ref eventually disappears
  auto buffer_view = ::arrow::Buffer::Wrap(bytes_vec);
  auto maybe_buffer = buffer_view->CopySlice(0, buffer_view->size(), arrow::default_memory_pool());
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow record batch  |  error: {}", maybe_buffer.status().message()));

  // Get a reader over the buffer
  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(*maybe_buffer);

  // Get a record batch reader over that
  auto maybe_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
  if (!maybe_reader.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow record batch  |  error: {}", maybe_reader.status().message()));

  // Read the table
  std::shared_ptr<arrow::RecordBatch> recordBatch;
  status = (*maybe_reader)->ReadNext(&recordBatch);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow record batch  |  error: {}", status.message()));

  return recordBatch;
}

std::vector<std::uint8_t> ArrowSerializer::recordBatch_to_bytes(const std::shared_ptr<arrow::RecordBatch>& recordBatch) {
  if (!recordBatch) {
    return {};
  }

  arrow::Status status;

  auto maybe_output_stream = arrow::io::BufferOutputStream::Create();
  if (!maybe_output_stream.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", maybe_output_stream.status().message()));

  auto maybe_writer = arrow::ipc::MakeStreamWriter((*maybe_output_stream).get(), recordBatch->schema());
  if (!maybe_writer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", maybe_writer.status().message()));

  status = (*maybe_writer)->WriteRecordBatch(*recordBatch);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", status.message()));

  status = (*maybe_writer)->Close();
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", status.message()));

  auto maybe_buffer = (*maybe_output_stream)->Finish();
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", status.message()));

  auto data = (*maybe_buffer)->data();
  auto length = (*maybe_buffer)->size();

  std::vector<std::uint8_t> bytes_vec(data, data + length);

  return bytes_vec;
}

std::shared_ptr<arrow::Schema> ArrowSerializer::bytes_to_schema(const std::vector<std::uint8_t>& bytes_vec) {
  if (bytes_vec.empty()) {
    return nullptr;
  }

  arrow::Status status;

  // Create a view over the given byte vector, but then get a copy because the vector ref eventually disappears
  auto buffer_view = ::arrow::Buffer::Wrap(bytes_vec);
  auto maybe_buffer = buffer_view->CopySlice(0, buffer_view->size(), arrow::default_memory_pool());
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow schema  |  error: {}", maybe_buffer.status().message()));

  // Get a reader over the buffer
  ::arrow::io::BufferReader buffer_reader(*maybe_buffer);
  ::arrow::ipc::DictionaryMemo dictionaryMemo;

  // Read the schema
  auto expSchema = ::arrow::ipc::ReadSchema(&buffer_reader, &dictionaryMemo);
  if (!expSchema.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow schema  |  error: {}", expSchema.status().message()));

  return *expSchema;
}

std::vector<std::uint8_t> ArrowSerializer::schema_to_bytes(const std::shared_ptr<arrow::Schema>& schema) {
  if (!schema) {
    return {};
  }

  auto expBuffer = ::arrow::ipc::SerializeSchema(*schema);
  if (!expBuffer.ok()) {
    throw std::runtime_error(fmt::format("Error converting Arrow schema to bytes  |  error: {}", expBuffer.status().message()));
  }
  const auto &buffer = *expBuffer;

  auto data = buffer->data();
  auto length = buffer->size();
  std::vector<std::uint8_t> bytes_vec(data, data + length);
  return bytes_vec;
}

std::shared_ptr<arrow::ChunkedArray> ArrowSerializer::bytes_to_chunkedArray(const std::vector<std::uint8_t> &bytes_vec) {
  if (bytes_vec.empty()) {
    return nullptr;
  }

  auto table = bytes_to_table(bytes_vec);
  return table->column(0);
}

std::vector<std::uint8_t> ArrowSerializer::chunkedArray_to_bytes(const std::shared_ptr<arrow::ChunkedArray> &chunkedArray) {
  if (!chunkedArray) {
    return {};
  }

  // prepare the Table, as arrow only supports RecordBatch/Table level serialization
  auto field = std::make_shared<arrow::Field>("", chunkedArray->type());
  auto fields = std::vector<std::shared_ptr<arrow::Field>>{field};
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto columns = std::vector<std::shared_ptr<arrow::ChunkedArray>>{chunkedArray};
  auto table = arrow::Table::Make(schema, columns);

  return table_to_bytes(table);
}

std::shared_ptr<arrow::Scalar> ArrowSerializer::bytes_to_scalar(const std::vector<std::uint8_t>& bytes_vec) {
  if (bytes_vec.empty()) {
    return nullptr;
  }

  auto recordBatch = bytes_to_recordBatch(bytes_vec);
  auto expScalar = recordBatch->column(0)->GetScalar(0);
  if (!expScalar.ok()) {
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow scalar  |  error: {}", expScalar.status().message()));
  }
  return expScalar.ValueOrDie();
}

std::vector<std::uint8_t> ArrowSerializer::scalar_to_bytes(const std::shared_ptr<arrow::Scalar>& scalar) {
  if (!scalar) {
    return {};
  }

  // prepare the RecordBatch
  auto field = std::make_shared<arrow::Field>("", scalar->type);
  auto fields = std::vector<std::shared_ptr<arrow::Field>>{field};
  auto schema = std::make_shared<arrow::Schema>(fields);

  std::unique_ptr<::arrow::ArrayBuilder> arrayBuilder;
  auto status = ::arrow::MakeBuilder(::arrow::default_memory_pool(), scalar->type, &arrayBuilder);
  if (!status.ok()) {
    throw std::runtime_error(fmt::format("Error converting Arrow scalar to bytes  |  error: {}", status.message()));
  }
  status = arrayBuilder->AppendScalar(*scalar);
  if (!status.ok()) {
    throw std::runtime_error(fmt::format("Error converting Arrow scalar to bytes  |  error: {}", status.message()));
  }
  auto expArray = arrayBuilder->Finish();
  if (!expArray.ok()) {
    throw std::runtime_error(fmt::format("Error converting Arrow scalar to bytes  |  error: {}", expArray.status().message()));
  }
  const auto &array = expArray.ValueOrDie();
  auto arrays = std::vector<std::shared_ptr<::arrow::Array>>{array};
  auto recordBatch = ::arrow::RecordBatch::Make(schema, 1, arrays);

  return recordBatch_to_bytes(recordBatch);
}

std::shared_ptr<arrow::DataType> ArrowSerializer::bytes_to_dataType(const std::vector<std::uint8_t>& bytes_vec) {
  if (bytes_vec.empty()) {
    return nullptr;
  }

  std::string typeName(bytes_vec.begin(), bytes_vec.end());
  if (typeName == "int32") {
    return arrow::int32();
  } else if (typeName == "int64") {
    return arrow::int64();
  } else if (typeName == "double") {
    return arrow::float64();
  } else if (typeName == "bool") {
    return arrow::boolean();
  } else if (typeName == "date64") {
    return arrow::date64();
  } else if (typeName == "utf8") {
    return arrow::utf8();
  } else {
    throw std::runtime_error(fmt::format("Error converting Arrow dataType to bytes  |  error: unsupported dataType, {}", typeName));
  }
}

std::vector<std::uint8_t> ArrowSerializer::dataType_to_bytes(const std::shared_ptr<arrow::DataType>& dataType) {
  if (!dataType) {
    return {};
  }

  auto typeName = dataType->name();
  return {typeName.begin(), typeName.end()};
}

tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
ArrowSerializer::bitmap_to_recordBatch(const std::vector<int64_t> &bitmap) {
  // make boolean array from bitmap
  auto arrayBuilder = std::make_shared<arrow::Int64Builder>();
  auto status = arrayBuilder->AppendValues(bitmap);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  auto expBitmapArray = arrayBuilder->Finish();
  if (!expBitmapArray.ok()) {
    return tl::make_unexpected(expBitmapArray.status().message());
  }
  auto bitmapArray = *expBitmapArray;

  // make record batch from array
  return arrow::RecordBatch::Make(::arrow::schema({{field(BITMAP_FIELD_NAME.data(), ::arrow::int64())}}),
                                  bitmapArray->length(),
                                  arrow::ArrayVector{bitmapArray});
}

tl::expected<std::vector<int64_t>, std::string>
ArrowSerializer::recordBatch_to_bitmap(const std::shared_ptr<arrow::RecordBatch> &recordBatch) {
  // get bitmap array
  auto bitmapArray = recordBatch->GetColumnByName(BITMAP_FIELD_NAME.data());
  if (bitmapArray == nullptr) {
    return tl::make_unexpected("Bitmap array not found in the recordBatch");
  }
  if (bitmapArray->type()->id() != arrow::int64()->id()) {
    return tl::make_unexpected("Type of bitmap array is not int64");
  }
  auto typedBitmapArray = std::static_pointer_cast<arrow::Int64Array>(bitmapArray);

  auto bitmapData = typedBitmapArray->data()->GetValues<int64_t>(1, typedBitmapArray->offset());
  return std::vector<int64_t>(bitmapData, bitmapData + typedBitmapArray->length());
}

}
