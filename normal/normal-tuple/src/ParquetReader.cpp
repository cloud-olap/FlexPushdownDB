//
// Created by matt on 12/8/20.
//

#include "normal/tuple/ParquetReader.h"

using namespace normal::tuple;

ParquetReader::ParquetReader(std::string Path) : path_(std::move(Path)) {}

ParquetReader::~ParquetReader() {
  close();
}

tl::expected<std::shared_ptr<ParquetReader>, std::string> ParquetReader::make(const std::string &Path) {
  auto reader = std::make_shared<ParquetReader>(Path);
  auto result = reader->open();
  if(!result) {
	return tl::make_unexpected(result.error());
  }
  return reader;
}

tl::expected<void, std::string> ParquetReader::close() {
  if(inputStream_ && !inputStream_->closed()) {
	auto result = inputStream_->Close();
	if (!result.ok())
	  return tl::make_unexpected(result.message());
  }
  return {};
}

tl::expected<std::shared_ptr<TupleSet2>, std::string> ParquetReader::readRange(int startPos, int finishPos) {

  ::arrow::Status status;

  std::vector<int> rowGroupIndexes;
  for (int rowGroupIndex = 0; rowGroupIndex < metadata_->num_row_groups(); ++rowGroupIndex) {
	auto rowGroupMetaData = metadata_->RowGroup(rowGroupIndex);
	auto columnChunkMetaData = rowGroupMetaData->ColumnChunk(0);
	auto offset = columnChunkMetaData->file_offset();

	if (offset >= startPos && offset <= finishPos) {
	  rowGroupIndexes.emplace_back(rowGroupIndex);
	}
  }

  std::unique_ptr<::arrow::RecordBatchReader> recordBatchReader;
  status = arrowReader_->GetRecordBatchReader(rowGroupIndexes, &recordBatchReader);
  if (!status.ok()) {
	close();
	return tl::make_unexpected(status.message());
  }

  std::shared_ptr<::arrow::Table> table;
  status = recordBatchReader->ReadAll(&table);
  if (!status.ok()) {
	close();
	return tl::make_unexpected(status.message());
  }

  auto tupleSet = TupleSet2::make(table);
  return tupleSet;
}

tl::expected<void, std::string> ParquetReader::open() {
  ::arrow::Status status;

  auto absolutePath = std::filesystem::absolute(path_);

  auto expectedInputStream = ::arrow::io::ReadableFile::Open(absolutePath);
  if (!expectedInputStream.ok())
	return tl::make_unexpected(expectedInputStream.status().message());
  inputStream_ = *expectedInputStream;

  status = ::parquet::arrow::OpenFile(inputStream_, ::arrow::default_memory_pool(), &arrowReader_);
  if (!status.ok()) {
	close();
	return tl::make_unexpected(status.message());
  }

  metadata_ = arrowReader_->parquet_reader()->metadata();

  return {};
}


