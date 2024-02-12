//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/parquet/LocalParquetReader.h>
#include <fpdb/util/Util.h>
#include <arrow/io/api.h>

using namespace fpdb::tuple;

namespace fpdb::tuple::parquet {

LocalParquetReader::LocalParquetReader(const std::shared_ptr<FileFormat> &format,
                                       const std::shared_ptr<::arrow::Schema> &schema,
                                       const std::string &path) :
  FileReader(format, schema),
  LocalFileReader(path) {}

std::shared_ptr<LocalParquetReader> LocalParquetReader::make(const std::shared_ptr<FileFormat> &format,
                                                             const std::shared_ptr<::arrow::Schema> &schema,
                                                             const std::string &path) {
  return std::make_shared<LocalParquetReader>(format, schema, path);
}

tl::expected<std::shared_ptr<TupleSet>, std::string> LocalParquetReader::read(const std::vector<std::string> &columnNames) {
  // open the file
  auto expInFile = ::arrow::io::ReadableFile::Open(path_);
  if (!expInFile.ok()) {
    return tl::make_unexpected(expInFile.status().message());
  }
  auto inFile = *expInFile;

  // read
  auto expTupleSet = ParquetReader::readImpl(columnNames, inFile);
  bytesReadLocal_ += inFile->GetBytesRead();

  // close
  close(inFile);
  return expTupleSet;
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
LocalParquetReader::readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) {
  // open the file
  auto expectedInputStream = ::arrow::io::ReadableFile::Open(path_);
  if (!expectedInputStream.ok()) {
    return tl::make_unexpected(expectedInputStream.status().message());
  }
  auto inputStream = *expectedInputStream;

  // read
  auto expTupleSet = ParquetReader::readRangeImpl(columnNames, startPos, finishPos, inputStream);
  bytesReadLocal_ += inputStream->GetBytesRead();

  // close
  close(inputStream);
  return expTupleSet;
}

}
