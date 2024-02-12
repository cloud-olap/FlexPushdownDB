//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/csv/LocalCSVReader.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/util/Util.h>
#include <arrow/io/api.h>
#include <filesystem>
#include <iostream>

using namespace fpdb::tuple;

namespace fpdb::tuple::csv {

LocalCSVReader::LocalCSVReader(const std::shared_ptr<FileFormat> &format,
                               const std::shared_ptr<::arrow::Schema> &schema,
                               const std::string &path) :
  FileReader(format, schema),
  LocalFileReader(path) {}

std::shared_ptr<LocalCSVReader> LocalCSVReader::make(const std::shared_ptr<FileFormat> &format,
                                                     const std::shared_ptr<::arrow::Schema> &schema,
                                                     const std::string &path) {
  return std::make_shared<LocalCSVReader>(format, schema, path);
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
LocalCSVReader::readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) {
  // open the file as input stream
  auto expInFile = ::arrow::io::ReadableFile::Open(path_);
  if (!expInFile.ok()) {
    return tl::make_unexpected(expInFile.status().message());
  }
  auto inFile = *expInFile;

  // read
  auto expTupleSet = CSVReader::readRangeImpl(columnNames, startPos, finishPos, inFile);
  auto fileSize = fpdb::util::getFileSize(path_);
  bytesReadLocal_ += (fileSize >= finishPos) ? (finishPos - startPos) : (fileSize - startPos);

  // close
  close(inFile);
  return expTupleSet;
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
LocalCSVReader::readUsingSimdParser(const std::vector<std::string> &columnNames) {
  // check if file exists
  if (!std::filesystem::exists(std::filesystem::path(path_))) {
    return tl::make_unexpected(fmt::format("File {} not exist.", path_));
  }

  // create the input stream
  std::ifstream inputStream(path_, std::ifstream::binary);

  // read
#ifdef __AVX2__
  auto expTupleSet = CSVReader::readUsingSimdParserImpl(columnNames, inputStream);
#else
  auto expTupleSet = TupleSet::makeWithEmptyTable();
#endif
  bytesReadLocal_ += fpdb::util::getFileSize(path_);

  // close
  inputStream.close();
  return expTupleSet;
}


tl::expected<std::shared_ptr<TupleSet>, std::string>
LocalCSVReader::readUsingArrowApi(const std::vector<std::string> &columnNames) {
  // open the file as input stream
  auto expInFile = ::arrow::io::ReadableFile::Open(path_);
  if (!expInFile.ok()) {
    return tl::make_unexpected(expInFile.status().message());
  }
  auto inFile = *expInFile;

  // read
  auto expTupleSet = CSVReader::readUsingArrowApiImpl(columnNames, inFile);
  bytesReadLocal_ += inFile->GetBytesRead();

  // close
  close(inFile);
  return expTupleSet;
}

}
