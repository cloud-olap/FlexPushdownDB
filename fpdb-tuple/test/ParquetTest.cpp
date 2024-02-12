//
// Created by matt on 11/8/20.
//

#include <fpdb/tuple/Converter.h>
#include <fpdb/tuple/Globals.h>
#include <fpdb/tuple/LocalFileReaderBuilder.h>
#include <fpdb/tuple/parquet/LocalParquetReader.h>
#include <fpdb/tuple/parquet/ParquetFormat.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/util/FileReaderTestUtil.h>
#include <fpdb/util/Util.h>
#include <filesystem>
#include <doctest/doctest.h>
#include <memory>

using namespace fpdb::tuple;
using namespace fpdb::tuple::util;
using namespace fpdb::tuple::csv;
using namespace fpdb::tuple::parquet;
using namespace fpdb::util;

const char *getCurrentTestName();
const char *getCurrentTestSuiteName();

#define SKIP_SUITE false

TEST_SUITE ("converter" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("converter-csv-to-parquet" * doctest::skip(false || SKIP_SUITE)) {

  const std::string inFile = "data/csv/test.csv";
  const std::string outFile = fmt::format("tests/{}/{}/test.snappy.parquet", getCurrentTestSuiteName(), getCurrentTestName());
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();

  auto result = Converter::csvToParquet(inFile,
                                        outFile,
                                        csvFormat,
                                        schema,
                                        DefaultChunkSize,
                                        ::parquet::Compression::type::SNAPPY);
  CHECK_MESSAGE(result.has_value(), result.error());
}

}

TEST_SUITE ("parquetReader" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("parquetReader-read-whole-test.csv-converted" * doctest::skip(false || SKIP_SUITE)) {
  auto csvFilePath = "data/csv/test.csv";
  auto parquetFilePath = fmt::format("tests/{}/{}/test.parquet",
                                     getCurrentTestSuiteName(),
                                     getCurrentTestName());
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto parquetFormat = std::make_shared<ParquetFormat>();
  auto schema = FileReaderTestUtil::makeTestSchema();

  auto result = Converter::csvToParquet(csvFilePath,
                                        parquetFilePath,
                                        csvFormat,
                                        schema,
                                        DefaultChunkSize,
                                        ::parquet::Compression::type::UNCOMPRESSED);
  CHECK_MESSAGE(result.has_value(), result.error());

  auto reader = LocalFileReaderBuilder::make(parquetFormat, schema, parquetFilePath);
  auto expTupleSet = reader->read();
  CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadWholeAllTestCsv(*expTupleSet);
}

TEST_CASE ("parquetReader-read-columns-test.csv-converted" * doctest::skip(false || SKIP_SUITE)) {
  auto csvFilePath = "data/csv/test.csv";
  auto parquetFilePath = fmt::format("tests/{}/{}/test.parquet",
                                     getCurrentTestSuiteName(),
                                     getCurrentTestName());
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto parquetFormat = std::make_shared<ParquetFormat>();
  auto schema = FileReaderTestUtil::makeTestSchema();

  auto result = Converter::csvToParquet(csvFilePath,
                                        parquetFilePath,
                                        csvFormat,
                                        schema,
                                        DefaultChunkSize,
                                        ::parquet::Compression::type::UNCOMPRESSED);
  CHECK_MESSAGE(result.has_value(), result.error());

  auto reader = LocalFileReaderBuilder::make(parquetFormat, schema, parquetFilePath);
  auto expTupleSet = reader->read({"a", "b"});
  CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadColumnsAllTestCsv(*expTupleSet);
}

TEST_CASE ("parquetReader-read-whole-test3x10000.csv-converted" * doctest::skip(false || SKIP_SUITE)) {
  auto csvFilePath = "data/csv/test3x10000.csv";
  auto parquetFilePath = fmt::format("tests/{}/{}/test.parquet",
                                     getCurrentTestSuiteName(),
                                     getCurrentTestName());
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto parquetFormat = std::make_shared<ParquetFormat>();
  auto schema = FileReaderTestUtil::makeTestSchema();

  auto result = Converter::csvToParquet(csvFilePath,
                                        parquetFilePath,
                                        csvFormat,
                                        schema,
                                        DefaultChunkSize,
                                        ::parquet::Compression::type::UNCOMPRESSED);
  CHECK_MESSAGE(result.has_value(), result.error());

  auto reader = LocalFileReaderBuilder::make(parquetFormat, schema, parquetFilePath);
  auto expTupleSet = reader->read();
  CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadWholeAllTestCsv3x10000(*expTupleSet);
}

TEST_CASE ("parquetReader-read-columns-test3x10000.csv-converted" * doctest::skip(false || SKIP_SUITE)) {
  auto csvFilePath = "data/csv/test3x10000.csv";
  auto parquetFilePath = fmt::format("tests/{}/{}/test.parquet",
                                     getCurrentTestSuiteName(),
                                     getCurrentTestName());
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto parquetFormat = std::make_shared<ParquetFormat>();
  auto schema = FileReaderTestUtil::makeTestSchema();

  auto result = Converter::csvToParquet(csvFilePath,
                                        parquetFilePath,
                                        csvFormat,
                                        schema,
                                        DefaultChunkSize,
                                        ::parquet::Compression::type::UNCOMPRESSED);
  CHECK_MESSAGE(result.has_value(), result.error());

  auto reader = LocalFileReaderBuilder::make(parquetFormat, schema, parquetFilePath);
  auto expTupleSet = reader->read({"a", "b"});
  CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadColumnsAllTestCsv3x10000(*expTupleSet);
}

TEST_CASE ("parquetReader-read-byte-range" * doctest::skip(false || SKIP_SUITE)) {

  const std::string inFile = "data/csv/test3x10000.csv";
  const std::string outFile = fmt::format("tests/{}/{}/test3x10000.snappy.parquet",
                                          getCurrentTestSuiteName(),
                                          getCurrentTestName());
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();

  auto result = Converter::csvToParquet(inFile,
                                        outFile,
                                        csvFormat,
                                        schema,
                                        300,
                                        ::parquet::Compression::type::SNAPPY);
	  CHECK_MESSAGE(result.has_value(), result.error());

  auto size = std::filesystem::file_size(outFile);
  auto scanRanges = fpdb::util::ranges<int>(0, size, 3);

  auto reader = fpdb::tuple::parquet::LocalParquetReader::make(std::make_shared<fpdb::tuple::parquet::ParquetFormat>(),
                                                               nullptr,
                                                               outFile);

  for (const auto &scanRange: scanRanges) {
    auto expectedTupleSet = reader->readRange({"A","B","C"},
                                              scanRange.first,
                                              scanRange.second);
    if (!expectedTupleSet)
              FAIL (expectedTupleSet.error());
    auto tupleSet = expectedTupleSet.value();
    SPDLOG_DEBUG("Output:\n{}",
                 tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  }
}

}
