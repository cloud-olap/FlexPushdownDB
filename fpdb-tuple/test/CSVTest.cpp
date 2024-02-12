//
// Created by matt on 26/5/20.
//

#include <memory>

#include <doctest/doctest.h>
#include <fpdb/tuple/csv/CSVParser.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/LocalFileReaderBuilder.h>
#include <fpdb/tuple/util/FileReaderTestUtil.h>

using namespace fpdb::tuple::csv;
using namespace fpdb::tuple::util;
using namespace fpdb::tuple;

#define SKIP_SUITE false

TEST_SUITE ("csvParser" * doctest::skip(SKIP_SUITE)) {

/**
 * Test Data:
 *
 * Row 1
 * - Including header - bytes 6-10 inclusive, bytes 6-12 end-exclusive
 * - Excluding header - bytes 0-4 inclusive, bytes 0-6 end-exclusive
 *
 * Row 2
 * - Including header - bytes 12-16 inclusive, bytes 12-18 end-exclusive
 * - Excluding header - bytes 6-10 inclusive, bytes 6-12 end-exclusive
 *
 */

void checkSchemaAll(const std::shared_ptr<Schema> &schema) {
  CHECK_EQ(schema->fields().size(), 3);
  CHECK_EQ(schema->fields()[0]->name(), "a");
  CHECK_EQ(schema->fields()[1]->name(), "b");
  CHECK_EQ(schema->fields()[2]->name(), "c");
}

void checkDataRow1(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->stringValue("a", pos);
  auto expValue2 = tupleSet->stringValue("b", pos);
  auto expValue3 = tupleSet->stringValue("c", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK(expValue3.has_value());
  CHECK_EQ(*expValue1, "1");
  CHECK_EQ(*expValue2, "2");
  CHECK_EQ(*expValue3, "3");
}

void checkDataRow2(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->stringValue("a", pos);
  auto expValue2 = tupleSet->stringValue("b", pos);
  auto expValue3 = tupleSet->stringValue("c", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK(expValue3.has_value());
  CHECK_EQ(*expValue1, "4");
  CHECK_EQ(*expValue2, "5");
  CHECK_EQ(*expValue3, "6");
}

void checkDataRow3(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->stringValue("a", pos);
  auto expValue2 = tupleSet->stringValue("b", pos);
  auto expValue3 = tupleSet->stringValue("c", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK(expValue3.has_value());
  CHECK_EQ(*expValue1, "7");
  CHECK_EQ(*expValue2, "8");
  CHECK_EQ(*expValue3, "9");
}

void checkShape(const std::shared_ptr<TupleSet> &tupleSet, int cols, int rows) {
  CHECK_EQ(tupleSet->numColumns(), cols);
  CHECK_EQ(tupleSet->numRows(), rows);
}

void checkAll(const std::shared_ptr<TupleSet> &tupleSet) {
  checkSchemaAll(Schema::make(tupleSet->schema()));
  checkShape(tupleSet, 3, 3);
  checkDataRow1(tupleSet, 0);
  checkDataRow2(tupleSet, 1);
  checkDataRow3(tupleSet, 2);
}

std::shared_ptr<TupleSet> parseData(int64_t bufferSize,
                                    int64_t startOffset = 0,
                                    std::optional<int64_t> finishOffset = std::nullopt) {
  auto expInFile = ::arrow::io::ReadableFile::Open("data/csv/test.csv");
  auto schema = FileReaderTestUtil::makeTestSchema();
  CSVParser parser(*expInFile, schema, std::nullopt, startOffset, finishOffset, bufferSize);

  auto maybeTupleSet = parser.parse();
	  CHECK_MESSAGE(maybeTupleSet.has_value(), maybeTupleSet.error());

  auto tupleSet = maybeTupleSet.value();
  SPDLOG_DEBUG("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  return tupleSet;
}

TEST_CASE ("csvParser-read-byte-range-row1-aligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSet = parseData(2, 0, 5);
  checkShape(tupleSet, 3, 1);
  checkDataRow1(tupleSet, 0);
}

TEST_CASE ("csvParser-read-byte-range-row2-aligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSet = parseData(2, 6, 11);
  checkShape(tupleSet, 3, 1);
  checkDataRow2(tupleSet, 0);
}

TEST_CASE ("csvParser-read-byte-range-row2-unaligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSet = parseData(2, 4, 10);
  checkShape(tupleSet, 3, 1);
  checkDataRow2(tupleSet, 0);
}

TEST_CASE ("csvParser-parse-data-all-small-buffer" * doctest::skip(false || SKIP_SUITE)) {
  checkAll(parseData(2));
}

TEST_CASE ("csvParser-parse-data-all-large-buffer" * doctest::skip(false || SKIP_SUITE)) {
  checkAll(parseData(16 * 1024));
}

}

TEST_SUITE ("csvReader" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("csvReader-read-whole-test.csv" * doctest::skip(false || SKIP_SUITE)) {
  auto filePath = "data/csv/test.csv";
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = LocalFileReaderBuilder::make(csvFormat, schema, filePath);

  auto expTupleSet = reader->read();
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadWholeAllTestCsv(*expTupleSet);
}

TEST_CASE ("csvReader-read-columns-test.csv" * doctest::skip(false || SKIP_SUITE)) {
  auto filePath = "data/csv/test.csv";
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = LocalFileReaderBuilder::make(csvFormat, schema, filePath);

  auto expTupleSet = reader->read({"a", "b"});
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadColumnsAllTestCsv(*expTupleSet);
}

TEST_CASE ("csvReader-read-whole-test3x10000.csv" * doctest::skip(false || SKIP_SUITE)) {
  auto filePath = "data/csv/test3x10000.csv";
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = LocalFileReaderBuilder::make(csvFormat, schema, filePath);

  auto expTupleSet = reader->read();
  CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadWholeAllTestCsv3x10000(*expTupleSet);
}

TEST_CASE ("csvReader-read-columns-test3x10000.csv" * doctest::skip(false || SKIP_SUITE)) {
  auto filePath = "data/csv/test3x10000.csv";
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = LocalFileReaderBuilder::make(csvFormat, schema, filePath);

  auto expTupleSet = reader->read({"a", "b"});
  CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadColumnsAllTestCsv3x10000(*expTupleSet);
}

TEST_CASE ("csvReader-file-size-test.csv" * doctest::skip(false || SKIP_SUITE)) {
  auto filePath = "data/csv/test.csv";
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = LocalFileReaderBuilder::make(csvFormat, schema, filePath);

  auto expFileSize = reader->getFileSize();
  CHECK(expFileSize.has_value());
  CHECK_EQ(*expFileSize, 24);
}

TEST_CASE ("csvReader-file-size-test3x10000.csv" * doctest::skip(false || SKIP_SUITE)) {
  auto filePath = "data/csv/test3x10000.csv";
  auto csvFormat = std::make_shared<CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = LocalFileReaderBuilder::make(csvFormat, schema, filePath);

  auto expFileSize = reader->getFileSize();
  CHECK(expFileSize.has_value());
  CHECK_EQ(*expFileSize, 60006);
}

}
