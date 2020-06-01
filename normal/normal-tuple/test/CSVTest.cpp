//
// Created by matt on 26/5/20.
//

#include <memory>

#include <doctest/doctest.h>
#include <normal/tuple/csv/CSVParser.h>

using namespace normal::tuple::csv;
using namespace normal::tuple;

#define SKIP_SUITE false


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
TEST_SUITE ("csv" * doctest::skip(SKIP_SUITE)) {

std::shared_ptr<Schema> parseSchema(int64_t bufferSize) {
  CSVParser parser("data/csv/test.csv", bufferSize);

  auto maybeSchema = parser.parseSchema();
	  CHECK_MESSAGE(maybeSchema.has_value(), maybeSchema.error());

  auto schema = maybeSchema.value();
  SPDLOG_DEBUG("Output:\n{}", schema->showString());

  return schema;
}

std::shared_ptr<TupleSet2> parseData(int64_t bufferSize,
									 int64_t startOffset = 0,
									 std::optional<int64_t> finishOffset = std::nullopt) {

  CSVParser parser("data/csv/test.csv", std::nullopt, startOffset, finishOffset, bufferSize);

  auto maybeTupleSet = parser.parse();
	  CHECK_MESSAGE(maybeTupleSet.has_value(), maybeTupleSet.error());

  auto tupleSet = maybeTupleSet.value();
  SPDLOG_DEBUG("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  return tupleSet;
}

void checkSchemaAll(std::shared_ptr<Schema> &&schema) {
	  CHECK_EQ(schema->fields().size(), 3);
	  CHECK_EQ(schema->fields()[0]->name(), "a");
	  CHECK_EQ(schema->fields()[1]->name(), "b");
	  CHECK_EQ(schema->fields()[2]->name(), "c");
}

void checkDataRow1(std::shared_ptr<TupleSet2> &tupleSet, int pos) {
	  CHECK_EQ(tupleSet->getString("a", pos), "1");
	  CHECK_EQ(tupleSet->getString("b", pos), "2");
	  CHECK_EQ(tupleSet->getString("c", pos), "3");
}

void checkDataRow2(std::shared_ptr<TupleSet2> &tupleSet, int pos) {
	  CHECK_EQ(tupleSet->getString("a", pos), "4");
	  CHECK_EQ(tupleSet->getString("b", pos), "5");
	  CHECK_EQ(tupleSet->getString("c", pos), "6");
}

void checkDataRow3(std::shared_ptr<TupleSet2> &tupleSet, int pos) {
	  CHECK_EQ(tupleSet->getString("a", pos), "7");
	  CHECK_EQ(tupleSet->getString("b", pos), "8");
	  CHECK_EQ(tupleSet->getString("c", pos), "9");
}

void checkShape(std::shared_ptr<TupleSet2> &tupleSet, int cols, int rows) {
	  CHECK_EQ(tupleSet->numColumns(), cols);
	  CHECK_EQ(tupleSet->numRows(), rows);
}

void checkAll(std::shared_ptr<TupleSet2> &&tupleSet) {
  checkSchemaAll(tupleSet->schema().value());
  checkShape(tupleSet, 3, 3);
  checkDataRow1(tupleSet, 0);
  checkDataRow2(tupleSet, 1);
  checkDataRow3(tupleSet, 2);
}

TEST_CASE ("parse-schema-small-buffer" * doctest::skip(false || SKIP_SUITE)) {
  checkSchemaAll(parseSchema(2));
}

TEST_CASE ("read-schema-large-buffer" * doctest::skip(false || SKIP_SUITE)) {
  checkSchemaAll(parseSchema(16 * 1024));
}

TEST_CASE ("read-byte-range-row1-aligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSet = parseData(2, 0, 5);
  checkShape(tupleSet, 3, 1);
  checkDataRow1(tupleSet, 0);
}

TEST_CASE ("read-byte-range-row2-aligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSet = parseData(2, 6, 11);
  checkShape(tupleSet, 3, 1);
  checkDataRow2(tupleSet, 0);
}

TEST_CASE ("read-byte-range-row2-unaligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSet = parseData(2, 4, 10);
  checkShape(tupleSet, 3, 1);
  checkDataRow2(tupleSet, 0);
}

TEST_CASE ("parse-data-all-small-buffer" * doctest::skip(false || SKIP_SUITE)) {
  checkAll(parseData(2));
}

TEST_CASE ("parse-data-all-large-buffer" * doctest::skip(false || SKIP_SUITE)) {
  checkAll(parseData(16 * 1024));
}

}
