//
// Created by matt on 26/5/20.
//

#include <memory>

#include <doctest/doctest.h>
#include <normal/test/TestUtil.h>
#include <normal/tuple/csv/CSVParser.h>

using namespace normal::tuple::csv;
using namespace normal::tuple;
using namespace normal::test;

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
TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("read-byte-range-row1-aligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {

  CSVParser parser("data/csv/test.csv", 0, 6);

  auto maybeTupleSet = parser.parse();
	  CHECK_MESSAGE(maybeTupleSet.has_value(), maybeTupleSet.error());

  auto tupleSet = maybeTupleSet.value();
  SPDLOG_DEBUG("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK_EQ(tupleSet->numColumns(), 3);
	  CHECK_EQ(tupleSet->numRows(), 1);
	  CHECK_EQ(tupleSet->getString("a", 0), "1");
	  CHECK_EQ(tupleSet->getString("b", 0), "2");
	  CHECK_EQ(tupleSet->getString("c", 0), "3");

}

TEST_CASE ("read-byte-range-row2-aligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {

  CSVParser parser("data/csv/test.csv", 6, 11);

  auto maybeTupleSet = parser.parse();
	  CHECK_MESSAGE(maybeTupleSet.has_value(), maybeTupleSet.error());

  auto tupleSet = maybeTupleSet.value();
  SPDLOG_DEBUG("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK_EQ(tupleSet->numColumns(), 3);
	  CHECK_EQ(tupleSet->numRows(), 1);
	  CHECK_EQ(tupleSet->getString("a", 0), "4");
	  CHECK_EQ(tupleSet->getString("b", 0), "5");
	  CHECK_EQ(tupleSet->getString("c", 0), "6");

}

TEST_CASE ("read-byte-range-row2-unaligned-boundaries" * doctest::skip(false || SKIP_SUITE)) {

  CSVParser parser("data/csv/test.csv", 4, 10);

  auto maybeTupleSet = parser.parse();
	  CHECK_MESSAGE(maybeTupleSet.has_value(), maybeTupleSet.error());

  auto tupleSet = maybeTupleSet.value();
  SPDLOG_DEBUG("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK_EQ(tupleSet->numColumns(), 3);
	  CHECK_EQ(tupleSet->numRows(), 1);
	  CHECK_EQ(tupleSet->getString("a", 0), "4");
	  CHECK_EQ(tupleSet->getString("b", 0), "5");
	  CHECK_EQ(tupleSet->getString("c", 0), "6");

}

}
