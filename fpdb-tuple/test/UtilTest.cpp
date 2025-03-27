//
// Created by Yifei Yang on 3/8/24.
//

#include <memory>

#include <doctest/doctest.h>
#include <fpdb/tuple/util/Util.h>
#include <fpdb/tuple/util/Sample.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple::util;
using namespace fpdb::tuple;

#define SKIP_SUITE false

TEST_SUITE ("array-copy" * doctest::skip(SKIP_SUITE)) {

std::shared_ptr<TupleSet> copyTupleSet(const std::shared_ptr<TupleSet> &tupleSet, int numCopies, bool copyWhole) {
  assert(tupleSet->table()->column(0)->num_chunks() == 1);
  arrow::ArrayVector out;
  for (const auto &column: tupleSet->table()->columns()) {
    auto expOutArray = copyWhole ?
            Util::copyArrayWhole(column->chunk(0), numCopies) :
            Util::copyArrayRow(column->chunk(0), numCopies);
    if (!expOutArray.has_value()) {
      throw std::runtime_error(expOutArray.error());
    }
    out.emplace_back(*expOutArray);
  }
  return TupleSet::make(tupleSet->schema(), out);
}

void runCopy(const std::shared_ptr<TupleSet> &tupleSet, int numCopies, bool copyWhole) {
  printf("[Original]:\n%s\n", tupleSet->showString(
          TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 100)).c_str());
  printf("[Copied]:\n%s\n", copyTupleSet(tupleSet, numCopies, copyWhole)->showString(
          TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 100)).c_str());
}

TEST_CASE ("array-copy-whole-int" * doctest::skip(false || SKIP_SUITE)) {
  runCopy(Sample::sampleCxRInt<long, arrow::Int64Type>(3, 10, std::uniform_int_distribution(0, 100)), 5, true);
}

TEST_CASE ("array-copy-whole-double" * doctest::skip(false || SKIP_SUITE)) {
  runCopy(Sample::sampleCxRReal<double, arrow::DoubleType>(3, 10, std::uniform_real_distribution(0.0, 100.0)), 5, true);
}

TEST_CASE ("array-copy-whole-string" * doctest::skip(false || SKIP_SUITE)) {
  runCopy(Sample::sampleCxRString(3, 10), 5, true);
}

TEST_CASE ("array-copy-row-int" * doctest::skip(false || SKIP_SUITE)) {
  runCopy(Sample::sampleCxRInt<long, arrow::Int64Type>(3, 10, std::uniform_int_distribution(0, 100)), 5, false);
}

TEST_CASE ("array-copy-row-double" * doctest::skip(false || SKIP_SUITE)) {
  runCopy(Sample::sampleCxRReal<double, arrow::DoubleType>(3, 10, std::uniform_real_distribution(0.0, 100.0)), 5, false);
}

TEST_CASE ("array-copy-row-string" * doctest::skip(false || SKIP_SUITE)) {
  runCopy(Sample::sampleCxRString(3, 10), 5, false);
}

}