//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/core/OperatorManager.h>
#include <normal/pushdown/shuffle/ATTIC/Shuffler.h>

using namespace normal::tuple;
using namespace normal::pushdown;
using namespace normal::pushdown::shuffle;

#define SKIP_SUITE true

TEST_SUITE ("shuffle" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("shuffle" * doctest::skip(false || SKIP_SUITE)) {

  auto stringType = arrow::utf8();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto columnA = std::vector{"1", "2", "3"};
  auto columnB = std::vector{"4", "5", "6"};
  auto columnC = std::vector{"7", "8", "9"};

  auto arrowColumnA = Arrays::make<arrow::StringType>(columnA).value();
  auto arrowColumnB = Arrays::make<arrow::StringType>(columnB).value();
  auto arrowColumnC = Arrays::make<arrow::StringType>(columnC).value();

  auto tupleSet = TupleSet2::make(schema, {arrowColumnA, arrowColumnB, arrowColumnC});

  SPDLOG_DEBUG("Input:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expectedShuffledTupleSets = Shuffler::shuffle("a", 2, tupleSet);
	  CHECK_MESSAGE(expectedShuffledTupleSets.has_value(), expectedShuffledTupleSets.error());

  if (expectedShuffledTupleSets.has_value()) {
	auto shuffledTupleSets = expectedShuffledTupleSets.value();

	size_t partitionIndex = 0;
	for (const auto &shuffledTupleSet: shuffledTupleSets) {
	  SPDLOG_DEBUG("Output: partitionIndex: {}, \n{}", partitionIndex,
				   shuffledTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
	  ++partitionIndex;
	}
  }
}

TEST_CASE ("shuffle-empty" * doctest::skip(false || SKIP_SUITE)) {

  auto stringType = arrow::utf8();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto columnA = std::vector<std::string>{};
  auto columnB = std::vector<std::string>{};
  auto columnC = std::vector<std::string>{};

  auto arrowColumnA = Arrays::make<arrow::StringType>(columnA).value();
  auto arrowColumnB = Arrays::make<arrow::StringType>(columnB).value();
  auto arrowColumnC = Arrays::make<arrow::StringType>(columnC).value();

  auto tupleSet = TupleSet2::make(schema, {arrowColumnA, arrowColumnB, arrowColumnC});

  SPDLOG_DEBUG("Input:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expectedShuffledTupleSets = Shuffler::shuffle("a", 2, tupleSet);
	  CHECK_MESSAGE(expectedShuffledTupleSets.has_value(), expectedShuffledTupleSets.error());

  if (expectedShuffledTupleSets.has_value()) {
	auto shuffledTupleSets = expectedShuffledTupleSets.value();
	size_t partitionIndex = 0;
	for (const auto &shuffledTupleSet: shuffledTupleSets) {
	  SPDLOG_DEBUG("Output: partitionIndex: {}, \n{}", partitionIndex,
				   shuffledTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
	  ++partitionIndex;
	}
  }
}

}