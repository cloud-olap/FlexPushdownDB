//
// Created by matt on 20/7/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/core/OperatorManager.h>
#include <normal/pushdown/merge/MergeKernel.h>

using namespace normal::tuple;
using namespace normal::pushdown;
using namespace normal::pushdown::merge;

#define SKIP_SUITE true

std::shared_ptr<TupleSet2> makeTupleSet_3x3_1() {
  auto stringType = arrow::utf8();

  auto fieldA = field("1a", stringType);
  auto fieldB = field("1b", stringType);
  auto fieldC = field("1c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto columnA = std::vector{"1", "2", "3"};
  auto columnB = std::vector{"4", "5", "6"};
  auto columnC = std::vector{"7", "8", "9"};

  auto arrowColumnA = Arrays::make<arrow::StringType>(columnA).value();
  auto arrowColumnB = Arrays::make<arrow::StringType>(columnB).value();
  auto arrowColumnC = Arrays::make<arrow::StringType>(columnC).value();

  auto tupleSet = TupleSet2::make(schema, {arrowColumnA, arrowColumnB, arrowColumnC});
  return tupleSet;
}

std::shared_ptr<TupleSet2> makeTupleSet_3x3_2() {
  auto stringType = arrow::utf8();

  auto fieldA = field("2a", stringType);
  auto fieldB = field("2b", stringType);
  auto fieldC = field("2c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto columnA = std::vector{"10", "11", "12"};
  auto columnB = std::vector{"13", "14", "15"};
  auto columnC = std::vector{"16", "17", "18"};

  auto arrowColumnA = Arrays::make<arrow::StringType>(columnA).value();
  auto arrowColumnB = Arrays::make<arrow::StringType>(columnB).value();
  auto arrowColumnC = Arrays::make<arrow::StringType>(columnC).value();

  auto tupleSet = TupleSet2::make(schema, {arrowColumnA, arrowColumnB, arrowColumnC});
  return tupleSet;
}

std::shared_ptr<TupleSet2> makeTupleSet_3x2_1() {
  auto stringType = arrow::utf8();

  auto fieldA = field("3a", stringType);
  auto fieldB = field("3b", stringType);
  auto fieldC = field("3c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto columnA = std::vector{"19", "20"};
  auto columnB = std::vector{"21", "22"};
  auto columnC = std::vector{"23", "24"};

  auto arrowColumnA = Arrays::make<arrow::StringType>(columnA).value();
  auto arrowColumnB = Arrays::make<arrow::StringType>(columnB).value();
  auto arrowColumnC = Arrays::make<arrow::StringType>(columnC).value();

  auto tupleSet = TupleSet2::make(schema, {arrowColumnA, arrowColumnB, arrowColumnC});
  return tupleSet;
}

std::shared_ptr<TupleSet2> makeEmptyTupleSet() {
  auto tupleSet = TupleSet2::make();
  return tupleSet;
}

TEST_SUITE ("merge" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("merge-3x3-with-3x3" * doctest::skip(false || SKIP_SUITE)) {

  std::shared_ptr<TupleSet2> tupleSet1 = makeTupleSet_3x3_1();
  std::shared_ptr<TupleSet2> tupleSet2 = makeTupleSet_3x3_2();

  SPDLOG_DEBUG("Input:\nTupleSet1:\n{}\nTupleSet2:\n{}",
			   tupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)),
			   tupleSet2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expectedMergedTupleSet = MergeKernel::merge(tupleSet1, tupleSet2);
	  CHECK_MESSAGE(expectedMergedTupleSet.has_value(), expectedMergedTupleSet.error());

  if (expectedMergedTupleSet.has_value()) {
	auto mergedTupleSet = expectedMergedTupleSet.value();

	SPDLOG_DEBUG("Output:\n{}",
				 mergedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	auto column1A = mergedTupleSet->getColumnByName("1A").value();
		CHECK(column1A->element(0).value()->value<std::string>() == "1");
		CHECK(column1A->element(1).value()->value<std::string>() == "2");
		CHECK(column1A->element(2).value()->value<std::string>() == "3");

	auto column1B = mergedTupleSet->getColumnByName("1B").value();
		CHECK(column1B->element(0).value()->value<std::string>() == "4");
		CHECK(column1B->element(1).value()->value<std::string>() == "5");
		CHECK(column1B->element(2).value()->value<std::string>() == "6");

	auto column1C = mergedTupleSet->getColumnByName("1C").value();
		CHECK(column1C->element(0).value()->value<std::string>() == "7");
		CHECK(column1C->element(1).value()->value<std::string>() == "8");
		CHECK(column1C->element(2).value()->value<std::string>() == "9");

	auto column2A = mergedTupleSet->getColumnByName("2A").value();
		CHECK(column2A->element(0).value()->value<std::string>() == "10");
		CHECK(column2A->element(1).value()->value<std::string>() == "11");
		CHECK(column2A->element(2).value()->value<std::string>() == "12");

	auto column2B = mergedTupleSet->getColumnByName("2B").value();
		CHECK(column2B->element(0).value()->value<std::string>() == "13");
		CHECK(column2B->element(1).value()->value<std::string>() == "14");
		CHECK(column2B->element(2).value()->value<std::string>() == "15");

	auto column2C = mergedTupleSet->getColumnByName("2C").value();
		CHECK(column2C->element(0).value()->value<std::string>() == "16");
		CHECK(column2C->element(1).value()->value<std::string>() == "17");
		CHECK(column2C->element(2).value()->value<std::string>() == "18");
  }
}

TEST_CASE ("merge-3x3-with-0x0" * doctest::skip(false || SKIP_SUITE)) {

  std::shared_ptr<TupleSet2> tupleSet1 = makeTupleSet_3x3_1();
  std::shared_ptr<TupleSet2> tupleSet2 = makeEmptyTupleSet();

  SPDLOG_DEBUG("Input:\nTupleSet1:\n{}\nTupleSet2:\n{}",
			   tupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)),
			   tupleSet2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expectedMergedTupleSet = MergeKernel::merge(tupleSet1, tupleSet2);
	  CHECK_MESSAGE(expectedMergedTupleSet.has_value(), expectedMergedTupleSet.error());

  if (expectedMergedTupleSet.has_value()) {
	auto mergedTupleSet = expectedMergedTupleSet.value();

	SPDLOG_DEBUG("Output:\n{}",
				 mergedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	auto column1A = mergedTupleSet->getColumnByName("1A").value();
		CHECK(column1A->element(0).value()->value<std::string>() == "1");
		CHECK(column1A->element(1).value()->value<std::string>() == "2");
		CHECK(column1A->element(2).value()->value<std::string>() == "3");

	auto column1B = mergedTupleSet->getColumnByName("1B").value();
		CHECK(column1B->element(0).value()->value<std::string>() == "4");
		CHECK(column1B->element(1).value()->value<std::string>() == "5");
		CHECK(column1B->element(2).value()->value<std::string>() == "6");

	auto column1C = mergedTupleSet->getColumnByName("1C").value();
		CHECK(column1C->element(0).value()->value<std::string>() == "7");
		CHECK(column1C->element(1).value()->value<std::string>() == "8");
		CHECK(column1C->element(2).value()->value<std::string>() == "9");
  }
}

TEST_CASE ("merge-0x0" * doctest::skip(false || SKIP_SUITE)) {

  std::shared_ptr<TupleSet2> tupleSet1 = makeEmptyTupleSet();
  std::shared_ptr<TupleSet2> tupleSet2 = makeEmptyTupleSet();

  SPDLOG_DEBUG("Input:\nTupleSet1:\n{}\nTupleSet2:\n{}",
			   tupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)),
			   tupleSet2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expectedMergedTupleSet = MergeKernel::merge(tupleSet1, tupleSet2);
	  CHECK_MESSAGE(expectedMergedTupleSet.has_value(), expectedMergedTupleSet.error());

  if (expectedMergedTupleSet.has_value()) {
	auto mergedTupleSet = expectedMergedTupleSet.value();

	SPDLOG_DEBUG("Output:\n{}",
				 mergedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

		CHECK(mergedTupleSet->numRows() == 0);
		CHECK(mergedTupleSet->numColumns() == 0);
  }
}

TEST_CASE ("merge-fail-non-equal-num-rows" * doctest::skip(false || SKIP_SUITE)) {

  std::shared_ptr<TupleSet2> tupleSet1 = makeTupleSet_3x3_1();
  std::shared_ptr<TupleSet2> tupleSet2 = makeTupleSet_3x2_1();

  SPDLOG_DEBUG("Input:\nTupleSet1:\n{}\nTupleSet2:\n{}",
			   tupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)),
			   tupleSet2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expectedMergedTupleSet = MergeKernel::merge(tupleSet1, tupleSet2);

  SPDLOG_DEBUG("Output: {}", expectedMergedTupleSet.error());

	  CHECK(!expectedMergedTupleSet.has_value());
}

TEST_CASE ("merge-fail-duplicate-columns" * doctest::skip(false || SKIP_SUITE)) {

  std::shared_ptr<TupleSet2> tupleSet1 = makeTupleSet_3x3_1();
  std::shared_ptr<TupleSet2> tupleSet2 = makeTupleSet_3x3_1();

  SPDLOG_DEBUG("Input:\nTupleSet1:\n{}\nTupleSet2:\n{}",
			   tupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)),
			   tupleSet2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expectedMergedTupleSet = MergeKernel::merge(tupleSet1, tupleSet2);

  SPDLOG_DEBUG("Output: {}", expectedMergedTupleSet.error());

	  CHECK(!expectedMergedTupleSet.has_value());
}

}