//
// Created by matt on 8/10/20.
//

#include <doctest/doctest.h>

#include <fpdb/tuple/TupleSetIndex.h>
#include <fpdb/tuple/arrow/Arrays.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple;

#define SKIP_SUITE false

auto makeEmptyTupleSetA() {
  auto schemaA = ::arrow::schema({::arrow::field("aa", ::arrow::int64()),
                                  ::arrow::field("ab", ::arrow::int64()),
                                  ::arrow::field("ac", ::arrow::int64())});

  auto arrayAA1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAA2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAA = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAA1, arrayAA2});
  auto arrayAB1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAB2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAB = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAB1, arrayAB2});
  auto arrayAC1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAC2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAC = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAC1, arrayAC2});
  auto tableA = arrow::Table::Make(schemaA, {arrayAA, arrayAB, arrayAC});
  auto tupleSetA = TupleSet::make(tableA);
  return tupleSetA;
}

auto makeTupleSetA() {
  auto schemaA = ::arrow::schema({::arrow::field("aa", ::arrow::int64()),
                                  ::arrow::field("ab", ::arrow::int64()),
                                  ::arrow::field("ac", ::arrow::int64())});

  auto arrayAA1 = Arrays::make<arrow::Int64Type>({1, 2}).value();
  auto arrayAA2 = Arrays::make<arrow::Int64Type>({3}).value();
  auto arrayAA = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAA1, arrayAA2});
  auto arrayAB1 = Arrays::make<arrow::Int64Type>({4, 5}).value();
  auto arrayAB2 = Arrays::make<arrow::Int64Type>({6}).value();
  auto arrayAB = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAB1, arrayAB2});
  auto arrayAC1 = Arrays::make<arrow::Int64Type>({7, 8}).value();
  auto arrayAC2 = Arrays::make<arrow::Int64Type>({9}).value();
  auto arrayAC = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAC1, arrayAC2});
  auto tableA = arrow::Table::Make(schemaA, {arrayAA, arrayAB, arrayAC});
  auto tupleSetA = TupleSet::make(tableA);
  return tupleSetA;
}

TEST_SUITE ("tupleset-index-test" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tupleset-index-test-make" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSetA = makeTupleSetA();
  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA);
	  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex = expectedTupleSetIndex1.value();
	  REQUIRE(tupleSetIndex->size() == tupleSetA->numRows());
}

TEST_CASE ("tupleset-index-test-make-non-existent-column" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSetA = makeTupleSetA();
  auto expectedTupleSetIndex1 = TupleSetIndex::make({"NON_EXISTENT_COLUMN_NAME"}, tupleSetA);
	  REQUIRE_FALSE(expectedTupleSetIndex1);
}

TEST_CASE ("tupleset-index-test-make-empty" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSetA = makeEmptyTupleSetA();
  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA);
	  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex = expectedTupleSetIndex1.value();
	  REQUIRE(tupleSetIndex->size() == tupleSetA->numRows());
}

TEST_CASE ("tupleset-index-test-put" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA1 = makeTupleSetA();
  auto tupleSetA2 = makeTupleSetA();
  auto totalNumRows = tupleSetA1->numRows() + tupleSetA2->numRows();

  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA1);
	  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex1 = expectedTupleSetIndex1.value();

  auto putResult1 = tupleSetIndex1->put(tupleSetA2->table());
	  REQUIRE(putResult1);
	  REQUIRE(tupleSetIndex1->size() == totalNumRows);
}

TEST_CASE ("tupleset-index-test-put-non-existent-column" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA1 = makeTupleSetA();
  auto tupleSetA2 = makeTupleSetA();

  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA1);
	  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex1 = expectedTupleSetIndex1.value();

  // Give column aa a bad name
  auto renameResult = tupleSetA2->renameColumns({"NON_EXISTENT_COLUMN_NAME", "ab", "ac"});
	  REQUIRE(renameResult);

  auto putResult1 = tupleSetIndex1->put(tupleSetA2->table());
	  REQUIRE_FALSE(putResult1);
}

TEST_CASE ("tupleset-index-test-put-out-of-order-column" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA1 = makeTupleSetA();
  auto tupleSetA2 = makeTupleSetA();
  auto totalNumRows = tupleSetA1->numRows() + tupleSetA2->numRows();

  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA1);
	  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex1 = expectedTupleSetIndex1.value();

  // Move column aa to a different position
  auto renameResult = tupleSetA2->renameColumns({"ab", "aa", "ac"});
	  REQUIRE(renameResult);

  auto putResult1 = tupleSetIndex1->put(tupleSetA2->table());
	  REQUIRE(putResult1);
          REQUIRE(tupleSetIndex1->size() == totalNumRows);
}

TEST_CASE ("tupleset-index-test-merge" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA1 = makeTupleSetA();
  auto tupleSetA2 = makeTupleSetA();
  auto totalNumRows = tupleSetA1->numRows() + tupleSetA2->numRows();

  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA1);
	  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex1 = expectedTupleSetIndex1.value();

  auto expectedTupleSetIndex2 = TupleSetIndex::make({"aa"}, tupleSetA2);
	  REQUIRE(expectedTupleSetIndex2);
  auto tupleSetIndex2 = expectedTupleSetIndex2.value();

  auto mergeResult1 = tupleSetIndex1->merge(tupleSetIndex2);
	  REQUIRE(mergeResult1);
	  REQUIRE(tupleSetIndex1->size() == totalNumRows);

}

TEST_CASE ("tupleset-index-test-merge-non-existent-column" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA1 = makeTupleSetA();
  auto tupleSetA2 = makeTupleSetA();

  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA1);
	  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex1 = expectedTupleSetIndex1.value();

  // Give column aa a bad name
  auto renameResult = tupleSetA2->renameColumns({"aa", "NON_EXISTENT_COLUMN_NAME", "ac"});
	  REQUIRE(renameResult);

  auto expectedTupleSetIndex2 = TupleSetIndex::make({"aa"}, tupleSetA2);
	  REQUIRE(expectedTupleSetIndex2);
  auto tupleSetIndex2 = expectedTupleSetIndex2.value();

  auto mergeResult1 = tupleSetIndex1->merge(tupleSetIndex2);
	  REQUIRE_FALSE(mergeResult1);
}

TEST_CASE ("tupleset-index-test-merge-out-of-order-column" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA1 = makeTupleSetA();
  auto tupleSetA2 = makeTupleSetA();
  auto totalNumRows = tupleSetA1->numRows() + tupleSetA2->numRows();

  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA1);
	  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex1 = expectedTupleSetIndex1.value();

  // Move column aa to a different position
  auto renameResult = tupleSetA2->renameColumns({"ab", "aa", "ac"});
	  REQUIRE(renameResult);

  auto expectedTupleSetIndex2 = TupleSetIndex::make({"aa"}, tupleSetA2);
	  REQUIRE(expectedTupleSetIndex2);
  auto tupleSetIndex2 = expectedTupleSetIndex2.value();

  auto mergeResult1 = tupleSetIndex1->merge(tupleSetIndex2);
          REQUIRE(mergeResult1);
          REQUIRE(tupleSetIndex1->size() == totalNumRows);
}

TEST_CASE ("tupleset-index-test-merge-different-hash-column" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA1 = makeTupleSetA();
  auto tupleSetA2 = makeTupleSetA();

  auto expectedTupleSetIndex1 = TupleSetIndex::make({"aa"}, tupleSetA1);
  REQUIRE(expectedTupleSetIndex1);
  auto tupleSetIndex1 = expectedTupleSetIndex1.value();

  // Move column aa to a different position
  auto renameResult = tupleSetA2->renameColumns({"aa", "ab", "ac"});
  REQUIRE(renameResult);

  auto expectedTupleSetIndex2 = TupleSetIndex::make({"ab"}, tupleSetA2);
  REQUIRE(expectedTupleSetIndex2);
  auto tupleSetIndex2 = expectedTupleSetIndex2.value();

  auto mergeResult1 = tupleSetIndex1->merge(tupleSetIndex2);
  REQUIRE_FALSE(mergeResult1);
}

}

