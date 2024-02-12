//
// Created by matt on 20/5/20.
//

#include <doctest/doctest.h>

#include <fpdb/tuple/arrow/Arrays.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple;

#define SKIP_SUITE false

TEST_SUITE ("tuple" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("make" * doctest::skip(false || SKIP_SUITE)) {

  auto column1 = std::vector{"1", "2", "3"};
  auto column2 = std::vector{"4", "5", "6"};
  auto column3 = std::vector{"7", "8", "9"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto arrowColumn1 = Arrays::make<arrow::StringType>(column1).value();
  auto arrowColumn2 = Arrays::make<arrow::StringType>(column2).value();
  auto arrowColumn3 = Arrays::make<arrow::StringType>(column3).value();

  auto tuples = TupleSet::make(schema, {arrowColumn1, arrowColumn2, arrowColumn3});

  SPDLOG_DEBUG("Output:\n{}", tuples->toString());
}

TEST_CASE ("iterate-column" * doctest::skip(false || SKIP_SUITE)) {

  auto vectorA = std::vector{"1", "2", "3", "4", "5", "6"};

  auto arrowColumnA = Arrays::make<arrow::StringType>(vectorA).value();
  auto columnA = Column::make("a", arrowColumnA);

  SPDLOG_DEBUG("Input:\n{}", columnA->showString());

  // Check explicit iteration
  std::vector<std::string> explicitScalars;
  auto explicitIt = columnA->begin();
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
	  CHECK_EQ(explicitScalars[0], "1");
	  CHECK_EQ(explicitScalars[1], "2");
	  CHECK_EQ(explicitScalars[2], "3");
	  CHECK_EQ(explicitScalars[3], "4");
	  CHECK_EQ(explicitScalars[4], "5");
	  CHECK_EQ(explicitScalars[5], "6");
	  CHECK_EQ(explicitIt, columnA->end());

  // Check classic iteration
  std::vector<std::string> classicScalars;
  for (auto classicIt = columnA->begin(); classicIt != columnA->end(); ++classicIt) {
	auto scalar = *classicIt;
	classicScalars.push_back(*(*scalar)->value<std::string>());
  }
	  CHECK_EQ(classicScalars[0], "1");
	  CHECK_EQ(classicScalars[1], "2");
	  CHECK_EQ(classicScalars[2], "3");
	  CHECK_EQ(classicScalars[3], "4");
	  CHECK_EQ(classicScalars[4], "5");
	  CHECK_EQ(classicScalars[5], "6");

  // Check enhanced for loop
  std::vector<std::string> enhancedScalars;
  for (const auto scalar: *columnA) {
	enhancedScalars.push_back(*(*scalar)->value<std::string>());
  }
	  CHECK_EQ(enhancedScalars[0], "1");
	  CHECK_EQ(enhancedScalars[1], "2");
	  CHECK_EQ(enhancedScalars[2], "3");
	  CHECK_EQ(enhancedScalars[3], "4");
	  CHECK_EQ(enhancedScalars[4], "5");
	  CHECK_EQ(enhancedScalars[5], "6");
}

TEST_CASE ("iterate-column-chunked" * doctest::skip(false || SKIP_SUITE)) {

  auto vectorA1 = std::vector{"1", "2", "3"};
  auto vectorA2 = std::vector{"4", "5", "6"};

  auto arrowColumnA1 = Arrays::make<arrow::StringType>(vectorA1).value();
  auto arrowColumnA2 = Arrays::make<arrow::StringType>(vectorA2).value();
  auto arrowChunkedColumn = std::make_shared<::arrow::ChunkedArray>(::arrow::ArrayVector{arrowColumnA1, arrowColumnA2});
  auto columnA = Column::make("a", arrowChunkedColumn);

  SPDLOG_DEBUG("Input:\n{}", columnA->showString());

  // Check explicit iteration
  std::vector<std::string> explicitScalars;
  auto explicitIt = columnA->begin();
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
  explicitScalars.push_back(*(**explicitIt)->value<std::string>());
  ++explicitIt;
	  CHECK_EQ(explicitScalars[0], "1");
	  CHECK_EQ(explicitScalars[1], "2");
	  CHECK_EQ(explicitScalars[2], "3");
	  CHECK_EQ(explicitScalars[3], "4");
	  CHECK_EQ(explicitScalars[4], "5");
	  CHECK_EQ(explicitScalars[5], "6");
	  CHECK_EQ(explicitIt, columnA->end());

  // Check classic iteration
  std::vector<std::string> classicScalars;
  for (auto classicIt = columnA->begin(); classicIt != columnA->end(); ++classicIt) {
	auto scalar = *classicIt;
	classicScalars.push_back(*(*scalar)->value<std::string>());
  }
	  CHECK_EQ(classicScalars[0], "1");
	  CHECK_EQ(classicScalars[1], "2");
	  CHECK_EQ(classicScalars[2], "3");
	  CHECK_EQ(classicScalars[3], "4");
	  CHECK_EQ(classicScalars[4], "5");
	  CHECK_EQ(classicScalars[5], "6");

  // Check enhanced for loop
  std::vector<std::string> enhancedScalars;
  for (const auto scalar: *columnA) {
	enhancedScalars.push_back(*(*scalar)->value<std::string>());
  }
	  CHECK_EQ(enhancedScalars[0], "1");
	  CHECK_EQ(enhancedScalars[1], "2");
	  CHECK_EQ(enhancedScalars[2], "3");
	  CHECK_EQ(enhancedScalars[3], "4");
	  CHECK_EQ(enhancedScalars[4], "5");
	  CHECK_EQ(enhancedScalars[5], "6");
}

TEST_CASE ("iterate-column-empty" * doctest::skip(false || SKIP_SUITE)) {

  auto vectorA = std::vector<std::string>{};
  auto arrowColumnA = Arrays::make<arrow::StringType>(vectorA).value();
  auto columnA = Column::make("a", arrowColumnA);

  SPDLOG_DEBUG("Input:\n{}", columnA->showString());

  // Check explicit iteration
  std::vector<std::string> explicitScalars;
  auto explicitIt = columnA->begin();
	  CHECK_FALSE((*explicitIt).has_value());
	  CHECK_EQ(explicitIt, columnA->end());
	  CHECK_EQ(explicitScalars.size(), 0);

  // Check classic iteration
  std::vector<std::string> classicScalars;
  for (auto classicIt = columnA->begin(); classicIt != columnA->end(); ++classicIt) {
	auto scalar = *classicIt;
	classicScalars.push_back(*(*scalar)->value<std::string>());
  }
	  CHECK_EQ(classicScalars.size(), 0);

  // Check enhanced for loop
  std::vector<std::string> enhancedScalars;
  for (const auto scalar: *columnA) {
	enhancedScalars.push_back(*(*scalar)->value<std::string>());
  }
	  CHECK_EQ(enhancedScalars.size(), 0);

}

TEST_CASE ("iterate-column-empty-chunk" * doctest::skip(false || SKIP_SUITE)) {

  auto vectorA = std::vector<std::string>{};

  auto arrowColumnA = Arrays::make<arrow::StringType>(vectorA).value();
  auto arrowChunkedColumn = std::make_shared<::arrow::ChunkedArray>(::arrow::ArrayVector{arrowColumnA});
  auto columnA = Column::make("a", arrowChunkedColumn);

  SPDLOG_DEBUG("Input:\n{}", columnA->showString());

  // Check explicit iteration
  std::vector<std::string> explicitScalars;
  auto explicitIt = columnA->begin();
          CHECK_FALSE((*explicitIt).has_value());
	  CHECK_EQ(explicitIt, columnA->end());
	  CHECK_EQ(explicitScalars.size(), 0);

  // Check classic iteration
  std::vector<std::string> classicScalars;
  for (auto classicIt = columnA->begin(); classicIt != columnA->end(); ++classicIt) {
	auto scalar = *classicIt;
	classicScalars.push_back(*(*scalar)->value<std::string>());
  }
	  CHECK_EQ(classicScalars.size(), 0);

  // Check enhanced for loop
  std::vector<std::string> enhancedScalars;
  for (const auto scalar: *columnA) {
	enhancedScalars.push_back(*(*scalar)->value<std::string>());
  }
	  CHECK_EQ(enhancedScalars.size(), 0);
}

}