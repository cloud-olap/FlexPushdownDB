//
// Created by matt on 8/5/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_TEST_TESTUTIL_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_TEST_TESTUTIL_H

#include <memory>

#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple;

namespace fpdb::expression::gandiva::test {

class TestUtil {

public:

/**
 * Create a simple 3 col, 3 row tupleset for testing expressions against
 *
 * @return
 */
  static std::shared_ptr<TupleSet> prepareTupleSet() {

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

	return tuples;
  }

  static std::shared_ptr<TupleSet> prepareEmptyTupleSet() {
	auto schema = arrow::schema({});
	auto tuples = TupleSet::make(schema, std::vector<std::shared_ptr<arrow::Array>>{});
	return tuples;
  }

  static std::shared_ptr<TupleSet> prepare3x0TupleSet() {
	auto column1 = std::vector<std::string>{};
	auto column2 = std::vector<std::string>{};
	auto column3 = std::vector<std::string>{};

	auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

	auto fieldA = field("a", stringType);
	auto fieldB = field("b", stringType);
	auto fieldC = field("c", stringType);
	auto schema = arrow::schema({fieldA, fieldB, fieldC});

	auto arrowColumn1 = Arrays::make<arrow::StringType>(column1).value();
	auto arrowColumn2 = Arrays::make<arrow::StringType>(column2).value();
	auto arrowColumn3 = Arrays::make<arrow::StringType>(column3).value();

	auto tuples = TupleSet::make(schema, {arrowColumn1, arrowColumn2, arrowColumn3});

	return tuples;
  }
};

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_TEST_TESTUTIL_H
