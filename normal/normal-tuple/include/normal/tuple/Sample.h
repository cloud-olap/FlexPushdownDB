//
// Created by matt on 22/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H

#include <memory>
#include <random>

#include "TupleSet2.h"

namespace normal::tuple {

/**
 * Pre built sample tuple sets, useful for testing
 */
class Sample {

public:

  /**
   * 3 x 3 tuple set of strings
   *
   * @return
   */
  static std::shared_ptr<TupleSet2> sample3x3String();

  static std::shared_ptr<Column> sample3String();

  /**
   * Creates a  numCols x numRows tuple set of random decimal strings between 0.0 and 100.0
   *
   * Each column is named "c_<column index>"
   *
   * @param numCols
   * @param numRows
   * @return
   */
  static std::shared_ptr<TupleSet2> sampleCxRString(int numCols, int numRows);
  static std::shared_ptr<TupleSet2> sampleCxRRealString(int numCols, int numRows, std::uniform_real_distribution<double> dis = std::uniform_real_distribution(0.0, 9.0));
  static std::shared_ptr<TupleSet2> sampleCxRIntString(int numCols, int numRows, std::uniform_int_distribution<int> dis = std::uniform_int_distribution(0, 9));
  static std::shared_ptr<TupleSet2> sampleCxRString(int numCols, int numRows, const std::function<std::string()> &valueGenerator);

  template <typename CType, typename ArrowType>
  static std::shared_ptr<TupleSet2> sampleCxRInt(int numCols, int numRows, std::uniform_int_distribution<int> dist = std::uniform_int_distribution(0, 9)) {
	std::random_device rd;
	std::mt19937 gen(rd());
	return sampleCxR<CType, ArrowType>(numCols, numRows, [&]() -> auto { return dist(gen); });
  }

  template <typename CType, typename ArrowType>
  static std::shared_ptr<TupleSet2> sampleCxRReal(int numCols, int numRows, std::uniform_real_distribution<double> dist = std::uniform_real_distribution(0.0, 9.0)) {
	std::random_device rd;
	std::mt19937 gen(rd());
	return sampleCxR<CType, ArrowType>(numCols, numRows, [&]() -> auto { return dist(gen); });
  }

  template <typename CType, typename ArrowType>
  static std::shared_ptr<TupleSet2> sampleCxR(int numCols, int numRows, const std::function<CType()> &valueGenerator) {

	std::vector<std::vector<CType>> data;
	for (int c = 0; c < numCols; ++c) {
	  std::vector<CType> row;
	  row.reserve(numRows);
	  for (int r = 0; r < numRows; ++r) {
		row.emplace_back(valueGenerator());
	  }
	  data.emplace_back(row);
	}

	std::vector<std::shared_ptr<::arrow::Field>> fields;
	fields.reserve(numCols);
	for (int c = 0; c < numCols; ++c) {
	  fields.emplace_back(field(fmt::format("c_{}", c), ::arrow::TypeTraits<ArrowType>::type_singleton()));
	}

	auto arrowSchema = arrow::schema(fields);
	auto schema = Schema::make(arrowSchema);

	std::vector<std::shared_ptr<::arrow::Array>> arrays;
	arrays.reserve(numCols);
	for (int c = 0; c < numCols; ++c) {
	  arrays.emplace_back(Arrays::make<ArrowType>(data[c]).value());
	}

	std::vector<std::shared_ptr<normal::tuple::Column>> columns;
	columns.reserve(numCols);
	for (int c = 0; c < numCols; ++c) {
	  columns.emplace_back(normal::tuple::Column::make(fields[c]->name(), arrays[c]));
	}

	auto tupleSet = TupleSet2::make(schema, columns);

	return tupleSet;
  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H
