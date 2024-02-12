//
// Created by matt on 22/5/20.
//

#include <random>
#include <fpdb/tuple/util/Sample.h>

namespace fpdb::tuple::util {

std::shared_ptr<Column> Sample::sample3String() {

  auto vec1 = std::vector{"1", "2", "3"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto arrowSchema = arrow::schema({fieldA});
  auto schema = Schema::make(arrowSchema);

  auto arrowColumn1 = Arrays::make<arrow::StringType>(vec1).value();

  auto column1 = Column::make(fieldA->name(), arrowColumn1);

  return column1;
}

[[maybe_unused]] std::shared_ptr<TupleSet> Sample::sample3x3String() {

  auto vec1 = std::vector{"1", "2", "3"};
  auto vec2 = std::vector{"4", "5", "6"};
  auto vec3 = std::vector{"7", "8", "9"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto arrowSchema = arrow::schema({fieldA, fieldB, fieldC});

  auto arrowColumn1 = Arrays::make<arrow::StringType>(vec1).value();
  auto arrowColumn2 = Arrays::make<arrow::StringType>(vec2).value();
  auto arrowColumn3 = Arrays::make<arrow::StringType>(vec3).value();

  auto column1 = Column::make(fieldA->name(), arrowColumn1);
  auto column2 = Column::make(fieldB->name(), arrowColumn2);
  auto column3 = Column::make(fieldC->name(), arrowColumn3);

  auto tupleSet = TupleSet::make(arrowSchema, {column1, column2, column3});

  return tupleSet;
}

std::shared_ptr<TupleSet> Sample::sampleCxRString(int numCols, int numRows) {
  return sampleCxRRealString(numCols, numRows, std::uniform_real_distribution(0.0, 100.0));
}

std::shared_ptr<TupleSet> Sample::sampleCxRRealString(int numCols, int numRows, std::uniform_real_distribution<double> dist) {
  std::random_device rd;
  std::mt19937 gen(rd());
  return sampleCxRString(numCols, numRows, [&]() -> auto { return fmt::format("{:.{}f}", dist(gen), 2); });
}

std::shared_ptr<TupleSet> Sample::sampleCxRIntString(int numCols, int numRows, std::uniform_int_distribution<int> dist) {
  std::random_device rd;
  std::mt19937 gen(rd());
  return sampleCxRString(numCols, numRows, [&]() -> auto { return fmt::format("{}", dist(gen)); });
}

std::shared_ptr<TupleSet> Sample::sampleCxRString(int numCols, int numRows, const std::function<std::string()> &valueGenerator) {

  std::vector<std::vector<std::string>> data;
  for (int c = 0; c < numCols; ++c) {
	std::vector<std::string> row;
	row.reserve(numRows);
	for (int r = 0; r < numRows; ++r) {
	  row.emplace_back(valueGenerator());
	}
	data.emplace_back(row);
  }

  std::vector<std::shared_ptr<::arrow::Field>> fields;
  fields.reserve(numCols);
  for (int c = 0; c < numCols; ++c) {
	fields.emplace_back(field(fmt::format("c_{}", c), ::arrow::utf8()));
  }

  auto arrowSchema = arrow::schema(fields);

  std::vector<std::shared_ptr<::arrow::Array>> arrays;
  arrays.reserve(numCols);
  for (int c = 0; c < numCols; ++c) {
	arrays.emplace_back(Arrays::make<arrow::StringType>(data[c]).value());
  }

  std::vector<std::shared_ptr<fpdb::tuple::Column>> columns;
  columns.reserve(numCols);
  for (int c = 0; c < numCols; ++c) {
	columns.emplace_back(fpdb::tuple::Column::make(fields[c]->name(), arrays[c]));
  }

  auto tupleSet = TupleSet::make(arrowSchema, columns);

  return tupleSet;
}

}
