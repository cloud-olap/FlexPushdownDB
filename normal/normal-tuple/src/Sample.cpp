//
// Created by matt on 22/5/20.
//

#include "normal/tuple/Sample.h"

using namespace normal::tuple;

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

std::shared_ptr<TupleSet2> Sample::sample3x3String() {

  auto vec1 = std::vector{"1", "2", "3"};
  auto vec2 = std::vector{"4", "5", "6"};
  auto vec3 = std::vector{"7", "8", "9"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto arrowSchema = arrow::schema({fieldA, fieldB, fieldC});
  auto schema = Schema::make(arrowSchema);

  auto arrowColumn1 = Arrays::make<arrow::StringType>(vec1).value();
  auto arrowColumn2 = Arrays::make<arrow::StringType>(vec2).value();
  auto arrowColumn3 = Arrays::make<arrow::StringType>(vec3).value();

  auto column1 = Column::make(fieldA->name(), arrowColumn1);
  auto column2 = Column::make(fieldB->name(), arrowColumn2);
  auto column3 = Column::make(fieldC->name(), arrowColumn3);

  auto tupleSet = TupleSet2::make(schema, {column1, column2, column3});

  return tupleSet;
}
