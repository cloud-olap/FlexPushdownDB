//
// Created by Yifei Yang on 2/14/22.
//

#include <fpdb/tuple/util/FileReaderTestUtil.h>
#include <doctest/doctest.h>

namespace fpdb::tuple::util {

std::shared_ptr<::arrow::Schema> FileReaderTestUtil::makeTestSchema() {
  return arrow::schema({
    ::arrow::field("a", ::arrow::int64()),
    ::arrow::field("b", ::arrow::int64()),
    ::arrow::field("c", ::arrow::int64())
  });
}

void FileReaderTestUtil::checkReadWholeAllTestCsv3x10000(const std::shared_ptr<TupleSet> &tupleSet) {
  int expNumRows = 10000;
  checkShape(tupleSet, 3, expNumRows);
  checkReadWholeSchemaAll(Schema::make(tupleSet->schema()));
  for (int r = 0; r < expNumRows; ++r) {
    switch(r % 3) {
      case 0: {
        checkReadWholeDataRow1(tupleSet, r);
        break;
      }
      case 1: {
        checkReadWholeDataRow2(tupleSet, r);
        break;
      }
      case 2: {
        checkReadWholeDataRow3(tupleSet, r);
        break;
      }
    }
  }
}

void FileReaderTestUtil::checkReadColumnsAllTestCsv(const std::shared_ptr<TupleSet> &tupleSet) {
  checkShape(tupleSet, 2, 3);
  checkReadColumnsSchemaAll(Schema::make(tupleSet->schema()));
  checkReadColumnsDataRow1(tupleSet, 0);
  checkReadColumnsDataRow2(tupleSet, 1);
  checkReadColumnsDataRow3(tupleSet, 2);
}

void FileReaderTestUtil::checkReadColumnsAllTestCsv3x10000(const std::shared_ptr<TupleSet> &tupleSet) {
  int expNumRows = 10000;
  checkShape(tupleSet, 2, expNumRows);
  checkReadColumnsSchemaAll(Schema::make(tupleSet->schema()));
  for (int r = 0; r < expNumRows; ++r) {
    switch(r % 3) {
      case 0: {
        checkReadColumnsDataRow1(tupleSet, r);
        break;
      }
      case 1: {
        checkReadColumnsDataRow2(tupleSet, r);
        break;
      }
      case 2: {
        checkReadColumnsDataRow3(tupleSet, r);
        break;
      }
    }
  }
}

void FileReaderTestUtil::checkShape(const std::shared_ptr<TupleSet> &tupleSet, int cols, int rows) {
  CHECK_EQ(tupleSet->numColumns(), cols);
  CHECK_EQ(tupleSet->numRows(), rows);
}

void FileReaderTestUtil::checkReadWholeSchemaAll(const std::shared_ptr<Schema> &schema) {
  CHECK_EQ(schema->fields().size(), 3);
  CHECK_EQ(schema->fields()[0]->name(), "a");
  CHECK_EQ(schema->fields()[1]->name(), "b");
  CHECK_EQ(schema->fields()[2]->name(), "c");
}

void FileReaderTestUtil::checkReadWholeDataRow1(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->value<::arrow::Int64Type>("a", pos);
  auto expValue2 = tupleSet->value<::arrow::Int64Type>("b", pos);
  auto expValue3 = tupleSet->value<::arrow::Int64Type>("c", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK(expValue3.has_value());
  CHECK_EQ(*expValue1, 1);
  CHECK_EQ(*expValue2, 2);
  CHECK_EQ(*expValue3, 3);
}

void FileReaderTestUtil::checkReadWholeDataRow2(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->value<::arrow::Int64Type>("a", pos);
  auto expValue2 = tupleSet->value<::arrow::Int64Type>("b", pos);
  auto expValue3 = tupleSet->value<::arrow::Int64Type>("c", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK(expValue3.has_value());
  CHECK_EQ(*expValue1, 4);
  CHECK_EQ(*expValue2, 5);
  CHECK_EQ(*expValue3, 6);
}

void FileReaderTestUtil::checkReadWholeDataRow3(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->value<::arrow::Int64Type>("a", pos);
  auto expValue2 = tupleSet->value<::arrow::Int64Type>("b", pos);
  auto expValue3 = tupleSet->value<::arrow::Int64Type>("c", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK(expValue3.has_value());
  CHECK_EQ(*expValue1, 7);
  CHECK_EQ(*expValue2, 8);
  CHECK_EQ(*expValue3, 9);
}

void FileReaderTestUtil::checkReadWholeAllTestCsv(const std::shared_ptr<TupleSet> &tupleSet) {
  checkShape(tupleSet, 3, 3);
  checkReadWholeSchemaAll(Schema::make(tupleSet->schema()));
  checkReadWholeDataRow1(tupleSet, 0);
  checkReadWholeDataRow2(tupleSet, 1);
  checkReadWholeDataRow3(tupleSet, 2);
}

void FileReaderTestUtil::checkReadColumnsSchemaAll(const std::shared_ptr<Schema> &schema) {
  CHECK_EQ(schema->fields().size(), 2);
  CHECK_EQ(schema->fields()[0]->name(), "a");
  CHECK_EQ(schema->fields()[1]->name(), "b");
}

void FileReaderTestUtil::checkReadColumnsDataRow1(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->value<::arrow::Int64Type>("a", pos);
  auto expValue2 = tupleSet->value<::arrow::Int64Type>("b", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK_EQ(*expValue1, 1);
  CHECK_EQ(*expValue2, 2);
}

void FileReaderTestUtil::checkReadColumnsDataRow2(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->value<::arrow::Int64Type>("a", pos);
  auto expValue2 = tupleSet->value<::arrow::Int64Type>("b", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK_EQ(*expValue1, 4);
  CHECK_EQ(*expValue2, 5);
}

void FileReaderTestUtil::checkReadColumnsDataRow3(const std::shared_ptr<TupleSet> &tupleSet, int pos) {
  auto expValue1 = tupleSet->value<::arrow::Int64Type>("a", pos);
  auto expValue2 = tupleSet->value<::arrow::Int64Type>("b", pos);
  CHECK(expValue1.has_value());
  CHECK(expValue2.has_value());
  CHECK_EQ(*expValue1, 7);
  CHECK_EQ(*expValue2, 8);
}

}
