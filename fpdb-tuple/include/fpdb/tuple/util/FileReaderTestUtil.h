//
// Created by Yifei Yang on 2/14/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_FILEREADERTESTUTIL_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_FILEREADERTESTUTIL_H

#include <fpdb/tuple/TupleSet.h>

namespace fpdb::tuple::util {

class FileReaderTestUtil {

public:
  static std::shared_ptr<::arrow::Schema> makeTestSchema();

  static void checkReadWholeAllTestCsv(const std::shared_ptr<TupleSet> &tupleSet);
  static void checkReadWholeAllTestCsv3x10000(const std::shared_ptr<TupleSet> &tupleSet);
  static void checkReadColumnsAllTestCsv(const std::shared_ptr<TupleSet> &tupleSet);
  static void checkReadColumnsAllTestCsv3x10000(const std::shared_ptr<TupleSet> &tupleSet);

private:
  static void checkShape(const std::shared_ptr<TupleSet> &tupleSet, int cols, int rows);

  static void checkReadWholeSchemaAll(const std::shared_ptr<Schema> &schema);
  static void checkReadWholeDataRow1(const std::shared_ptr<TupleSet> &tupleSet, int pos);
  static void checkReadWholeDataRow2(const std::shared_ptr<TupleSet> &tupleSet, int pos);
  static void checkReadWholeDataRow3(const std::shared_ptr<TupleSet> &tupleSet, int pos);

  static void checkReadColumnsSchemaAll(const std::shared_ptr<Schema> &schema);
  static void checkReadColumnsDataRow1(const std::shared_ptr<TupleSet> &tupleSet, int pos);
  static void checkReadColumnsDataRow2(const std::shared_ptr<TupleSet> &tupleSet, int pos);
  static void checkReadColumnsDataRow3(const std::shared_ptr<TupleSet> &tupleSet, int pos);

};

}

#endif // FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_FILEREADERTESTUTIL_H
