//
// Created by Yifei Yang on 5/6/21.
//

#include <memory>
#include <doctest/doctest.h>
#include <nanobench.h>
#include <spdlog/spdlog.h>

#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <normal/tuple/TupleSet2.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/expression/gandiva/Filter.h>

#define SKIP_SUITE false

using namespace normal::tuple;
using namespace normal::core::type;
using namespace normal::expression::gandiva;

std::shared_ptr<arrow::Schema> lineOrderSchema() {
  auto fields = {::arrow::field(ColumnName::canonicalize("LO_ORDERKEY"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_LINENUMBER"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_CUSTKEY"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_PARTKEY"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_SUPPKEY"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_ORDERDATE"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_ORDERPRIORITY"), ::arrow::utf8()),
                 ::arrow::field(ColumnName::canonicalize("LO_SHIPPRIORITY"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_QUANTITY"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_EXTENDEDPRICE"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_ORDTOTALPRICE"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_DISCOUNT"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_REVENUE"), ::arrow::int64()),
                 ::arrow::field(ColumnName::canonicalize("LO_SUPPLYCOST"), ::arrow::int64()),
                 ::arrow::field(ColumnName::canonicalize("LO_TAX"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_COMMITDATE"), ::arrow::int32()),
                 ::arrow::field(ColumnName::canonicalize("LO_SHIPMODE"), ::arrow::utf8())};

  auto schema = std::make_shared<::arrow::Schema>(fields);
  return schema;
}

std::shared_ptr<TupleSet2> readParquetFile() {
  auto schema = lineOrderSchema();
  auto columnNames = {"lo_custkey", "lo_orderdate", "lo_partkey", "lo_revenue", "lo_suppkey", "lo_supplycost", "lo_quantity"};
  std::vector<int> columnIndices;
  for (auto &columnName : columnNames) {
    columnIndices.emplace_back(schema->GetFieldIndex(columnName));
  }

  std::string filePath = "/dev/shm/lineorder.parquet.0";
  auto expectedInFile = arrow::io::ReadableFile::Open(filePath);
  if (!expectedInFile.ok()) {
    SPDLOG_ERROR("Error creating ReadableFile for: {}", filePath);
  }
  auto inFile = expectedInFile.ValueOrDie();

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  arrow::MemoryPool *pool = arrow::default_memory_pool();
  arrow::Status st = parquet::arrow::OpenFile(inFile, pool, &arrow_reader);
  arrow_reader->set_use_threads(false);
  if (!st.ok()) {
    SPDLOG_ERROR("Error opening file for {}\nError: {}", filePath, st.message());
  }

  std::shared_ptr<arrow::Table> table;
  st = arrow_reader->ReadTable(columnIndices, &table);
  if (!st.ok()) {
    SPDLOG_ERROR("Error reading parquet data for {}\nError: {}", filePath, st.message());
  }

  auto tupleSet = TupleSet2::make(table);
  return tupleSet;
}

std::shared_ptr<TupleSet2> selectParquet() {
  auto inputTupleSet = readParquetFile();

  // sql: "select lo_custkey, lo_orderdate, lo_partkey, lo_revenue, lo_suppkey, lo_supplycost from s3Object
  // where (cast(lo_quantity as int) >= 20 and cast(lo_quantity as int) <= 30)"
  auto comp1 = gte(col("lo_quantity"), num_lit<::arrow::Int32Type, int>(20));
  auto comp2 = lte(col("lo_quantity"), num_lit<::arrow::Int32Type, int>(30));
  auto expr = and_(comp1, comp2);
  auto filter = normal::expression::gandiva::Filter::make(expr);
  filter->compile(inputTupleSet->schema().value());
  auto filteredTupleSet = filter->evaluate(*inputTupleSet);

  // project
  auto columnNames = {"lo_custkey", "lo_orderdate", "lo_partkey", "lo_revenue", "lo_suppkey", "lo_supplycost"};
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> arrowColumns;
  for (auto &columnName: columnNames) {
    fields.emplace_back(filteredTupleSet->schema().value()->getSchema()->GetFieldByName(columnName));
    arrowColumns.emplace_back(filteredTupleSet->getArrowTable().value()->GetColumnByName(columnName));
  }
  auto outputSchema = std::make_shared<arrow::Schema>(fields);
  auto outputTupleSet = TupleSet2::make(outputSchema, arrowColumns);

  return outputTupleSet;
}

TEST_SUITE ("parquet-benchmark-single-core" * doctest::skip(false)) {

TEST_CASE ("parquet-benchmark-single-core-read" * doctest::skip(false || SKIP_SUITE)) {
  std::shared_ptr<TupleSet2> tupleSet;

  ankerl::nanobench::Config().minEpochIterations(5).run(
    fmt::format("parquet-single-core-read"), [&] {
        tupleSet = readParquetFile();
    });

  SPDLOG_INFO("NumRows: {}\nOutput: \n{}", tupleSet->numRows(),
              tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("parquet-benchmark-single-core-select" * doctest::skip(false || SKIP_SUITE)) {
  std::shared_ptr<TupleSet2> tupleSet;

  ankerl::nanobench::Config().minEpochIterations(5).run(
    fmt::format("parquet-single-core-select"), [&] {
        tupleSet = selectParquet();
    });

  SPDLOG_INFO("NumRows: {}\nOutput: \n{}", tupleSet->numRows(),
              tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

}