//
// Created by matt on 11/8/20.
//


#include <memory>
#include <filesystem>

#include <doctest/doctest.h>
#include <fmt/format.h>

#include <normal/tuple/Converter.h>
#include <normal/tuple/Globals.h>

using namespace normal::tuple;

const char *getCurrentTestName();
const char *getCurrentTestSuiteName();

#define SKIP_SUITE false

TEST_SUITE ("parquet" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("parquet-csv-to-parquet" * doctest::skip(false || SKIP_SUITE)) {

  auto outDir = fmt::format("tests/{}/{}", getCurrentTestSuiteName(), getCurrentTestName());

  std::filesystem::create_directories(outDir);

  auto fields = {::arrow::field("A", ::arrow::int32()),
				 ::arrow::field("B", ::arrow::int32()),
				 ::arrow::field("C", ::arrow::int32())};
  auto schema = std::make_shared<::arrow::Schema>(fields);

  auto result = Converter::csvToParquet("data/csv/test.csv",
										fmt::format("{}/test.snappy.parquet", outDir),
										*schema,
										DefaultChunkSize);

	  CHECK_MESSAGE(result.has_value(), result.error());

}

}
