//
// Created by matt on 11/8/20.
//


#include <memory>
#include <experimental/filesystem>

#include <doctest/doctest.h>
#include <fmt/format.h>

#include <normal/tuple/Converter.h>
#include <normal/tuple/Globals.h>
#include <normal/tuple/ParquetReader.h>
#include <normal/tuple/Util.h>

using namespace normal::tuple;

const char *getCurrentTestName();
const char *getCurrentTestSuiteName();

#define SKIP_SUITE false

tl::expected<void, std::string> convert(const std::string& inFile, const std::string& outFile, int rowGroupSize) {

  std::experimental::filesystem::create_directories(std::experimental::filesystem::absolute(outFile).remove_filename());

  auto fields = {::arrow::field("A", ::arrow::int32()),
				 ::arrow::field("B", ::arrow::int32()),
				 ::arrow::field("C", ::arrow::int32())};
  auto schema = std::make_shared<::arrow::Schema>(fields);

  auto result = Converter::csvToParquet(inFile, outFile, *schema, rowGroupSize, ::parquet::Compression::type::SNAPPY);

  return result;
}

TEST_SUITE ("parquet" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("parquet-csv-to-parquet" * doctest::skip(false || SKIP_SUITE)) {

  const std::string inFile = "data/csv/test.csv";
  const std::string outFile = fmt::format("tests/{}/{}/test.snappy.parquet", getCurrentTestSuiteName(), getCurrentTestName());

  auto result = convert(inFile, outFile, DefaultChunkSize);
	  CHECK_MESSAGE(result.has_value(), result.error());
}

TEST_CASE ("parquet-read-byte-range" * doctest::skip(false || SKIP_SUITE)) {

  const std::string inFile = "data/csv/test3x10000.csv";
  const std::string outFile = fmt::format("tests/{}/{}/test3x10000.snappy.parquet", getCurrentTestSuiteName(), getCurrentTestName());

  auto result = convert(inFile, outFile, 300);
	  CHECK_MESSAGE(result.has_value(), result.error());

  auto size = std::experimental::filesystem::file_size(outFile);
  auto scanRanges = Util::ranges<int>(0, size, 3);

  auto expectedReader = ParquetReader::make(outFile);
  if(!expectedReader)
		FAIL (expectedReader.error());
  auto reader = expectedReader.value();

  for (const auto &scanRange: scanRanges) {
	auto expectedTupleSet = reader->read({"A","B","C"}, scanRange.first, scanRange.second);
	if (!expectedTupleSet)
		  FAIL (expectedTupleSet.error());
	auto tupleSet = expectedTupleSet.value();
	SPDLOG_DEBUG("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  }
}

}
