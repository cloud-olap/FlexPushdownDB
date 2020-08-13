//
// Created by matt on 11/8/20.
//

#include <filesystem>

#include <spdlog/spdlog.h>

#include <normal/tuple/Converter.h>
#include <normal/tuple/Globals.h>
#include <normal/ssb/SSBSchema.h>

using namespace normal::tuple;

void convert(const std::string &inDir, const std::string &outDir) {

  std::filesystem::create_directories(outDir);

  auto tables = {
	  std::pair{"customer", SSBSchema::customerSchema()},
	  std::pair{"date", SSBSchema::dateSchema()},
	  std::pair{"lineorder", SSBSchema::lineOrder()},
	  std::pair{"part", SSBSchema::part()},
	  std::pair{"supplier", SSBSchema::supplier()}
  };

  for (const auto &table: tables) {
	const std::string inFile = fmt::format("{}/{}.tbl", inDir, table.first);
	const std::string outFile = fmt::format("{}/{}.snappy.parquet", outDir, table.first);

	auto result = Converter::csvToParquet(inFile,
										  outFile,
										  *table.second,
										  DefaultChunkSize);

	if (result.has_value())
	  SPDLOG_INFO("Success. In: {}, Out: {}", inFile, outFile);
	else
	  SPDLOG_ERROR("Failure. {}", result.error());
  }
}

int main(int, char **) {

  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S.%e] [thread %t] [%! (%s:%#)] [%l]  %v");

  convert("/home/matt/Work/pushdownDB/normal/normal-ssb/data/ssb-sf0.01", "/home/matt/Work/pushdownDB/normal/normal-ssb/data/ssb-sf0.01/parquet");
  convert("/home/matt/Work/pushdownDB/normal/normal-ssb/data/ssb-sf1", "/home/matt/Work/pushdownDB/normal/normal-ssb/data/ssb-sf1/parquet");

  return EXIT_SUCCESS;
}