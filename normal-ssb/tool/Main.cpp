//
// Created by matt on 11/8/20.
//

#include <experimental/filesystem>

#include <spdlog/spdlog.h>

#include <normal/tuple/Converter.h>
#include <normal/tuple/Globals.h>
#include <normal/ssb/SSBSchema.h>

using namespace normal::tuple;

std::vector<std::pair<const char *, std::shared_ptr<arrow::Schema>>> getTables() {
  auto tables = {
	  std::pair{"customer", SSBSchema::customer()},
	  std::pair{"date", SSBSchema::date()},
	  std::pair{"lineorder", SSBSchema::lineOrder()},
	  std::pair{"part", SSBSchema::part()},
	  std::pair{"supplier", SSBSchema::supplier()}
  };
  return tables;
}

void convertFile(std::string inFile, std::string outFile, ::parquet::Compression::type compressionType,
                 const arrow::Schema &schema) {
  auto result = Converter::csvToParquet(inFile,
                                        outFile,
                                        schema,
                                        DefaultChunkSize,
                                        compressionType);

  if (result.has_value())
    SPDLOG_INFO("Success. In: {}, Out: {}", inFile, outFile);
  else
    SPDLOG_ERROR("Failure. {}", result.error());
}

void convert(const std::string &inDir, const std::string &outDir) {

  std::experimental::filesystem::create_directories(outDir);

  auto tables = getTables();

  for (const auto &table: tables) {
    const std::string inFile = fmt::format("{}/{}.tbl", inDir, table.first);
    const std::string outFile = fmt::format("{}/{}.snappy.parquet", outDir, table.first);

    convertFile(inFile, outFile, ::parquet::Compression::type::SNAPPY, *table.second);
  }
}

void convertWithShards(const std::string &inDir, const std::string &outDir,
                       std::unordered_map<std::string, int> tableToShards,
                       ::parquet::Compression::type compressionType,
                       std::string fileExtension) {

  std::experimental::filesystem::create_directories(outDir);

  auto tables = getTables();

  for (const auto &table: tables) {
    auto tableNumShards = tableToShards[table.first];
    if (tableNumShards == 1) {
      const std::string inFile = fmt::format("{}/{}.tbl", inDir, table.first);
      const std::string outFile = fmt::format("{}/{}.{}", outDir, table.first, fileExtension);
      convertFile(inFile, outFile, compressionType, *table.second);
    } else {
      std::experimental::filesystem::create_directories(fmt::format("{}/{}_sharded", outDir, table.first));
      for (int shard = 0; shard < tableNumShards; shard++) {
        const std::string inFile = fmt::format("{}/{}_sharded/{}.tbl.{}", inDir, table.first, table.first, shard);
        const std::string outFile = fmt::format("{}/{}_sharded/{}.{}.{}", outDir, table.first, table.first,
                                                fileExtension, shard);
        convertFile(inFile, outFile, compressionType, *table.second);
      }
    }
  }
}

int main(int, char **) {

  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S.%e] [thread %t] [%! (%s:%#)] [%l]  %v");

//  convert("/home/matt/Work/pushdownDB/normal/normal-ssb/data/ssb-sf0.01", "/home/matt/Work/pushdownDB/normal/normal-ssb/data/ssb-sf0.01/parquet");
//  convert("/home/matt/Work/pushdownDB/normal/normal-ssb/data/ssb-sf1", "/home/matt/Work/pushdownDB/normal/normal-ssb/data/ssb-sf1/parquet");

//  convert("/data/remoteCLion/normal-ssb/data/ssb-sf0.01", "/data/remoteCLion/normal-ssb/data/ssb-sf0.01/parquet");


//  convertFile("/datagen/home/ec2-user/ssb-dbgen/lineorder.tbl.0", "/datagen/home/ec2-user/ssb-dbgen/lineorder.parquet.0",
//              ::parquet::Compression::type::UNCOMPRESSED, *SSBSchema::lineOrder());
//  convertFile("/datagen/home/ec2-user/ssb-dbgen/lineorder.tbl.0", "/datagen/home/ec2-user/ssb-dbgen/lineorder.snappy.parquet.0",
//              ::parquet::Compression::type::SNAPPY, *SSBSchema::lineOrder());

//  std::unordered_map<std::string, int> tableToShards16MB = {
//	  {"customer", 20},
//	  {"date", 1},
//	  {"lineorder", 4001},
//	  {"part", 10},
//	  {"supplier", 1}
//  };

  std::unordered_map<std::string, int> tableToShards163MB = {
	  {"customer", 1},
	  {"date", 1},
	  {"lineorder", 400},
	  {"part", 1},
	  {"supplier", 1}
  };

  convertWithShards("/datagen/csv_150MB", "/datagen/parquet_150MB",
                    tableToShards163MB, ::parquet::Compression::type::UNCOMPRESSED, "parquet");

  convertWithShards("/datagen/csv_150MB", "/datagen/gzip_parquet_150MB",
                  tableToShards163MB, ::parquet::Compression::type::GZIP, "gzip.parquet");

  return EXIT_SUCCESS;
}
