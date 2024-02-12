//
// Created by Matt Woicik on 1/8/21.
//

#include <memory>

#include <doctest/doctest.h>

// nanobench doesn't appear as applicable here for getting MB/s conversion rates, so not using it for now
// #include <nanobench.h>

#include <iostream>
#include <random>
#include <functional>
#include <fpdb/pushdown/s3/S3CSVParser.h>
#include "fpdb/pushdown/Globals.h"
#include <fpdb/tuple/TupleSet2.h>

#define SKIP_SUITE true

namespace fpdb::pushdown {
std::shared_ptr<std::stringstream> sampleCxRIntCSVIOStream(int numCols, int numRows) {
  std::uniform_int_distribution<int> dis = std::uniform_int_distribution(1, 9);
  std::random_device rd;
  std::mt19937 gen(rd());
  auto ss = std::make_shared<std::stringstream>();
  for (int r = 0; r < numRows; ++r) {
    for (int c = 1; c <= numCols; ++c) {
      int randomNum = dis(gen);
      if (c < numCols) {
        *ss << randomNum << ",";
      } else {
        *ss << randomNum << "\n";
      }
    }
  }
//  SPDLOG_DEBUG("Input data:\n{}", ss->str());
  // TODO: Wanted this as a basic_iostream but had trouble figuring out how to make one / easily convert it to one
  //       Left it as a stringstream for now as that seems to inherit from basic_iostream, but noting this
  //       in case later change becomes necessary
//  std::basic_iostream<char, std::char_traits<char>> stream = ss;
//  return std::basic_iostream(ss);
//  auto iostream = std::make_shared<std::basic_iostream<char, std::char_traits<char>>>();

  return ss;
}

std::shared_ptr<Aws::Vector<unsigned char>> sampleCxRIntCSVAWSVector(int numCols, int numRows) {
  std::uniform_int_distribution<int> dis = std::uniform_int_distribution(1, 9);
  std::random_device rd;
  std::mt19937 gen(rd());
  auto charAwsVec = std::make_shared<Aws::Vector<unsigned char>>();
  for (int r = 0; r < numRows; ++r) {
    for (int c = 1; c <= numCols; ++c) {
      int randomNum = dis(gen);
      auto stringVal = fmt::format("{}", randomNum);
      char charVal = stringVal.at(0);
      if (c < numCols) {
        charAwsVec->emplace_back(charVal);
        charAwsVec->emplace_back(',');
      } else {
        charAwsVec->emplace_back(charVal);
        charAwsVec->emplace_back('\n');
      }
    }
  }
  return charAwsVec;
}

void S3SelectScanPut(std::shared_ptr<TupleSet2> tupleSet, std::vector<std::string> columnNames, std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>> columns) {
  // code from put(tupleSet); in S3SelectScan.cpp call below
  for (int columnIndex = 0; columnIndex < tupleSet->numColumns(); ++columnIndex) {
    auto columnName = columnNames.at(columnIndex);
    auto readColumn = tupleSet->getColumnByIndex(columnIndex).value();
    auto canonicalColumnName = ColumnName::canonicalize(columnName);
    readColumn->setName(canonicalColumnName);

    auto bufferedColumnArrays = columns[columnIndex];

    if (bufferedColumnArrays == nullptr) {
      bufferedColumnArrays = std::make_shared<std::pair<std::string, ::arrow::ArrayVector>>(readColumn->getName(),
                                                                                            readColumn->getArrowArray()->chunks());
      columns[columnIndex] = bufferedColumnArrays;
    } else {
      // Add the read chunks to this buffered columns chunk vector
      for (int chunkIndex = 0; chunkIndex < readColumn->getArrowArray()->num_chunks(); ++chunkIndex) {
        auto readChunk = readColumn->getArrowArray()->chunk(chunkIndex);
        bufferedColumnArrays->second.emplace_back(readChunk);
      }
    }
  }
}

// Return the number of bytes processed as the S3Select parsing method gets this value from the AWS response
size_t runS3Scan(const std::shared_ptr<std::stringstream> csvStream, std::vector<std::string> columnNames, std::shared_ptr<arrow::Schema> schema){
//  std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();
//  // Code copied from from s3Scan() in S3SelectScan.cpp
//  S3CSVParser s3CSVParser({}, {});
//  auto readSize = DefaultS3ScanBufferSize - 1;
//  char buffer[DefaultS3ScanBufferSize];
//  auto parser = S3CSVParser::make(columnNames, schema);
//
//  auto columns = std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>>(columnNames.size());
//
//  size_t processedBytes = 0;
//  while (!csvStream->eof()) {
//    memset(buffer, 0, DefaultS3ScanBufferSize);
//
//    csvStream->read(buffer, readSize);
//    processedBytes += strlen(buffer);
//    Aws::Vector<unsigned char> charAwsVec(buffer, buffer + readSize);
//
//    std::shared_ptr<TupleSet> tupleSetV1 = parser->parsePayload(charAwsVec);
//    auto tupleSet = TupleSet2::create(tupleSetV1);
////    SPDLOG_DEBUG("Output: {}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
////    SPDLOG_DEBUG("Output: {}", tupleSet->toString());
//    S3SelectScanPut(tupleSet, columnNames, columns);
//  }
//  std::chrono::steady_clock::time_point endTime = std::chrono::steady_clock::now();
//  auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
//  auto convertedMB = processedBytes/1024.0/1024.0;
//  auto conversionRate = convertedMB / (conversionTime / 1.0e9);
//  SPDLOG_DEBUG("S3 Scan converted MB: {}", convertedMB);
//  SPDLOG_DEBUG("S3 Scan converted MB/s for one core: {}", conversionRate);
//  return processedBytes;
}

void runS3SelectScan(Aws::Vector<unsigned char> inputData, std::vector<std::string> columnNames, std::shared_ptr<arrow::Schema> schema, size_t bytesProcessed) {
//  std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();
//  // Code copied from from generateParser() in S3SelectScan.cpp
//  // Including this in runtime as this must be done for parsing the data, even though it is done before the
//  // data is converted
//  S3CSVParser s3CSVParser({}, {});
//  auto parser = S3CSVParser::make(columnNames, schema);
//
//  auto columns = std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>>(columnNames.size());
//
//  std::shared_ptr<TupleSet> tupleSetV1 = parser->parsePayload(inputData);
//	auto tupleSet = TupleSet2::create(tupleSetV1);
//	S3SelectScanPut(tupleSet, columnNames, columns);
//
//  std::chrono::steady_clock::time_point endTime = std::chrono::steady_clock::now();
//  auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
//
//  auto convertedMB = bytesProcessed/1024.0/1024.0;
//  auto conversionRate = convertedMB / (conversionTime / 1.0e9);
//  SPDLOG_DEBUG("S3 Select Scan converted MB: {}", convertedMB);
//  SPDLOG_DEBUG("S3 Select Scan converted MB/s for one core: {}", conversionRate);
}

TEST_SUITE ("parser-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("parser-benchmark" * doctest::skip(false || SKIP_SUITE)) {
  auto numRows = 5000000;
  auto numCols = 10;
  // Generate CSV data as a stream and awsvector, as this is how data is received in S3SelectScan.cpp
  // not worrying about header in the csvStream as the one row shouldn't make a large difference in processing time
  // and isn't necessary for this benchmarking
  auto csvStream = sampleCxRIntCSVIOStream(numCols, numRows);
  auto vectorCharData = sampleCxRIntCSVAWSVector(numCols, numRows);
  SPDLOG_DEBUG("Data generated");

  // Generate column names and the arrow fields to make the schema
  std::vector<std::string> columnNames;
  std::vector<std::shared_ptr<::arrow::Field>> arrowFields;
  for (int i = 0; i < numCols; i++) {
    auto columnName = fmt::format("c_{}", i);
    columnNames.push_back(columnName);
    arrowFields.push_back(::arrow::field(columnName, arrow::int32()));
  }
  std::shared_ptr<arrow::Schema> schema = ::arrow::schema(arrowFields);

  size_t convertedBytes = runS3Scan(csvStream, columnNames, schema);

  runS3SelectScan(*vectorCharData, columnNames, schema, convertedBytes);
}

}
}
