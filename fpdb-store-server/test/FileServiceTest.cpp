//
// Created by Yifei Yang on 2/17/22.
//

#include <fpdb/store/server/file/FileServiceHandler.hpp>
#include <fpdb/store/server/file/RemoteFileReaderBuilder.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/parquet/ParquetFormat.h>
#include <fpdb/tuple/util/FileReaderTestUtil.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <future>

#include <doctest/doctest.h>

using namespace fpdb::store::server::file;
using namespace fpdb::tuple;
using namespace fpdb::tuple::util;
using namespace std::chrono_literals;

#define SKIP_SUITE false

std::unique_ptr<Server> fileServer;

void startFileServer(int port) {
  FileServiceHandler service(".");

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;

  builder.AddListeningPort("0.0.0.0:" + std::to_string(port), grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  fileServer = builder.BuildAndStart();
  fileServer->Wait();
}

void stopFileServer(const std::future<void> &future) {
  fileServer->Shutdown();
  future.wait();
}

TEST_SUITE ("fpdb-store-server/FileServiceTest/remoteCsvReader" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteCsvReader-read-whole-test.csv" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 50051;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto csvFormat = std::make_shared<csv::CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(csvFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/csv/test.csv",
                                              "localhost",
                                              port);

  auto expTupleSet = reader->read();
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadWholeAllTestCsv(*expTupleSet);

  // stop
  stopFileServer(future);
}

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteCsvReader-read-columns-test.csv" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto csvFormat = std::make_shared<csv::CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(csvFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/csv/test.csv",
                                              "localhost",
                                              port);

  auto expTupleSet = reader->read({"a", "b"});
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadColumnsAllTestCsv(*expTupleSet);

  // stop
  stopFileServer(future);
}

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteCsvReader-read-whole-test3x10000.csv" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto csvFormat = std::make_shared<csv::CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(csvFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/csv/test3x10000.csv",
                                              "localhost",
                                              port);

  auto expTupleSet = reader->read();
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadWholeAllTestCsv3x10000(*expTupleSet);

  // stop
  stopFileServer(future);
}

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteCsvReader-read-columns-test3x10000.csv" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto csvFormat = std::make_shared<csv::CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(csvFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/csv/test3x10000.csv",
                                              "localhost",
                                              port);

  auto expTupleSet = reader->read({"a", "b"});
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadColumnsAllTestCsv3x10000(*expTupleSet);

  // stop
  stopFileServer(future);
}

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteCsvReader-file-size-test.csv" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto csvFormat = std::make_shared<csv::CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(csvFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/csv/test.csv",
                                              "localhost",
                                              port);

  auto expFileSize = reader->getFileSize();
          CHECK(expFileSize.has_value());
          CHECK_EQ(*expFileSize, 24);

  // stop
  stopFileServer(future);
}

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteCsvReader-file-size-test3x10000.csv" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto csvFormat = std::make_shared<csv::CSVFormat>(',');
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(csvFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/csv/test3x10000.csv",
                                              "localhost",
                                              port);

  auto expFileSize = reader->getFileSize();
          CHECK(expFileSize.has_value());
          CHECK_EQ(*expFileSize, 60006);

  // stop
  stopFileServer(future);
}

}

TEST_SUITE ("fpdb-store-server/FileServiceTest/remoteParquetReader" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteParquetReader-read-whole-test.parquet" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto parquetFormat = std::make_shared<parquet::ParquetFormat>();
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(parquetFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/parquet/test.parquet",
                                              "localhost",
                                              port);

  auto expTupleSet = reader->read();
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadWholeAllTestCsv(*expTupleSet);

  // stop
  stopFileServer(future);
}

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteParquetReader-read-columns-test.parquet" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto parquetFormat = std::make_shared<parquet::ParquetFormat>();
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(parquetFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/parquet/test.parquet",
                                              "localhost",
                                              port);

  auto expTupleSet = reader->read({"a", "b"});
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadColumnsAllTestCsv(*expTupleSet);

  // stop
  stopFileServer(future);
}

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteParquetReader-read-whole-test3x10000.parquet" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto parquetFormat = std::make_shared<parquet::ParquetFormat>();
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(parquetFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/parquet/test3x10000.parquet",
                                              "localhost",
                                              port);

  auto expTupleSet = reader->read();
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadWholeAllTestCsv3x10000(*expTupleSet);

  // stop
  stopFileServer(future);
}

TEST_CASE ("fpdb-store-server/FileServiceTest/remoteParquetReader-read-columns-test3x10000.parquet" * doctest::skip(false || SKIP_SUITE)) {
  // server
  int port = 4321;
  auto future = std::async(std::launch::async, [&]() { return startFileServer(port); });
  future.wait_for(100ms);

  // read
  auto parquetFormat = std::make_shared<parquet::ParquetFormat>();
  auto schema = FileReaderTestUtil::makeTestSchema();
  auto reader = RemoteFileReaderBuilder::make(parquetFormat,
                                              schema,
                                              "test-resources",
                                              "simple_data/parquet/test3x10000.parquet",
                                              "localhost",
                                              port);

  auto expTupleSet = reader->read({"a", "b"});
          CHECK(expTupleSet.has_value());

  FileReaderTestUtil::checkReadColumnsAllTestCsv3x10000(*expTupleSet);

  // stop
  stopFileServer(future);
}

}
