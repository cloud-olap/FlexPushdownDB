//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/frontend/Client.h>
#include <normal/frontend/Config.h>
#include <normal/connector/MiniCatalogue.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/pushdown/Globals.h>
#include <normal/util/Util.h>

using namespace normal::frontend;
using namespace normal::connector;
using namespace normal::sql;
using namespace normal::util;
using namespace normal::pushdown;

Client::Client(){
  config_ = parseConfig();
}

std::string Client::boot() {
  /* Initialize query processing */
  DefaultS3Client = AWSClient::defaultS3Client();
  defaultMiniCatalogue = MiniCatalogue::defaultMiniCatalogue(config_->getS3Bucket(), config_->getS3Dir());
  interpreter_ = std::make_shared<Interpreter>(config_->getMode(), config_->getCachingPolicy());
  configureS3ConnectorMultiPartition(interpreter_, config_->getS3Bucket(), config_->getS3Dir());
  interpreter_->boot();
  std::string output = "Client booted";
  return output;
}

std::string Client::stop() {
  interpreter_->getOperatorGraph().reset();
  interpreter_->stop();
  interpreter_.reset();
  defaultMiniCatalogue.reset();
  return "Client stopped";
}

std::string Client::reboot() {
  stop();
  boot();
  return "Client rebooted";
}

std::shared_ptr<TupleSet> Client::execute() {
  interpreter_->getCachingPolicy()->onNewQuery();
  interpreter_->getOperatorGraph()->boot();
  interpreter_->getOperatorGraph()->start();
  interpreter_->getOperatorGraph()->join();
  auto tuples = interpreter_->getOperatorGraph()->getQueryResult();
  return tuples;
}

std::string Client::executeSql(const std::string &sql) {
  interpreter_->clearOperatorGraph();
  interpreter_->parse(sql);
  auto tuples = execute();
  auto tupleSet = TupleSet2::create(tuples);
  std::string output = fmt::format("Result |\n{}", tupleSet->showString(
          TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  output += fmt::format("{}", interpreter_->getOperatorGraph()->showMetrics(
          config_->showOpTimes(), config_->showScanMetrics()));
  output += fmt::format("\nFinished, time: {} secs",
                        (double) (interpreter_->getOperatorGraph()->getElapsedTime().value()) / 1000000000.0);
  return output;
}

std::string Client::executeSqlFile(const std::string &filePath) {
  auto sql = readFile(filePath);
  return executeSql(sql);
}

void Client::configureS3ConnectorSinglePartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, const std::string& dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3", conn);

  // look up tables
  auto tableNames = normal::connector::defaultMiniCatalogue->tables();
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (const auto &tableName: *tableNames) {
    auto s3Object = dir_prefix + tableName + ".tbl";
    s3Objects->emplace_back(s3Object);
  }
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, DefaultS3Client);

  // configure s3Connector
  for (size_t tbl_id = 0; tbl_id < tableNames->size(); tbl_id++) {
    auto &tableName = tableNames->at(tbl_id);
    auto &s3Object = s3Objects->at(tbl_id);
    auto numBytes = objectNumBytes_Map.find(s3Object)->second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i->put(cat);
}

void Client::configureS3ConnectorMultiPartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, const std::string& dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3", conn);

  // get partitionNums
  auto s3ObjectsMap = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  auto partitionNums = normal::connector::defaultMiniCatalogue->partitionNums();
  for (auto const &partitionNumEntry: *partitionNums) {
    auto tableName = partitionNumEntry.first;
    auto partitionNum = partitionNumEntry.second;
    auto objects = std::make_shared<std::vector<std::string>>();
    if (partitionNum == 1) {
      if (dir_prefix.find("csv") != std::string::npos) {
        objects->emplace_back(dir_prefix + tableName + ".tbl");
      } else if (dir_prefix.find("parquet") != std::string::npos) {
        objects->emplace_back(dir_prefix + tableName + ".parquet");
      } else {
        // something went wrong, this will cause an error later
        SPDLOG_ERROR("Unknown file name to use for directory with prefix: {}", dir_prefix);
      }
      s3ObjectsMap->emplace(tableName, objects);
    } else {
      for (int i = 0; i < partitionNum; i++) {
        if (dir_prefix.find("csv") != std::string::npos) {
          objects->emplace_back(fmt::format("{0}{1}_sharded/{1}.tbl.{2}", dir_prefix, tableName, i));
        } else if (dir_prefix.find("parquet") != std::string::npos) {
          objects->emplace_back(fmt::format("{0}{1}_sharded/{1}.parquet.{2}", dir_prefix, tableName, i));
        } else {
          // something went wrong, this will cause an error later
          SPDLOG_ERROR("Unknown file name to use for directory with prefix: {}", dir_prefix);
        }
      }
      s3ObjectsMap->emplace(tableName, objects);
    }
  }

  // look up tables
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto objects = s3ObjectPair.second;
    s3Objects->insert(s3Objects->end(), objects->begin(), objects->end());
  }
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, DefaultS3Client);

  // configure s3Connector
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto tableName = s3ObjectPair.first;
    auto objects = s3ObjectPair.second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    for (auto const &s3Object: *objects) {
      auto numBytes = objectNumBytes_Map.find(s3Object)->second;
      partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
    }
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i->put(cat);
}

std::vector<std::string> normal::frontend::readAllRemoteIps() {
  auto localIp = getLocalIp();
  auto ips = readFileByLine(std::filesystem::current_path()
          .parent_path()
          .append("resources/config/cluster_ips")
          .string());
  for (auto it = ips.begin(); it != ips.end(); it++) {
    if (localIp == *it) {
      ips.erase(it);
      return ips;
    }
  }
  return ips;
}
