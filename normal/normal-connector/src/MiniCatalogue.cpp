//
// Created by Yifei Yang on 7/15/20.
//

#include <string>
#include <normal/connector/MiniCatalogue.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <experimental/filesystem>
#include <fstream>

normal::connector::MiniCatalogue::MiniCatalogue(
        const std::shared_ptr<std::unordered_map<std::string, int>> partitionNums,
        const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> &schemas,
        const std::shared_ptr<std::unordered_map<std::string, int>> &columnLengthMap,
        const std::shared_ptr<std::vector<std::string>> &defaultJoinOrder,
        const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
                std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> &sortedColumns) :
        partitionNums_(partitionNums),
        schemas_(schemas),
        columnLengthMap_(columnLengthMap),
        defaultJoinOrder_(defaultJoinOrder),
        sortedColumns_(sortedColumns) {}

std::vector<std::string> split(std::string str, std::string splitStr) {
  std::vector<std::string> res;
  std::string::size_type pos1, pos2;
  pos2 = str.find(splitStr);
  pos1 = 0;
  while (pos2 != std::string::npos)
  {
    res.push_back(str.substr(pos1, pos2 - pos1));
    pos1 = pos2 + 1;
    pos2 = str.find(splitStr, pos1);
  }
  if (pos1 < str.length()) {
    res.push_back(str.substr(pos1));
  }
  return res;
}

std::vector<std::string> readFileByLines(std::experimental::filesystem::path filePath) {
  std::ifstream file(filePath.string());
  std::vector<std::string> res;
  std::string str;
  while (getline(file, str)) {
    res.emplace_back(str);
  }
  return res;
}

std::shared_ptr<std::vector<std::pair<std::string, std::string>>> readMetadataSort(std::string schemaName, std::string fileName) {
  auto res = std::make_shared<std::vector<std::pair<std::string, std::string>>>();
  auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append("sort").append(fileName);
  for (auto const &str: readFileByLines(filePath)) {
    auto splitRes = split(str, ",");
    res->emplace_back(std::pair<std::string, std::string>(splitRes[0], splitRes[1]));
  }
  return res;
}

std::shared_ptr<std::unordered_map<std::string, int>> readMetadataColumnLength(std::string schemaName) {
  auto res = std::make_shared<std::unordered_map<std::string, int>>();
  auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append("column_length");
  for (auto const &str: readFileByLines(filePath)) {
    auto splitRes = split(str, ",");
    res->emplace(splitRes[0], stoi(splitRes[1]));
  }
  return res;
}

std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> readMetadataSchemas(std::string schemaName) {
  auto res = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append("schemas");
  for (auto const &str: readFileByLines(filePath)) {
    auto splitRes = split(str, ":");
    auto columnNames = std::make_shared<std::vector<std::string>>();
    for (auto const &columnName: split(splitRes[1], ",")) {
      columnNames->emplace_back(columnName);
    }
    res->emplace(splitRes[0], columnNames);
  }
  return res;
}

std::shared_ptr<std::unordered_map<std::string, int>> readMetadataPartitionNums(std::string schemaName) {
  auto res = std::make_shared<std::unordered_map<std::string, int>>();
  auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append("partitionNums");
  for (auto const &str: readFileByLines(filePath)) {
    auto splitRes = split(str, ",");
    res->emplace(splitRes[0], stoi(splitRes[1]));
  }
  return res;
}

std::shared_ptr<normal::connector::MiniCatalogue> normal::connector::MiniCatalogue::defaultMiniCatalogue(
        std::string s3Bucket, std::string schemaName) {
  // star join order
  auto defaultJoinOrder = std::make_shared<std::vector<std::string>>();
  defaultJoinOrder->emplace_back("supplier");
  defaultJoinOrder->emplace_back("date");
  defaultJoinOrder->emplace_back("customer");
  defaultJoinOrder->emplace_back("part");

  // schemas
  auto schemas = readMetadataSchemas(schemaName);

  // partitionNums
  auto partitionNums = readMetadataPartitionNums(schemaName);

  // columnLengthMap
  auto columnLengthMap = readMetadataColumnLength(schemaName);

  // sortedColumns
  auto sortedColumns = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
          std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>>();

  // sorted lo_orderdate
  auto sortedValues = std::make_shared<std::unordered_map<std::shared_ptr<Partition>, std::pair<std::string, std::string>,
          PartitionPointerHash, PartitionPointerPredicate>>();
  auto valuePairs = readMetadataSort(schemaName, "lineorder_orderdate");
  std::string s3ObjectDir = schemaName + "/lineorder_sharded/";
  for (int i = 0; i < valuePairs->size(); i++) {
    sortedValues->emplace(std::make_shared<S3SelectPartition>(s3Bucket, s3ObjectDir + "lineorder.tbl." + std::to_string(i)),
                          valuePairs->at(i));
  }
  sortedColumns->emplace("lo_orderdate", sortedValues);

  return std::make_shared<MiniCatalogue>(partitionNums, schemas, columnLengthMap, defaultJoinOrder, sortedColumns);
}

const std::shared_ptr<std::unordered_map<std::string, int>> &normal::connector::MiniCatalogue::partitionNums() const {
  return partitionNums_;
}

const std::shared_ptr<std::vector<std::string>> &normal::connector::MiniCatalogue::defaultJoinOrder() const {
  return defaultJoinOrder_;
}

std::string normal::connector::MiniCatalogue::findTableOfColumn(std::string columnName) {
  for (const auto &schema: *schemas_) {
    for (const auto &existColumnName: *(schema.second)) {
      if (existColumnName == columnName) {
        return schema.first;
      }
    }
  }
  throw std::runtime_error("Column " + columnName + " not found");
}

double normal::connector::MiniCatalogue::lengthFraction(std::string columnName) {
  auto thisLength = columnLengthMap_->find(columnName)->second;
  auto tableName = findTableOfColumn(columnName);
  int allLength = 0;
  for (auto const &otherColumnName: *schemas_->find(tableName)->second) {
    allLength += columnLengthMap_->find(otherColumnName)->second;
  }
  return (double)thisLength / (double)allLength;
}

const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> &
normal::connector::MiniCatalogue::sortedColumns() const {
  return sortedColumns_;
}

std::shared_ptr<std::vector<std::string>> normal::connector::MiniCatalogue::tables(){
  auto tables = std::make_shared<std::vector<std::string>>();
  for (auto const &schema: *schemas_) {
    tables->emplace_back(schema.first);
  }
  return tables;
}
