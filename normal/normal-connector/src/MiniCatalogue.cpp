//
// Created by Yifei Yang on 7/15/20.
//

#include <string>
#include <normal/connector/MiniCatalogue.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <experimental/filesystem>
#include <fstream>
#include <utility>
#include <iostream>
#include <normal/cache/SegmentKey.h>

using namespace normal::cache;

normal::connector::MiniCatalogue::MiniCatalogue(
        std::shared_ptr<std::unordered_map<std::string, int>>  partitionNums,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> schemas,
        std::shared_ptr<std::unordered_map<std::string, int>> columnLengthMap,
        std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, size_t,
                          SegmentKeyPointerHash, SegmentKeyPointerPredicate>> segmentKeyToSize,
        std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::vector<std::shared_ptr<cache::SegmentKey>>>>> queryNumToInvolvedSegments,
        std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
                          cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeysToInvolvedQueryNums,
        std::shared_ptr<std::vector<std::string>> defaultJoinOrder,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
                std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> sortedColumns) :
        partitionNums_(std::move(partitionNums)),
        schemas_(std::move(schemas)),
        columnLengthMap_(std::move(columnLengthMap)),
        segmentKeyToSize_(std::move(segmentKeyToSize)),
        queryNumToInvolvedSegments_(std::move(queryNumToInvolvedSegments)),
        segmentKeysToInvolvedQueryNums_(std::move(segmentKeysToInvolvedQueryNums)),
        defaultJoinOrder_(std::move(defaultJoinOrder)),
        sortedColumns_(std::move(sortedColumns)) {

  // initialize as 1, only needs to be updated for certain tasks (ie Belady Caching Policy)
  currentQueryNum_ = 1;

  // generate rowLengthMap from columnLengthMap
  rowLengthMap_ = std::make_shared<std::unordered_map<std::string, int>>();
  for (auto const &schemaEntry: *schemas_) {
    auto tableName = schemaEntry.first;
    int rowLength = 0;
    for (auto const &columnName: *schemaEntry.second) {
      rowLength += columnLengthMap_->find(columnName)->second;
    }
    rowLengthMap_->emplace(tableName, rowLength);
  }
}

std::vector<std::string> split(const std::string& str, const std::string& splitStr) {
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

std::shared_ptr<std::vector<std::pair<std::string, std::string>>> readMetadataSort(const std::string& schemaName, const std::string& fileName) {
  auto res = std::make_shared<std::vector<std::pair<std::string, std::string>>>();
  auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append("sort").append(fileName);
  for (auto const &str: readFileByLines(filePath)) {
    auto splitRes = split(str, ",");
    res->emplace_back(std::pair<std::string, std::string>(splitRes[0], splitRes[1]));
  }
  return res;
}

std::shared_ptr<std::unordered_map<std::string, int>> readMetadataColumnLength(const std::string& schemaName) {
  auto res = std::make_shared<std::unordered_map<std::string, int>>();
  auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append("column_length");
  for (auto const &str: readFileByLines(filePath)) {
    auto splitRes = split(str, ",");
    res->emplace(splitRes[0], stoi(splitRes[1]));
  }
  return res;
}

std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> readMetadataSchemas(const std::string& schemaName) {
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

std::shared_ptr<std::unordered_map<std::string, int>> readMetadataPartitionNums(const std::string& schemaName) {
  auto res = std::make_shared<std::unordered_map<std::string, int>>();
  auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append("partitionNums");
  for (auto const &str: readFileByLines(filePath)) {
    auto splitRes = split(str, ",");
    res->emplace(splitRes[0], stoi(splitRes[1]));
  }
  return res;
}

// Create a mapping of segmentKey -> size of segment with values already gathered
std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, size_t, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
readMetadataSegmentInfo(std::string s3Bucket, std::string schemaName) {
  auto res = std::make_shared<std::unordered_map<std::shared_ptr<SegmentKey>, size_t, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append("segment_info");
  for (auto const &str: readFileByLines(filePath)) {
    auto splitRes = split(str, ",");
    std::string objectName = splitRes[0];
    std::string column = splitRes[1];
    unsigned long startOffset = std::stoul(splitRes[2]);
    unsigned long endOffset  = std::stoul(splitRes[3]);
    size_t sizeInBytes;
    sscanf(splitRes[4].c_str(), "%zu", &sizeInBytes);

    std::string s3Object = schemaName + objectName;
    // create the SegmentKey
    auto segmentPartition = std::make_shared<S3SelectPartition>(s3Bucket, s3Object);
    auto segmentRange = SegmentRange::make(startOffset, endOffset);
    auto segmentKey = SegmentKey::make(segmentPartition, column, segmentRange);
    res->emplace(segmentKey, sizeInBytes);
  }
  return res;
}

std::shared_ptr<normal::connector::MiniCatalogue> normal::connector::MiniCatalogue::defaultMiniCatalogue(
        const std::string& s3Bucket, const std::string& schemaName) {
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
  std::string s3ObjectDir = schemaName + "lineorder_sharded/";
  for (int i = 0; i < (int) valuePairs->size(); i++) {
    sortedValues->emplace(std::make_shared<S3SelectPartition>(s3Bucket, s3ObjectDir + "lineorder.tbl." + std::to_string(i)),
                          valuePairs->at(i));
  }
  sortedColumns->emplace("lo_orderdate", sortedValues);

  // initialize this as empty and populate it if necessary
  auto queryNumToInvolvedSegments = std::make_shared<std::unordered_map<int, std::shared_ptr<std::vector<std::shared_ptr<cache::SegmentKey>>>>>();

  // segmentKey to size
  auto segmentKeyToSize = readMetadataSegmentInfo(s3Bucket, schemaName);

  // initialize this as empty and populate it if necessary
  auto segmentKeysToInvolvedQueryNums = std::make_shared<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
                          cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>>();

  return std::make_shared<MiniCatalogue>(partitionNums, schemas, columnLengthMap, segmentKeyToSize, queryNumToInvolvedSegments, segmentKeysToInvolvedQueryNums, defaultJoinOrder, sortedColumns);
}

const std::shared_ptr<std::unordered_map<std::string, int>> &normal::connector::MiniCatalogue::partitionNums() const {
  return partitionNums_;
}

const std::shared_ptr<std::vector<std::string>> &normal::connector::MiniCatalogue::defaultJoinOrder() const {
  return defaultJoinOrder_;
}

std::string normal::connector::MiniCatalogue::findTableOfColumn(const std::string& columnName) {
  for (const auto &schema: *schemas_) {
    for (const auto &existColumnName: *(schema.second)) {
      if (existColumnName == columnName) {
        return schema.first;
      }
    }
  }
  throw std::runtime_error("Column " + columnName + " not found");
}

std::shared_ptr<std::vector<std::string>> normal::connector::MiniCatalogue::getColumnsOfTable(std::string tableName) {
  auto columns = std::make_shared<std::vector<std::string>>();
  for (const auto &schema: *schemas_) {
    if (schema.first == tableName) {
      for (const auto &columnName: *(schema.second)) {
        columns->push_back(columnName);
      }
      return columns;
    }
  }
  throw std::runtime_error("table " + tableName + " not found");
}

// Throws runtime exception if key not present, which ensures failing fast in case of key not being present
// If use cases of this method expand we can change this if necessary.
size_t normal::connector::MiniCatalogue::getSegmentSize(std::shared_ptr<cache::SegmentKey> segmentKey) {
  return segmentKeyToSize_->at(segmentKey);
}

// Used to populate the queryNumToInvolvedSegments_ and segmentKeysToInvolvedQueryNums_ mappings
void normal::connector::MiniCatalogue::addToSegmentQueryNumMappings(int queryNum, std::shared_ptr<cache::SegmentKey> segmentKey) {
  auto queryNumKeyEntry = queryNumToInvolvedSegments_->find(queryNum);
  if (queryNumKeyEntry != queryNumToInvolvedSegments_->end()) {
    queryNumKeyEntry->second->emplace_back(segmentKey);
  } else {
    // first time seeing this queryNum, create a map entry for it
    auto segmentKeys = std::make_shared<std::vector<std::shared_ptr<cache::SegmentKey>>>();
    segmentKeys->emplace_back(segmentKey);
    queryNumToInvolvedSegments_->emplace(queryNum, segmentKeys);
  }

  auto segmentKeyEntry = segmentKeysToInvolvedQueryNums_->find(segmentKey);
  if (segmentKeyEntry != segmentKeysToInvolvedQueryNums_->end()) {
    segmentKeyEntry->second->insert(queryNum);
  } else {
    // first time seeing this key, create a map entry for it
    auto queryNumbers = std::make_shared<std::set<int>>();
    queryNumbers->insert(queryNum);
    segmentKeysToInvolvedQueryNums_->emplace(segmentKey, queryNumbers);
  }
}

// Throws runtime exception if queryNum not present, which ensures failing fast in case of queryNum not being present
// If use cases of this method expand we can change this if necessary.
std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> normal::connector::MiniCatalogue::getSegmentsInQuery(int queryNum) {
  auto key = queryNumToInvolvedSegments_->find(queryNum);
  if (key != queryNumToInvolvedSegments_->end()) {
    return key->second;
  }
  // we must not have populated this queryNum->[segments used] mapping for this queryNum,
  // throw an error as we should have never called this then
  throw std::runtime_error("Error, " + std::to_string(queryNum) + " not populated in segmentKeysToInvolvedQueryNums_ in MiniCatalogue.cpp");
}

// Return the next query that this segmentKey is used in, if not used again according to the
// segmentKeysToInvolvedQueryNums_ mapping set via setSegmentKeysToInvolvedQueryNums then return -1
// Throws runtime exception if segmentKeysToInvolvedQueryNums_ is never set via setSegmentKeysToInvolvedQueryNums
int normal::connector::MiniCatalogue::querySegmentNextUsedIn(std::shared_ptr<cache::SegmentKey> segmentKey, int currentQuery) {
  auto key = segmentKeysToInvolvedQueryNums_->find(segmentKey);
  if (key != segmentKeysToInvolvedQueryNums_->end()) {
    auto involvedQueriesList = key->second;
    auto nextQueryIt = involvedQueriesList->upper_bound(currentQuery);
    if (nextQueryIt != involvedQueriesList->end()) {
      return *nextQueryIt;
    }
    // TODO: segment never used again so return -1 to indicate this.
    return -1;
  }
  // must not exist in our queryNums, throw an error as we should have never called this then
  throw std::runtime_error("Error, " + segmentKey->toString() + " next query requested but never should have been used");
}

double normal::connector::MiniCatalogue::lengthFraction(std::string columnName) {
  auto thisLength = columnLengthMap_->find(columnName)->second;
  auto tableName = findTableOfColumn(columnName);
  auto allLength = rowLengthMap_->find(tableName)->second;
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

int normal::connector::MiniCatalogue::lengthOfRow(const std::string& tableName) {
  return rowLengthMap_->find(tableName)->second;
}

int normal::connector::MiniCatalogue::lengthOfColumn(const std::string& columnName) {
  return columnLengthMap_->find(columnName)->second;
}

void normal::connector::MiniCatalogue::setCurrentQueryNum(int queryNum) {
  currentQueryNum_ = queryNum;
}

int normal::connector::MiniCatalogue::getCurrentQueryNum() {
  return currentQueryNum_;
}
