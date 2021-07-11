//
// Created by Yifei Yang on 7/15/20.
//

#ifndef NORMAL_MINICATALOGUE_H
#define NORMAL_MINICATALOGUE_H

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <set>
#include <normal/connector/partition/Partition.h>
#include <normal/cache/SegmentKey.h>
#include <arrow/api.h>

namespace normal::connector {

/**
 * A hardcoded catalogue used for generating the logical plan. May be integrated with normal-connector::Catalogue?
 */
class MiniCatalogue {

public:
  MiniCatalogue(std::string schemaName,
                std::shared_ptr<std::unordered_map<std::string, int>>  partitionNums,
                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>> schemas,
                std::shared_ptr<std::unordered_map<std::string, int>> columnLengthMap,
                std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, size_t,
                        cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeyToSize,
                std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::unordered_set
                        <std::shared_ptr<cache::SegmentKey>, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>>>> queryNumToInvolvedSegments,
                std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
                        cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeysToInvolvedQueryNums,
                std::shared_ptr<std::vector<std::string>> defaultJoinOrder,
                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
                        std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> sortedColumns,
                char csvFileDelimiter);
  static std::shared_ptr<MiniCatalogue> defaultMiniCatalogue(const std::string& s3Bucket, const std::string& schemaName);

  [[nodiscard]] const std::shared_ptr<std::unordered_map<std::string, int>> &partitionNums() const;
  [[nodiscard]] const std::shared_ptr<std::vector<std::string>> &defaultJoinOrder() const;
  [[nodiscard]] const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> &
    sortedColumns() const;

  std::shared_ptr<std::vector<std::string>> tables();
  std::string findTableOfColumn(const std::string& columnName);
  double lengthFraction(const std::string& columnName);
  int lengthOfRow(const std::string& tableName);
  int lengthOfColumn(const std::string& columnName);

  /*
   * Used in Belady
   */
  std::shared_ptr<std::vector<std::string>> getColumnsOfTable(const std::string& tableName);
  size_t getSegmentSize(const std::shared_ptr<cache::SegmentKey>& segmentKey);
  std::shared_ptr<std::unordered_set<std::shared_ptr<cache::SegmentKey>, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> getSegmentsInQuery(int queryNum);
  void addToSegmentQueryNumMappings(int queryNum, const std::shared_ptr<cache::SegmentKey>& segmentKey);
  int querySegmentNextUsedIn(const std::shared_ptr<cache::SegmentKey>& segmentKey, int currentQuery);
  void setCurrentQueryNum(int queryNum);
  [[nodiscard]] int getCurrentQueryNum() const;

  std::shared_ptr<arrow::Schema> getSchema(const std::string &tableName);
  [[nodiscard]] const std::string &getSchemaName() const;
  [[nodiscard]] char getCSVFileDelimiter() const;

private:
  std::string schemaName_;
  std::shared_ptr<std::unordered_map<std::string, int>> partitionNums_;
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>> schemas_;
  std::shared_ptr<std::unordered_map<std::string, int>> columnLengthMap_;
  std::shared_ptr<std::unordered_map<std::string, int>> rowLengthMap_;
  std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, size_t,
  cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeyToSize_;
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::unordered_set<std::shared_ptr<cache::SegmentKey>, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>>>> queryNumToInvolvedSegments_;
  std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
  cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeysToInvolvedQueryNums_;
  int currentQueryNum_;

  // default star join order, "lineorder" is center, order rest from small size to large size
  std::shared_ptr<std::vector<std::string>> defaultJoinOrder_;

  // FIXME: better if using std::pair<gandiva::Expression, gandiva::Expression> ?
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
    std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> sortedColumns_;
  char csvFileDelimiter_;
};

std::string getFileExtensionByDirPrefix(const std::string& dir_prefix);

inline std::shared_ptr<MiniCatalogue> defaultMiniCatalogue;

}


#endif //NORMAL_MINICATALOGUE_H
