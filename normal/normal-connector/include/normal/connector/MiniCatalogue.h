//
// Created by Yifei Yang on 7/15/20.
//

#ifndef NORMAL_MINICATALOGUE_H
#define NORMAL_MINICATALOGUE_H


#include <unordered_map>
#include <vector>
#include <set>
#include <normal/connector/partition/Partition.h>
#include <normal/cache/SegmentKey.h>

namespace normal::connector {

/**
 * A hardcoded catalogue used for generating the logical plan. May be integrated with normal-connector::Catalogue?
 */
class MiniCatalogue {

public:
    MiniCatalogue(const std::shared_ptr<std::unordered_map<std::string, int>> partitionNums,
                  const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> &schemas,
                  const std::shared_ptr<std::unordered_map<std::string, int>> &columnLengthMap,
                  const std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, size_t,
                          cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> &segmentKeyToSize,
                  const std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::vector<std::shared_ptr<cache::SegmentKey>>>>> &queryNumToInvolvedSegments,
                  const std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
                          cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> &segmentKeysToInvolvedQueryNums,
                  const std::shared_ptr<std::vector<std::string>> &defaultJoinOrder,
                  const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
                          std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> &sortedColumns);

    static std::shared_ptr<MiniCatalogue> defaultMiniCatalogue(std::string s3Bucket, std::string schemaName);

  const std::shared_ptr<std::unordered_map<std::string, int>> &partitionNums() const;
  const std::shared_ptr<std::vector<std::string>> &defaultJoinOrder() const;
  const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> &
    sortedColumns() const;

  std::shared_ptr<std::vector<std::string>> tables();
  std::string findTableOfColumn(std::string columnName);
  std::shared_ptr<std::vector<std::string>> getColumnsOfTable(std::string tableName);
  size_t getSegmentSize(std::shared_ptr<cache::SegmentKey> segmentKey);
  std::shared_ptr<std::vector<std::shared_ptr<cache::SegmentKey>>> getSegmentsInQuery(int queryNum);
  void addToSegmentQueryNumMappings(int queryNum, std::shared_ptr<cache::SegmentKey> segmentKey);

  int querySegmentNextUsedIn(std::shared_ptr<cache::SegmentKey> segmentKey, int currentQuery);
  double lengthFraction(std::string columnName);

  void setCurrentQueryNum(int queryNum);
  int getCurrentQueryNum();

private:
  std::shared_ptr<std::unordered_map<std::string, int>> partitionNums_;
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> schemas_;
  std::shared_ptr<std::unordered_map<std::string, int>> columnLengthMap_;
  std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, size_t,
  cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeyToSize_;
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::vector<std::shared_ptr<cache::SegmentKey>>>>> queryNumToInvolvedSegments_;
  std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
  cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeysToInvolvedQueryNums_;
  int currentQueryNum_;

  // default star join order, "lineorder" is center, order rest from small size to large size
  std::shared_ptr<std::vector<std::string>> defaultJoinOrder_;

  // FIXME: better if using std::pair<gandiva::Expression, gandiva::Expression> ?
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
    std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> sortedColumns_;
};

const static std::shared_ptr<MiniCatalogue> defaultMiniCatalogue =
        MiniCatalogue::defaultMiniCatalogue("pushdowndb", "ssb-sf10-sortlineorder/csv/");

}


#endif //NORMAL_MINICATALOGUE_H
