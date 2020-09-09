//
// Created by Yifei Yang on 7/15/20.
//

#ifndef NORMAL_MINICATALOGUE_H
#define NORMAL_MINICATALOGUE_H


#include <unordered_map>
#include <vector>
#include <normal/connector/partition/Partition.h>

namespace normal::connector {

/**
 * A hardcoded catalogue used for generating the logical plan. May be integrated with normal-connector::Catalogue?
 */
class MiniCatalogue {

public:
  MiniCatalogue(const std::shared_ptr<std::unordered_map<std::string, int>> partitionNums,
                const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> &schemas,
                const std::shared_ptr<std::unordered_map<std::string, int>> &columnLengthMap,
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
  double lengthFraction(std::string columnName);

private:
  std::shared_ptr<std::unordered_map<std::string, int>> partitionNums_;
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> schemas_;
  std::shared_ptr<std::unordered_map<std::string, int>> columnLengthMap_;

  // default star join order, "lineorder" is center, order rest from small size to large size
  std::shared_ptr<std::vector<std::string>> defaultJoinOrder_;

  // FIXME: better if using std::pair<gandiva::Expression, gandiva::Expression> ?
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
    std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> sortedColumns_;
};

const static std::shared_ptr<MiniCatalogue> defaultMiniCatalogue =
        MiniCatalogue::defaultMiniCatalogue("s3filter", "ssb-sf10-sortlineorder/");

}


#endif //NORMAL_MINICATALOGUE_H
