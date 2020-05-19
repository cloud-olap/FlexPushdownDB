//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_LOCALFILEPARTITION_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_LOCALFILEPARTITION_H

#include <string>

#include <normal/connector/partition/Partition.h>

class LocalFilePartition: public Partition {
public:
  explicit LocalFilePartition(std::string Path);

  [[nodiscard]] const std::string &getPath() const;
  std::string toString() override;
  size_t hash() override;

  bool equalTo(std::shared_ptr<Partition> other) override;

  bool operator==(const LocalFilePartition& other);

private:
  std::string path_;

};

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_LOCALFILEPARTITION_H
