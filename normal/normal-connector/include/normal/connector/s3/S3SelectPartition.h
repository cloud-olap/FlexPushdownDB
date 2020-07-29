//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H

#include <string>

#include <normal/connector/partition/Partition.h>
#include <memory>

class S3SelectPartition: public Partition {
public:
  explicit S3SelectPartition(std::string bucket, std::string object, long numBytes);

  const std::string &getBucket() const;
  const std::string &getObject() const;
  const long &getNumBytes() const;

  std::string toString() override;
  size_t hash() override;

  bool equalTo(std::shared_ptr<Partition> other) override;

  bool operator==(const S3SelectPartition& other);

private:
  std::string bucket_;
  std::string object_;
  long numBytes_;

};

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
