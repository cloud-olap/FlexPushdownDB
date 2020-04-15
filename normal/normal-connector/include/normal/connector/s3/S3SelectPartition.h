//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H

#include <string>

#include <normal/connector/partition/Partition.h>

class S3SelectPartition: public Partition {
public:
  explicit S3SelectPartition(std::string bucket, std::string object);

  const std::string &getBucket() const;
  const std::string &getObject() const;

private:
  std::string bucket_;
  std::string object_;

};

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
