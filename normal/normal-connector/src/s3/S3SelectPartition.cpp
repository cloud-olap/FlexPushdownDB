//
// Created by matt on 15/4/20.
//

#include "normal/connector/s3/S3SelectPartition.h"

S3SelectPartition::S3SelectPartition(std::string bucket, std::string object) :
	bucket_(bucket),
	object_(object) {}

const std::string &S3SelectPartition::getBucket() const {
  return bucket_;
}

const std::string &S3SelectPartition::getObject() const {
  return object_;
}

std::string S3SelectPartition::toString() {
  return bucket_ + "/" + object_;
}


