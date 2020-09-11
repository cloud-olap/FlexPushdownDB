//
// Created by matt on 15/4/20.
//

#include "normal/connector/s3/S3SelectPartition.h"

#include <utility>

S3SelectPartition::S3SelectPartition(std::string bucket, std::string object) :
        bucket_(std::move(bucket)),
        object_(std::move(object)) {}

S3SelectPartition::S3SelectPartition(std::string bucket, std::string object, long numBytes) :
	bucket_(std::move(bucket)),
	object_(std::move(object)){
  setNumBytes(numBytes);
}

const std::string &S3SelectPartition::getBucket() const {
  return bucket_;
}

const std::string &S3SelectPartition::getObject() const {
  return object_;
}

std::string S3SelectPartition::toString() {
  return "s3://" + bucket_ + "/" + object_;
}

size_t S3SelectPartition::hash() {
  return std::hash<std::string>()("s3://" + bucket_ + "/" + object_);
}

bool S3SelectPartition::equalTo(std::shared_ptr<Partition> other) {
  auto typedOther = std::static_pointer_cast<const S3SelectPartition>(other);
  if(!typedOther){
	return false;
  }
  else{
	return this->operator==(*typedOther);
  }
}

bool S3SelectPartition::operator==(const S3SelectPartition &other) {
  return bucket_ == other.bucket_ && object_ == other.object_;
}
