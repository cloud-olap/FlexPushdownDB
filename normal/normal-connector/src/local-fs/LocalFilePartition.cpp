//
// Created by matt on 15/4/20.
//

#include <normal/connector/local-fs/LocalFilePartition.h>

#include <utility>

LocalFilePartition::LocalFilePartition(std::string Path) : path_(std::move(Path)) {}

const std::string &LocalFilePartition::getPath() const {
  return path_;
}

std::string LocalFilePartition::toString() {
  return "file://" + path_;
}

size_t LocalFilePartition::hash() {
  return std::hash<std::string>()("file://" + path_);
}

bool LocalFilePartition::equalTo(std::shared_ptr<Partition> other) {
  auto typedOther = std::static_pointer_cast<const LocalFilePartition>(other);
  if(!typedOther){
	return false;
  }
  else{
	return *this == *typedOther;
  }
}

bool LocalFilePartition::operator==(const LocalFilePartition &other) {
  return path_ == other.path_;
}


