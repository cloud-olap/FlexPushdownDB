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
  return path_;
}
