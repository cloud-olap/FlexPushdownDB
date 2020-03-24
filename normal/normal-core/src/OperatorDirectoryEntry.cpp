//
// Created by matt on 24/3/20.
//

#include "normal/core/OperatorDirectoryEntry.h"

#include <utility>

namespace normal::core {

OperatorDirectoryEntry::OperatorDirectoryEntry(std::string name,
                                               bool complete) :
    name_(std::move(name)),
    complete_(complete) {}

const std::string &OperatorDirectoryEntry::name() const {
  return name_;
}

bool OperatorDirectoryEntry::complete() const {
  return complete_;
}

void OperatorDirectoryEntry::complete(bool complete) {
  complete_ = complete;
}

}
