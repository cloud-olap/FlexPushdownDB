//
// Created by matt on 24/3/20.
//

#include "normal/core/OperatorDirectoryEntry.h"

#include <utility>
#include <caf/all.hpp>

namespace normal::core {

OperatorDirectoryEntry::OperatorDirectoryEntry(std::string name,
                                               std::optional<caf::actor> actor,
                                               bool complete) :
    name_(std::move(name)),
    actor_(std::move(actor)),
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
