//
// Created by Yifei Yang on 1/17/22.
//

#include <fpdb/plan/prephysical/SortKey.h>

namespace fpdb::plan::prephysical {

SortKey::SortKey(const std::string &name, SortOrder order) :
  name_(name),
  order_(order) {}

bool SortKey::equals(const SortKey &k1, const SortKey &k2) {
  return k1.name_ == k2.name_ && k1.order_ == k2.order_;
}

bool SortKey::equals(const std::vector<SortKey> &k1, const std::vector<SortKey> &k2) {
  if (k1.size() != k2.size()) {
    return false;
  }
  for (uint i = 0; i < k1.size(); ++i) {
    if (!equals(k1[i], k2[i])) {
      return false;
    }
  }
  return true;
}

const std::string &SortKey::getName() const {
  return name_;
}

SortOrder SortKey::getOrder() const {
  return order_;
}

}
