//
// Created by Yifei Yang on 4/26/24.
//

#ifndef FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_DISJOINTSET_H
#define FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_DISJOINTSET_H

#include <vector>
#include <unordered_map>
#include <sys/types.h>

namespace fpdb::util {

/**
 * A simple naive implementation of disjoint set
 */

template <typename T>
class DisjointSet {
public:
  void makeSet(const std::vector<T> &elements) {
    for (const T &e: elements) {
      insert(e);
    }
  }

  void insert(const T &e) {
    parent_[e] = e;
  }

  void unionSet(const T &e1, const T &e2) {
    const T &p1 = find(e1);
    const T &p2 = find(e2);
    parent_[p1] = p2;
  }

  bool checkUnion(const T &e1, const T &e2) const {
    return find(e1) == find(e2);
  }

private:
  const T &find(const T &e) const {
    auto it = parent_.find(e);
    if (it == parent_.end()) {
      throw std::runtime_error("Non-exist key in disjoint set.");
    }
    const T &p = it->second;
    if (p == e) {
      return e;
    }
    return find(p);
  }

  std::unordered_map<T, T> parent_;
};

}

#endif // FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_DISJOINTSET_H
