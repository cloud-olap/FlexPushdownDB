//
// Created by matt on 30/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_HASH_HASHUTIL_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_HASH_HASHUTIL_H

#include <cstddef>
#include <functional>

namespace normal::core::hash {

/* hash based off of a pointer dereference */
template<typename T>
struct PointerDereferenceHash {
  inline size_t operator()(const T &pointer) const {
	return std::hash(*pointer);
  }
};

/* equality based off of pointer dereference */
template<typename T>
struct PointerDereferenceEqualTo {
  inline bool operator()(const T &lhs, const T &rhs) const {
	return *lhs == *rhs;
  }
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_HASH_HASHUTIL_H
