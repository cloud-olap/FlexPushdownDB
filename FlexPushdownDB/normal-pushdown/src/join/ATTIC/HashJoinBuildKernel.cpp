//
// Created by matt on 31/7/20.
//

#include "normal/pushdown/join/ATTIC/HashJoinBuildKernel.h"

#include <utility>

using namespace normal::pushdown::join;

HashJoinBuildKernel::HashJoinBuildKernel(std::string columnName) :
	columnName_(std::move(columnName)),
	hashtable_(std::make_shared<HashTable>()) {}

HashJoinBuildKernel HashJoinBuildKernel::make(const std::string &columnName) {

  assert(!columnName.empty());

  auto canonicalColumnName = ColumnName::canonicalize(columnName);
  return HashJoinBuildKernel(canonicalColumnName);
}

tl::expected<void, std::string> HashJoinBuildKernel::put(const std::shared_ptr<TupleSet2> &tupleSet) {

  assert(tupleSet);

  return hashtable_->put(columnName_, tupleSet);
}

size_t HashJoinBuildKernel::size() {
  return hashtable_->size();
}

void HashJoinBuildKernel::clear() {
  hashtable_->clear();
}

std::shared_ptr<HashTable> HashJoinBuildKernel::getHashTable() {
  return hashtable_;
}
