//
// Created by matt on 31/7/20.
//

#include <normal/pushdown/join/ATTIC/HashJoinProbeKernel.h>

#include <utility>

#include <normal/pushdown/join/ATTIC/Joiner.h>

using namespace normal::pushdown::join;

HashJoinProbeKernel::HashJoinProbeKernel(JoinPredicate pred) :
	pred_(std::move(pred)) {}

HashJoinProbeKernel HashJoinProbeKernel::make(JoinPredicate pred) {
  return HashJoinProbeKernel(std::move(pred));
}

void HashJoinProbeKernel::putHashTable(const std::shared_ptr<HashTable> &hashTable) {
  if (!hashTable_.has_value()) {
	hashTable_ = hashTable;
  } else {
	hashTable_.value()->merge(hashTable);
  }
}

tl::expected<void, std::string> HashJoinProbeKernel::putTupleSet(const std::shared_ptr<TupleSet2> &tupleSet) {
  if (!tupleSet_.has_value()) {
	tupleSet_ = tupleSet;
	return {};
  } else {
	return tupleSet_.value()->append(tupleSet);
  }
}

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> HashJoinProbeKernel::join() {

  if (!hashTable_.has_value())
	return tl::make_unexpected("HashTable not set");
  if (!tupleSet_.has_value())
	return tl::make_unexpected("TupleSet not set");

  Joiner joiner(pred_, hashTable_.value(), tupleSet_.value());
  auto joinedTuplesExpected = joiner.join();
  return joinedTuplesExpected;
}
