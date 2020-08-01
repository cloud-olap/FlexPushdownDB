//
// Created by matt on 1/8/20.
//

#include "HashJoinBuildKernel2.h"
#include "ArraySetIndexBuilder.h"

#include <utility>

using namespace normal::pushdown::join;

HashJoinBuildKernel2::HashJoinBuildKernel2(std::string columnName) :
	columnName_(std::move(columnName)) {}

HashJoinBuildKernel2 HashJoinBuildKernel2::make(const std::string &columnName) {

  assert(!columnName.empty());

  auto canonicalColumnName = ColumnName::canonicalize(columnName);
  return HashJoinBuildKernel2(canonicalColumnName);
}

tl::expected<void, std::string> HashJoinBuildKernel2::put(const std::shared_ptr<TupleSet2> &tupleSet) {

  assert(tupleSet);

  if(!arraySetIndex_.has_value()){
	arraySetIndex_ = ArraySetIndexBuilder::make(tupleSet->getArrowTable().value(), columnName_);
	return {};
  }
  else{
	return arraySetIndex_.value()->put(tupleSet->getArrowTable().value());
  }
}

size_t HashJoinBuildKernel2::size() {
  return arraySetIndex_.value()->size();
}

void HashJoinBuildKernel2::clear() {
  arraySetIndex_.value()->clear();
}

std::shared_ptr<ArraySetIndex> HashJoinBuildKernel2::getArraySetIndex() {
  return arraySetIndex_.value();
}
