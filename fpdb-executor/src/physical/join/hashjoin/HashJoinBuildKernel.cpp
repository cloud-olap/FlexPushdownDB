//
// Created by matt on 1/8/20.
//

#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildKernel.h>
#include <fpdb/tuple/ColumnName.h>
#include <utility>

using namespace fpdb::executor::physical::join;

HashJoinBuildKernel::HashJoinBuildKernel(vector<string> columnNames) :
	columnNames_(move(columnNames)) {}

HashJoinBuildKernel HashJoinBuildKernel::make(const vector<string> &columnNames) {

  assert(!columnNames.empty());

  auto canonicalColumnNames = ColumnName::canonicalize(columnNames);
  return HashJoinBuildKernel(canonicalColumnNames);
}

tl::expected<void, string> HashJoinBuildKernel::put(const shared_ptr<TupleSet> &tupleSet) {

  assert(tupleSet);

  if(!tupleSetIndex_.has_value()){
    auto expectedTupleSetIndex = TupleSetIndex::make(columnNames_, tupleSet);
    if(!expectedTupleSetIndex.has_value()){
      return tl::make_unexpected(expectedTupleSetIndex.error());
    }
    tupleSetIndex_ = expectedTupleSetIndex.value();
    return {};
  }

  auto result = tupleSetIndex_.value()->put(tupleSet->table());
  return result;
}

size_t HashJoinBuildKernel::size() {
  if(!tupleSetIndex_.has_value()){
    return 0;
  }
  return tupleSetIndex_.value()->size();
}

void HashJoinBuildKernel::clear() {
  tupleSetIndex_ = nullopt;
}

optional<shared_ptr<TupleSetIndex>> HashJoinBuildKernel::getTupleSetIndex() {
  return tupleSetIndex_;
}
