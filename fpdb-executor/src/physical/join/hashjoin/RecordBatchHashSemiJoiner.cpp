//
// Created by Yifei Yang on 12/15/21.
//

#include <fpdb/executor/physical/join/hashjoin/RecordBatchHashSemiJoiner.h>
#include <fpdb/tuple/TupleSetIndexFinder.h>

namespace fpdb::executor::physical::join {

RecordBatchHashSemiJoiner::RecordBatchHashSemiJoiner(const shared_ptr<TupleSetIndex> &buildTupleSetIndex,
                                                     const vector<string> &probeJoinColumnNames,
                                                     int64_t rowIndexOffset):
  buildTupleSetIndex_(buildTupleSetIndex),
  probeJoinColumnNames_(probeJoinColumnNames),
  rowIndexOffset_(rowIndexOffset) {}

shared_ptr<RecordBatchHashSemiJoiner> RecordBatchHashSemiJoiner::make(const shared_ptr<TupleSetIndex> &buildTupleSetIndex,
                                                                      const vector<string> &probeJoinColumnNames,
                                                                      int64_t rowIndexOffset) {
  return make_shared<RecordBatchHashSemiJoiner>(buildTupleSetIndex, probeJoinColumnNames, rowIndexOffset);
}

tl::expected<void, string> RecordBatchHashSemiJoiner::join(const shared_ptr<::arrow::RecordBatch> &recordBatch) {
  arrow::Status status;

  // Combine the chunks in the build table so we have single arrays for each column
  auto combineResult = buildTupleSetIndex_->combine();
  if (!combineResult)
    return tl::make_unexpected(combineResult.error());

  // Create a tupleSetIndexFinder
  const auto &expectedIndexFinder = TupleSetIndexFinder::make(buildTupleSetIndex_, probeJoinColumnNames_, recordBatch);
  if (!expectedIndexFinder.has_value())
    return tl::make_unexpected(expectedIndexFinder.error());
  auto indexFinder = expectedIndexFinder.value();

  // Iterate through the probe join column
  for (int64_t pr = 0; pr < recordBatch->num_rows(); ++pr) {

    // Find matched rowIndexes in the build column
    const auto &expBuildRows = indexFinder->find(pr);
    if (!expBuildRows.has_value()) {
      return tl::make_unexpected(expBuildRows.error());
    }
    const auto &buildRows = expBuildRows.value();

    // Check if the key is previously found
    if (buildRows.empty() || rowMatchIndexes_.find(buildRows[0]) != rowMatchIndexes_.end()) {
      continue;
    }

    // Save matched rowIndexes, incorporating rowIndexOffset
    for (int64_t rowIndex: buildRows) {
      rowMatchIndexes_.emplace(rowIndex + rowIndexOffset_);
    }
  }

  return {};
}

const unordered_set<int64_t> &RecordBatchHashSemiJoiner::getRowMatchIndexes() const {
  return rowMatchIndexes_;
}

}
