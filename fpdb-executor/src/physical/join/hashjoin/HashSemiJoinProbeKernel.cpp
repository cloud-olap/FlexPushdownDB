//
// Created by Yifei Yang on 12/15/21.
//

#include <fpdb/executor/physical/join/hashjoin/HashSemiJoinProbeKernel.h>
#include <fpdb/executor/physical/join/hashjoin/RecordBatchHashSemiJoiner.h>
#include <arrow/api.h>
#include <utility>

namespace fpdb::executor::physical::join {

HashSemiJoinProbeKernel::HashSemiJoinProbeKernel(HashJoinPredicate pred, set<string> neededColumnNames) :
  HashJoinProbeAbstractKernel(move(pred), move(neededColumnNames)) {}

shared_ptr<HashSemiJoinProbeKernel> HashSemiJoinProbeKernel::make(HashJoinPredicate pred, set<string> neededColumnNames) {
  return make_shared<HashSemiJoinProbeKernel>(move(pred), move(neededColumnNames));
}

bool HashSemiJoinProbeKernel::isSemi() const {
  return true;
}

tl::expected<unordered_set<int64_t>, string>
join(const shared_ptr<RecordBatchHashSemiJoiner> &joiner, const shared_ptr<TupleSet> &tupleSet) {
  unordered_set<int64_t> joinResult;
  ::arrow::Result<shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Read the table a batch at a time
  auto probeTable = tupleSet->table();
  ::arrow::TableBatchReader reader{*probeTable};
  reader.set_chunksize((int64_t) DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {
    // Join
    auto result = joiner->join(recordBatch);
    if (!result.has_value()) {
      return tl::make_unexpected(result.error());
    }

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  // Return joined result
  return joiner->getRowMatchIndexes();
}

tl::expected<void, string> HashSemiJoinProbeKernel::joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex> &tupleSetIndex) {
  // Get rowIndexOffset before buffering the input
  int64_t rowIndexOffset = buildTupleSetIndex_.has_value() ? buildTupleSetIndex_.value()->size() : 0;

  // Buffer tupleSetIndex
  auto putResult = putBuildTupleSetIndex(tupleSetIndex);
  if (!putResult)
    return tl::make_unexpected(putResult.error());

  // Create output schema
  if (probeTupleSet_.has_value()) {
    bufferOutputSchema(tupleSetIndex, probeTupleSet_.value());
  }

  // Check empty
  if (!probeTupleSet_.has_value() || probeTupleSet_.value()->numRows() == 0 || tupleSetIndex->size() == 0) {
    return {};
  }

  // Create joiner
  const auto &joiner = RecordBatchHashSemiJoiner::make(tupleSetIndex,
                                                       pred_.getRightColumnNames(),
                                                       rowIndexOffset);

  // Join
  const auto &expJoinResult = join(joiner, probeTupleSet_.value());
  if (!expJoinResult.has_value()) {
    return tl::make_unexpected(expJoinResult.error());
  }
  const auto &joinResult = expJoinResult.value();

  // Buffer join result
  rowMatchIndexes_.insert(joinResult.begin(), joinResult.end());

  return {};
}

tl::expected<void, string> HashSemiJoinProbeKernel::joinProbeTupleSet(const shared_ptr<TupleSet> &tupleSet) {
  // Buffer tupleSet
  auto putResult = putProbeTupleSet(tupleSet);
  if (!putResult)
    return tl::make_unexpected(putResult.error());

  // Create output schema
  if (buildTupleSetIndex_.has_value()) {
    bufferOutputSchema(buildTupleSetIndex_.value(), tupleSet);
  }

  // Check empty
  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0 || tupleSet->numRows() == 0) {
    return {};
  }

  // Create joiner
  const auto &joiner = RecordBatchHashSemiJoiner::make(buildTupleSetIndex_.value(),
                                                       pred_.getRightColumnNames(),
                                                       0);

  // Join
  const auto &expJoinResult = join(joiner, tupleSet);
  if (!expJoinResult.has_value()) {
    return tl::make_unexpected(expJoinResult.error());
  }
  const auto &joinResult = expJoinResult.value();

  // Buffer join result
  rowMatchIndexes_.insert(joinResult.begin(), joinResult.end());

  return {};
}

tl::expected<void, string> HashSemiJoinProbeKernel::finalize() {
  // Check if has input rows
  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0) {
    return {};
  }

  // Make input as a record batch, project only neededColumns of tupleSetIndex
  buildTupleSetIndex_.value()->combine();
  const auto &inputTable = buildTupleSetIndex_.value()->getTupleSet()->table();
  arrow::ArrayVector arrayVector;
  for (const auto &index: neededColumnIndice_) {
    if (index->first) {
      arrayVector.emplace_back(inputTable->column(index->second)->chunk(0));
    }
  }

  if (!outputSchema_.has_value()) {
    return tl::make_unexpected("Output schema not set yet during finalize");
  }
  const auto &recordBatch = arrow::RecordBatch::Make(outputSchema_.value(),
                                                     inputTable->num_rows(),
                                                     arrayVector);

  // Filter to get matched rows
  const auto &expSelectionVector = makeSelectionVector(rowMatchIndexes_);
  if (!expSelectionVector.has_value()) {
    return tl::make_unexpected(expSelectionVector.error());
  }
  const auto &selectionVector = expSelectionVector.value();
  const auto &expFilteredArrayVector =
          fpdb::expression::gandiva::Filter::evaluateBySelectionVectorStatic(*recordBatch,
                                                                               selectionVector);
  if (!expFilteredArrayVector.has_value()) {
    return tl::make_unexpected(expFilteredArrayVector.error());
  }

  // Buffer
  auto result = buffer(TupleSet::make(outputSchema_.value(), *expFilteredArrayVector));
  if (!result.has_value()) {
    return result;
  }

  return {};
}

tl::expected<shared_ptr<::gandiva::SelectionVector>, string>
HashSemiJoinProbeKernel::makeSelectionVector(const unordered_set<int64_t> &rowMatchIndexes) {
  // Create a selection vector
  shared_ptr<::gandiva::SelectionVector> selectionVector;
  auto status = ::gandiva::SelectionVector::MakeInt64((int64_t) rowMatchIndexes.size(),
                                                      ::arrow::default_memory_pool(),
                                                      &selectionVector);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  selectionVector->SetNumSlots((int64_t) rowMatchIndexes.size());

  // Sort rowMatchIndexes_
  vector<int64_t> rowMatchIndexesSorted(rowMatchIndexes.begin(), rowMatchIndexes.end());
  std::sort(rowMatchIndexesSorted.begin(), rowMatchIndexesSorted.end());

  // Set values for the selection vector
  for (uint i = 0; i < rowMatchIndexesSorted.size(); ++i) {
    selectionVector->SetIndex(i, rowMatchIndexesSorted[i]);
  }

  return selectionVector;
}

void HashSemiJoinProbeKernel::clear() {
  rowMatchIndexes_.clear();
  HashJoinProbeAbstractKernel::clear();
}

}
