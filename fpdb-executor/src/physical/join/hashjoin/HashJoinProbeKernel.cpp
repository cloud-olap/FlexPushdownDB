//
// Created by matt on 31/7/20.
//

#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeKernel.h>
#include <arrow/api.h>
#include <utility>

namespace fpdb::executor::physical::join {

HashJoinProbeKernel::HashJoinProbeKernel(HashJoinPredicate pred,
                                         set<string> neededColumnNames,
                                         bool isLeft,
                                         bool isRight) :
  HashJoinProbeAbstractKernel(move(pred), move(neededColumnNames)),
  isLeft_(isLeft),
  isRight_(isRight) {}

shared_ptr<HashJoinProbeKernel> HashJoinProbeKernel::make(HashJoinPredicate pred,
                                                          set<string> neededColumnNames,
                                                          bool isLeft,
                                                          bool isRight) {
  return make_shared<HashJoinProbeKernel>(move(pred), move(neededColumnNames), isLeft, isRight);
}

bool HashJoinProbeKernel::isSemi() const {
  return false;
}

tl::expected<shared_ptr<fpdb::tuple::TupleSet>, string>
HashJoinProbeKernel::join(const shared_ptr<RecordBatchHashJoiner> &joiner,
                          const shared_ptr<TupleSet> &probeTupleSet,
                          int64_t probeRowOffset) {
  ::arrow::Result<shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Read the table a batch at a time
  auto probeTable = probeTupleSet->table();
  ::arrow::TableBatchReader reader{*probeTable};
  reader.set_chunksize((int64_t) DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;
  int64_t localProbeRowOffset = 0;

  while (recordBatch) {

    // Join
    auto result = joiner->join(recordBatch, probeRowOffset + localProbeRowOffset);
    if (!result.has_value()) {
      return tl::make_unexpected(result.error());
    }

    // Increment localProbeRowOffset
    localProbeRowOffset += recordBatch->num_rows();

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  // Save row match indexes
  if (leftJoinHelper_.has_value()) {
    leftJoinHelper_.value()->putRowMatchIndexes(joiner->getBuildRowMatchIndexes());
  }
  if (rightJoinHelper_.has_value()) {
    rightJoinHelper_.value()->putRowMatchIndexes(joiner->getProbeRowMatchIndexes());
  }

  // Return joined result
  return joiner->toTupleSet();
}

tl::expected<void, string> HashJoinProbeKernel::joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex> &tupleSetIndex) {
  // Get offset before buffering the input
  int64_t buildRowOffset = buildTupleSetIndex_.has_value() ? buildTupleSetIndex_.value()->size() : 0;

  // Buffer tupleSetIndex
  auto putResult = putBuildTupleSetIndex(tupleSetIndex);
  if (!putResult)
    return tl::make_unexpected(putResult.error());

  // Create output schema and outer join helpers
  if (probeTupleSet_.has_value()) {
    bufferOutputSchema(tupleSetIndex, probeTupleSet_.value());
    auto result = makeOuterJoinHelpers();
    if (!result.has_value()) {
      return result;
    }
  }

  // Check empty
  if (!probeTupleSet_.has_value() || probeTupleSet_.value()->numRows() == 0 || tupleSetIndex->size() == 0) {
    return {};
  }

  // Create joiner
  auto expectedJoiner = RecordBatchHashJoiner::make(tupleSetIndex,
                                                    pred_.getRightColumnNames(),
                                                    outputSchema_.value(),
                                                    neededColumnIndice_,
                                                    buildRowOffset);
  if (!expectedJoiner.has_value()) {
    return tl::make_unexpected(expectedJoiner.error());
  }

  // Join
  auto expectedJoinedTupleSet = join(expectedJoiner.value(), probeTupleSet_.value(), 0);
  if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());

  // Buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value())
    return tl::make_unexpected(bufferResult.error());

  return {};
}

tl::expected<void, string> HashJoinProbeKernel::joinProbeTupleSet(const shared_ptr<TupleSet> &tupleSet) {
  // Get offset before buffering the input
  int64_t probeRowOffset = probeTupleSet_.has_value() ? probeTupleSet_.value()->numRows() : 0;

  // Buffer tupleSet
  auto putResult = putProbeTupleSet(tupleSet);
  if (!putResult)
    return tl::make_unexpected(putResult.error());

  // Create output schema and outer join helpers
  if (buildTupleSetIndex_.has_value()) {
    bufferOutputSchema(buildTupleSetIndex_.value(), tupleSet);
    auto result = makeOuterJoinHelpers();
    if (!result.has_value()) {
      return result;
    }
  }

  // Check empty
  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0 || tupleSet->numRows() == 0) {
    return {};
  }

  // Create joiner
  auto expectedJoiner = RecordBatchHashJoiner::make(buildTupleSetIndex_.value(),
                                                    pred_.getRightColumnNames(),
                                                    outputSchema_.value(),
                                                    neededColumnIndice_,
                                                    0);
  if (!expectedJoiner.has_value()) {
    return tl::make_unexpected(expectedJoiner.error());
  }

  // Join
  auto expectedJoinedTupleSet = join(expectedJoiner.value(), tupleSet, probeRowOffset);
  if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());

  // Buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value())
    return tl::make_unexpected(bufferResult.error());

  return {};
}

tl::expected<void, string> HashJoinProbeKernel::finalize() {
  // compute outer join
  auto result = computeOuterJoin();
  if (!result) {
    return result;
  }

  return {};
}

tl::expected<void, string> HashJoinProbeKernel::makeOuterJoinHelpers() {
  if (!isOuterJoinHelperCreated_) {
    if (!outputSchema_.has_value()) {
      return tl::make_unexpected("Output schema not set when making outer join helpers");
    }
    if (isLeft_ && !leftJoinHelper_.has_value()) {
      leftJoinHelper_ = OuterJoinHelper::make(true, outputSchema_.value(), neededColumnIndice_);
    }
    if (isRight_ && !rightJoinHelper_.has_value()) {
      rightJoinHelper_ = OuterJoinHelper::make(false, outputSchema_.value(), neededColumnIndice_);
    }
    isOuterJoinHelperCreated_ = true;
  }
  return {};
}

tl::expected<void, string> HashJoinProbeKernel::computeOuterJoin() {
  // left outer join
  if (leftJoinHelper_.has_value() && buildTupleSetIndex_.has_value()) {
    const auto &expLeftOutput = leftJoinHelper_.value()->compute(buildTupleSetIndex_.value()->getTupleSet());
    if (!expLeftOutput.has_value()) {
      return tl::make_unexpected(expLeftOutput.error());
    }
    auto result = buffer(expLeftOutput.value());
    if (!result.has_value()) {
      return result;
    }
  }

  // right outer join
  if (rightJoinHelper_.has_value() && probeTupleSet_.has_value()) {
    const auto &expRightOutput = rightJoinHelper_.value()->compute(probeTupleSet_.value());
    if (!expRightOutput.has_value()) {
      return tl::make_unexpected(expRightOutput.error());
    }
    auto result = buffer(expRightOutput.value());
    if (!result.has_value()) {
      return result;
    }
  }

  return {};
}

void HashJoinProbeKernel::clear() {
  leftJoinHelper_ = nullopt;
  rightJoinHelper_ = nullopt;
  HashJoinProbeAbstractKernel::clear();
}

}
