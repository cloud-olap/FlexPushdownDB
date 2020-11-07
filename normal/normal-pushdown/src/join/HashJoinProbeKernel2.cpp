//
// Created by matt on 31/7/20.
//


#include "normal/pushdown/join/HashJoinProbeKernel2.h"

#include <utility>

#include <arrow/api.h>

#include "normal/pushdown/join/RecordBatchJoiner.h"

using namespace normal::pushdown::join;

HashJoinProbeKernel2::HashJoinProbeKernel2(JoinPredicate pred, std::set<std::string> neededColumnNames) :
	pred_(std::move(pred)), neededColumnNames_(std::move(neededColumnNames)) {}

HashJoinProbeKernel2 HashJoinProbeKernel2::make(JoinPredicate pred, std::set<std::string> neededColumnNames) {
  return HashJoinProbeKernel2(std::move(pred), std::move(neededColumnNames));
}

tl::expected<void, std::string> HashJoinProbeKernel2::putBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex> &tupleSetIndex) {

  if(tupleSetIndex->getTable()->schema()->GetFieldIndex(pred_.getLeftColumnName()) == -1)
    return tl::make_unexpected(fmt::format("Cannot put build tuple set index into probe kernel. Index does not contain join predicate left column '{}'", pred_.getLeftColumnName()));

  if (!buildTupleSetIndex_.has_value()) {
	buildTupleSetIndex_ = tupleSetIndex;
	return {};
  }
  return buildTupleSetIndex_.value()->merge(tupleSetIndex);
}

tl::expected<void, std::string> HashJoinProbeKernel2::putProbeTupleSet(const std::shared_ptr<TupleSet2> &tupleSet) {

  if(tupleSet->getArrowTable().value()->schema()->GetFieldIndex(pred_.getRightColumnName()) == -1)
	return tl::make_unexpected(fmt::format("Cannot put probe tuple set into probe kernel. Tuple set does not contain join predicate right column '{}'", pred_.getRightColumnName()));

  if (!probeTupleSet_.has_value()) {
	probeTupleSet_ = tupleSet;
	return {};
  }
  return probeTupleSet_.value()->append(tupleSet);
}

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string>
join1(const std::shared_ptr<RecordBatchJoiner> &joiner, const std::shared_ptr<TupleSet2> &tupleSet) {
  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Read the table a batch at a time
  auto probeTable = tupleSet->getArrowTable().value();
  ::arrow::TableBatchReader reader{*probeTable};
  reader.set_chunksize(DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {

    // join
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

  // Get joined result
  auto expectedJoinedTupleSet = joiner->toTupleSet();

#ifndef NDEBUG

  assert(expectedJoinedTupleSet.has_value());
  assert(expectedJoinedTupleSet.value()->getArrowTable().has_value());
  auto result = expectedJoinedTupleSet.value()->getArrowTable().value()->ValidateFull();
  if(!result.ok())
    return tl::make_unexpected(fmt::format("{}, HashJoinProbeKernel2", result.message()));

#endif

  return expectedJoinedTupleSet.value();
}

tl::expected<void, std::string> HashJoinProbeKernel2::joinBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex> &tupleSetIndex) {

  // Check empty
  if (!probeTupleSet_.has_value() || probeTupleSet_.value()->numRows() == 0 || tupleSetIndex->size() == 0) {
    return {};
  }

  // Buffer tupleSetIndex
  auto result = putBuildTupleSetIndex(tupleSetIndex);
  if (!result)
    return tl::make_unexpected(result.error());

  // Create output schema
  bufferOutputSchema(tupleSetIndex, probeTupleSet_.value());

  // Create joiner
  auto expectedJoiner = RecordBatchJoiner::make(tupleSetIndex, pred_.getRightColumnName(), outputSchema_.value(), neededColumnIndice_);
  if (!expectedJoiner.has_value()) {
    return tl::make_unexpected(expectedJoiner.error());
  }

  // Join
  auto expectedJoinedTupleSet = join1(expectedJoiner.value(), probeTupleSet_.value());
  if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());

  // Buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value())
    return tl::make_unexpected(bufferResult.error());

  return {};
}

tl::expected<void, std::string> HashJoinProbeKernel2::joinProbeTupleSet(const std::shared_ptr<TupleSet2> &tupleSet) {

  // Check empty
  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0 || tupleSet->numRows() == 0) {
    return {};
  }

  // Buffer tupleSet
  auto result = putProbeTupleSet(tupleSet);
  if(!result)
    return tl::make_unexpected(result.error());

  // Create output schema
  bufferOutputSchema(buildTupleSetIndex_.value(), tupleSet);

  // Create joiner
  auto expectedJoiner = RecordBatchJoiner::make(buildTupleSetIndex_.value(), pred_.getRightColumnName(), outputSchema_.value(), neededColumnIndice_);
  if (!expectedJoiner.has_value()) {
    return tl::make_unexpected(expectedJoiner.error());
  }

  // Join
  auto expectedJoinedTupleSet = join1(expectedJoiner.value(), tupleSet);
  if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());

  // Buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value())
    return tl::make_unexpected(bufferResult.error());

  return {};
}

tl::expected<void, std::string> HashJoinProbeKernel2::buffer(const std::shared_ptr<TupleSet2> &tupleSet) {

  if (!buffer_.has_value()) {
    buffer_ = tupleSet;
  }
  else {
    const auto &bufferedTupleSet = buffer_.value();
    const auto &concatenateResult = TupleSet2::concatenate({bufferedTupleSet, tupleSet});
    if (!concatenateResult)
      return tl::make_unexpected(concatenateResult.error());

    buffer_ = concatenateResult.value();
  }

  return {};
}

const std::optional<std::shared_ptr<normal::tuple::TupleSet2>> &HashJoinProbeKernel2::getBuffer() const {
  return buffer_;
}

void HashJoinProbeKernel2::clear() {
  buffer_ = std::nullopt;
}

void HashJoinProbeKernel2::bufferOutputSchema(const std::shared_ptr<TupleSetIndex> &tupleSetIndex, const std::shared_ptr<TupleSet2> &tupleSet) {
  if (!outputSchema_.has_value()) {

    // Create the outputSchema_ and neededColumnIndice_ from neededColumnNames_
    std::vector<std::shared_ptr<::arrow::Field>> outputFields;

    for (int c = 0; c < tupleSetIndex->getTable()->schema()->num_fields(); ++c) {
      auto field = tupleSetIndex->getTable()->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(std::make_shared<std::pair<bool, int>>(true, c));
        outputFields.emplace_back(field);
      }
    }
    for (int c = 0; c < tupleSet->schema().value()->getSchema()->num_fields(); ++c) {
      auto field = tupleSet->schema().value()->getSchema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(std::make_shared<std::pair<bool, int>>(false, c));
        outputFields.emplace_back(field);
      }
    }
    outputSchema_ = std::make_shared<::arrow::Schema>(outputFields);
  }
}
