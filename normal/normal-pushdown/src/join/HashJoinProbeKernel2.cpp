//
// Created by matt on 31/7/20.
//


#include "normal/pushdown/join/HashJoinProbeKernel2.h"

#include <utility>

#include <arrow/api.h>

#include <normal/pushdown/join/RecordBatchJoiner.h>

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

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> HashJoinProbeKernel2::join() {

  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  if (!buildTupleSetIndex_.has_value())
	return tl::make_unexpected("ArraySetIndex not set");
  if (!probeTupleSet_.has_value()) {
    return tl::make_unexpected("TupleSet not set");
  }

  // Combine the chunks in the build table so we have single arrays for each column
  auto combineResult = buildTupleSetIndex_.value()->combine();
  if (!combineResult)
	return tl::make_unexpected(combineResult.error());

  //  buildTupleSetIndex_->validate();

  auto buildTable = buildTupleSetIndex_.value()->getTable();
  auto probeTable = probeTupleSet_.value()->getArrowTable().value();

  // Create the output schema and neededColumnIndice from neededColumns
  std::vector<std::shared_ptr<::arrow::Field>> outputFields;
  std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice;    // <true/false, i> -> ith column in build/probe table

  for (int c = 0; c < buildTable->schema()->num_fields(); ++c) {
    auto field = buildTable->schema()->field(c);
    if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
      neededColumnIndice.emplace_back(std::make_shared<std::pair<bool, int>>(true, c));
      outputFields.emplace_back(field);
    }
  }
  for (int c = 0; c < probeTable->schema()->num_fields(); ++c) {
    auto field = probeTable->schema()->field(c);
    if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
      neededColumnIndice.emplace_back(std::make_shared<std::pair<bool, int>>(false, c));
      outputFields.emplace_back(field);
    }
  }
  auto outputSchema = std::make_shared<::arrow::Schema>(outputFields);

  // check empty
  if (buildTable->num_rows() == 0 || probeTable->num_rows() == 0) {
    return TupleSet2::make(Schema::make(outputSchema));
  }

  // Create the joiner
  auto expectedJoiner = RecordBatchJoiner::make(buildTupleSetIndex_.value(), pred_.getRightColumnName(), outputSchema, neededColumnIndice);
  if (!expectedJoiner.has_value()) {
	return tl::make_unexpected(expectedJoiner.error());
  }
  auto joiner = expectedJoiner.value();

  // Read the table a batch at a time
  ::arrow::TableBatchReader reader{*probeTable};
  reader.set_chunksize(DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
	return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

//  /**
//   * compute the size of batch
//   */
//
//  size_t size = 0;
//  for (int col_id = 0; col_id < recordBatch->num_columns(); col_id++) {
//    auto array = recordBatch->column(col_id);
//    for (auto const &buffer: array->data()->buffers) {
//      size += buffer->size();
//    }
//  }
//  /**
//   * end
//   */

  while (recordBatch) {

//  SPDLOG_INFO("Join probed for {} tuples, column: {}, bytesize: {}", recordBatch->num_rows(), pred_.getRightColumnName(), size);

	// Shuffle batch
	joiner->join(recordBatch);


    // Read a batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok()) {
	  return tl::make_unexpected(recordBatchResult.status().message());
	}
	recordBatch = *recordBatchResult;
  }

  auto expectedJoinedTupleSet = joiner->toTupleSet();

#ifndef NDEBUG

  assert(expectedJoinedTupleSet.has_value());
  assert(expectedJoinedTupleSet.value()->getArrowTable().has_value());
  auto result = expectedJoinedTupleSet.value()->getArrowTable().value()->ValidateFull();
  if(!result.ok())
    throw std::runtime_error(fmt::format("{}, HashJoinProbeKernel2", result.message()));

#endif

  return expectedJoinedTupleSet;
}

const std::optional<std::shared_ptr<TupleSet2>> &HashJoinProbeKernel2::getProbeTupleSet() const {
  return probeTupleSet_;
}
