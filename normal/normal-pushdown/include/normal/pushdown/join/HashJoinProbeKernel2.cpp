//
// Created by matt on 31/7/20.
//


#include "HashJoinProbeKernel2.h"

#include <utility>

#include <arrow/api.h>

using namespace normal::pushdown::join;

HashJoinProbeKernel2::HashJoinProbeKernel2(JoinPredicate pred) :
	pred_(std::move(pred)) {}

HashJoinProbeKernel2 HashJoinProbeKernel2::make(JoinPredicate pred) {
  return HashJoinProbeKernel2(std::move(pred));
}

void HashJoinProbeKernel2::putHashTable(const std::shared_ptr<HashTable> &hashTable) {
  if (!hashTable_.has_value()) {
	hashTable_ = hashTable;
  } else {
	hashTable_.value()->merge(hashTable);
  }
}

tl::expected<void, std::string> HashJoinProbeKernel2::putTupleSet(const std::shared_ptr<TupleSet2> &tupleSet) {
  if (!tupleSet_.has_value()) {
	tupleSet_ = tupleSet;
	return {};
  } else {
	return tupleSet_.value()->append(tupleSet);
  }
}

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> HashJoinProbeKernel2::join() {

  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  if (!hashTable_.has_value())
	return tl::make_unexpected("HashTable not set");
  if (!tupleSet_.has_value())
	return tl::make_unexpected("TupleSet not set");

  auto probeTable = tupleSet_.value()->getArrowTable().value();

  // Read the table a batch at a time
  ::arrow::TableBatchReader reader{*probeTable};
  reader.set_chunksize(DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
	return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {

	// Shuffle batch
	joinRecordBatch(recordBatch);

	// Read a batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok()) {
	  return tl::make_unexpected(recordBatchResult.status().message());
	}
	recordBatch = *recordBatchResult;
  }
}

void HashJoinProbeKernel2::joinRecordBatch(const std::shared_ptr<arrow::RecordBatch> &recordBatch) {
  auto joinColumn = recordBatch->GetColumnByName(pred_.getRightColumnName());

  for (int64_t r = 0; r < joinColumn->length(); ++r) {
  }
}
